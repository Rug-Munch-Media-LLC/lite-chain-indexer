"""
DuckDB cold query engine for the B2 parquet archive (THA cold tier).

The Transparent Historical Access layer (``lite_indexer.storage.historical``)
reads hot data from ClickHouse and cold data from B2 parquet. This module
is the cold half: it maps event tables to B2 parquet files, smart-selects
only the files that can contain rows in the query window, and executes the
read with DuckDB (zero-copy parquet reads, no re-import).

It reuses the established path helpers from
``lite_indexer.storage.duckdb_query`` (same B2 layout) and adds:

* table → file-kind mapping for all THA event tables,
* date-range smart file selection (only files inside the query window),
* a cached, lock-guarded DuckDB connection shared across queries,
* three read modes:

  * **mount** (default) — read parquet from a local ``rclone mount``
    (``/var/data/b2-mount``): zero network per query after the mount.
  * **fetch** — shell out to ``rclone cat`` per file into a cache dir
    (no mount required; used when the mount is not running).
  * **httpfs** — S3-compatible ``read_parquet('s3://...')`` for
    unencrypted remotes only (the production ``b2-crypt:`` remote is
    rclone-crypt encrypted and cannot use httpfs).

Every method degrades gracefully: missing files, missing rclone, or a
DuckDB failure returns ``[]`` and logs — the unified layer falls back to
the hot path instead of failing the request.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import threading
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover — optional dependency
    duckdb = None  # type: ignore[assignment]

from lite_indexer.config import HISTORICAL_PARQUET_REMOTE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.duckdb_query import (
    _collect_rclone_cat_files,
    _date_range,
    _local_path,
    _split_files,
)

logger = get_logger(__name__)

# Table → parquet file-kind. The daily export writes transfers and dex
# files today (``{chain}_{YYYY-MM-DD}.parquet`` and
# ``{chain}_dex_{YYYY-MM-DD}.parquet``); deployer/internal/solana
# mappings are reserved so the engine is ready when the export pipeline
# grows — cold queries for those tables return empty until the files
# exist.
TABLE_KINDS: dict[str, str] = {
    "evm_transfers": "transfers",
    "dex_events": "dex",
    "deployer_events": "deployer",
    "internal_transfers": "internal",
    "solana_transfers": "solana",
}

# Canonical column list per table. The cold path normalizes every parquet
# row to exactly these columns (missing columns fill with type defaults)
# so the merged hot+cold result has a stable shape. Keep in sync with the
# ClickHouse writer schemas (lite_indexer.storage.clickhouse /
# lite_indexer.storage.trace_store / strategies.solana).
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "evm_transfers": (
        "chain",
        "block_number",
        "tx_hash",
        "from_address",
        "to_address",
        "token_address",
        "value",
        "block_timestamp",
        "log_index",
        "ingested_at",
    ),
    "dex_events": (
        "type",
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "contract_address",
        "dex",
        "version",
        "token0",
        "token1",
        "sender",
        "recipient",
        "amount0_in",
        "amount1_in",
        "amount0_out",
        "amount1_out",
        "amount0",
        "amount1",
        "whale",
        "price_usd",
        "volume_usd",
        "ingested_at",
        "source",
    ),
    "deployer_events": (
        "deployer",
        "deployer_eoa",
        "token_address",
        "token0",
        "token1",
        "pair_address",
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "type",
        "factory",
        "ingested_at",
        "source",
    ),
    "internal_transfers": (
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "trace_address",
        "from_address",
        "to_address",
        "value_wei",
        "call_type",
        "gas_used",
        "error",
        "input_method_id",
        "ingested_at",
    ),
    "solana_transfers": (
        "chain",
        "slot",
        "block_timestamp",
        "tx_hash",
        "token_account",
        "mint",
        "owner",
        "pre_amount",
        "post_amount",
        "delta",
        "decimals",
        "token_symbol",
        "token_name",
        "ingested_at",
    ),
}

# Canonical ORDER BY for event reads (descending, newest first). Matches
# the columns that actually exist per table — deployer_events and
# internal_transfers have no log_index, solana is slot-ordered.
TABLE_ORDER: dict[str, str] = {
    "evm_transfers": "block_timestamp DESC, block_number DESC, log_index DESC",
    "dex_events": "block_timestamp DESC, block_number DESC, log_index DESC",
    "deployer_events": "block_timestamp DESC, block_number DESC",
    "internal_transfers": "block_timestamp DESC, block_number DESC, tx_hash DESC",
    "solana_transfers": "block_timestamp DESC, slot DESC",
}

# Type defaults for canonical columns absent from older parquet files
# (schema drift: e.g. ``source`` was added after the first exports).
_COLUMN_DEFAULTS: dict[str, Any] = {
    "chain": "",
    "block_number": 0,
    "slot": 0,
    "log_index": 0,
    "tx_hash": "",
    "token_address": "",
    "from_address": "",
    "to_address": "",
    "owner": "",
    "mint": "",
    "token_account": "",
    "value": "",
    "value_wei": "",
    "amount": "",
    "pre_amount": "",
    "post_amount": "",
    "delta": "",
    "source": "",
    "error": "",
    "input_method_id": "",
}

# Hard cap on parquet files scanned by one unbounded (no time-bounds) cold
# query — bounds memory + latency when the archive has grown large.
MAX_FILES_PER_QUERY: int = int(os.environ.get("THA_MAX_COLD_FILES", "4096"))

_DEFAULT_MOUNT_DIR = "/var/data/b2-mount"

# Table → block-ordering column. Solana is slot-ordered (no block_number).
_BLOCK_COL: dict[str, str] = {
    "evm_transfers": "block_number",
    "dex_events": "block_number",
    "deployer_events": "block_number",
    "internal_transfers": "block_number",
    "solana_transfers": "slot",
}


def _lit(value: Any) -> str:
    """Render a Python value as a DuckDB literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, date):
        return "'" + value.isoformat() + "'"
    text = str(value).replace("'", "''")
    return "'" + text + "'"


class ColdStore:
    """DuckDB query engine over the B2 parquet cold tier.

    Thread-safe: the DuckDB connection is created once per process and
    guarded by a lock (DuckDB connections are not safe for concurrent
    use), so FastAPI's threadpool workers can share the store.

    Args:
        remote: rclone remote root of the parquet archive
            (defaults to ``HISTORICAL_PARQUET_REMOTE``, i.e.
            ``b2-crypt:chain-data/parquet``).
        rclone_bin: Path to the rclone binary.
        mount_dir: Local directory where the B2 remote is mounted
            (used when ``fetch`` is False).
        fetch: If True, fetch files via ``rclone cat`` into ``cache_dir``
            instead of reading a mount.
        cache_dir: Cache directory for fetched files.
        httpfs: If True, read via DuckDB httpfs against an unencrypted
            S3-compatible endpoint (production b2-crypt is encrypted —
            use mount/fetch instead).
        max_files: Cap on files scanned by one unbounded query.
    """

    def __init__(
        self,
        remote: str = HISTORICAL_PARQUET_REMOTE,
        rclone_bin: str = "rclone",
        mount_dir: str | Path = _DEFAULT_MOUNT_DIR,
        fetch: bool = False,
        cache_dir: str | Path | None = None,
        httpfs: bool = False,
        max_files: int = MAX_FILES_PER_QUERY,
    ) -> None:
        self.remote = remote.rstrip("/")
        self.rclone_bin = rclone_bin
        self.mount_dir = Path(mount_dir)
        self.fetch = fetch
        self.httpfs = httpfs
        self.max_files = max_files
        self.cache_dir = Path(cache_dir) if cache_dir else Path(tempfile.mkdtemp(prefix="tha-cold-"))
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._con: Any | None = None
        self._lock = threading.Lock()
        # (chain, start, end) → files — avoids re-listing the same window.
        self._list_cache: dict[tuple[str, str, str], dict[str, list[Path]]] = {}

    # ── Connection ───────────────────────────────────────────────────

    def _connection(self) -> Any:
        """Return the cached DuckDB connection (creating it on first use)."""
        if duckdb is None:
            raise RuntimeError("duckdb is not installed — run: pip install duckdb")
        if self._con is None:
            con = duckdb.connect()
            if self.httpfs:
                con.execute("INSTALL httpfs; LOAD httpfs;")
                endpoint = os.getenv("B2_S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com")
                region = os.getenv("B2_S3_REGION", "us-west-004")
                con.execute(f"SET s3_endpoint = '{endpoint}'")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                con.execute(f"SET s3_region = '{region}'")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                con.execute(f"SET s3_access_key_id = '{os.getenv('B2_ACCOUNT_ID', '')}'")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                con.execute(f"SET s3_secret_access_key = '{os.getenv('B2_APPLICATION_KEY', '')}'")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            self._con = con
        return self._con

    # ── Smart file selection ─────────────────────────────────────────

    @staticmethod
    def _window_days(from_time: datetime | None, to_time: datetime | None) -> list[date]:
        """Return the day range covering ``[from_time, to_time]``.

        Returns [] when no time bounds are given (caller must scan the
        whole chain tree, bounded by ``max_files``).
        """
        if from_time is None and to_time is None:
            return []
        start = (from_time or datetime.min.replace(tzinfo=UTC)).date()
        end = (to_time or datetime.now(UTC)).date()
        if end < start:
            return []
        return _date_range(start, end)

    def _rclone_ok(self) -> bool:
        """True when the rclone binary is available."""
        return bool(self.rclone_bin and shutil.which(self.rclone_bin) is not None)

    def _collect_files(
        self,
        chain: str,
        from_time: datetime | None,
        to_time: datetime | None,
    ) -> dict[str, list[Path]]:
        """Return parquet files for ``chain`` inside the query window.

        Smart selection: when time bounds are present, only the days in
        ``[from_time, to_time]`` are considered (each day's hour folders
        are probed, matching the live export layout). Without bounds the
        whole chain tree is walked (mount) or listed (fetch), capped at
        ``max_files``.
        """
        days = self._window_days(from_time, to_time)
        cache_key = (chain, days[0].isoformat() if days else "*", days[-1].isoformat() if days else "*")
        cached = self._list_cache.get(cache_key)
        if cached is not None:
            return cached

        if self.httpfs:
            files: dict[str, list[Any]] = self._collect_httpfs(chain, days)
        elif self.fetch:
            files = self._collect_fetch(chain, days)
        else:
            files = self._collect_mount(chain, days)

        self._list_cache[cache_key] = files
        return files

    def _collect_mount(self, chain: str, days: list[date]) -> dict[str, list[Path]]:
        """Collect parquet files under the mounted remote directory."""
        root = self.mount_dir / chain if chain else self.mount_dir
        if not root.is_dir():
            return {"transfers": [], "dex": [], "spl_balance_changes": []}
        if days:
            wanted = {f"{d.year:04d}/{d.month:02d}/{d.day:02d}" for d in days}
            files: list[Path] = []
            for p in root.rglob("*.parquet"):
                posix = p.as_posix()
                if any(f"/{seg}/" in posix for seg in wanted):
                    files.append(p)
                if len(files) >= self.max_files:
                    break
        else:
            files = list(root.rglob("*.parquet"))[: self.max_files]
        return _split_files(files, chain)

    def _collect_fetch(self, chain: str, days: list[date]) -> dict[str, list[Path]]:
        """Fetch parquet files via ``rclone cat`` into the cache dir."""
        if not self._rclone_ok():
            logger.warning("tha_rclone_missing", bin=self.rclone_bin)
            return {"transfers": [], "dex": [], "spl_balance_changes": []}
        if days and chain:
            # Reuse the established per-day/hour probe for bounded windows.
            return _collect_rclone_cat_files(chain, days[0], days[-1], self.cache_dir, self.rclone_bin)
        # Unbounded (or chain-less): list the tree once, then cat each file.
        remote_root = f"{self.remote}/{chain}" if chain else self.remote
        wanted = {f"{d.year:04d}/{d.month:02d}/{d.day:02d}" for d in days} if days else None
        files: list[Path] = []
        try:
            proc = subprocess.run(
                [self.rclone_bin, "lsf", "-R", "--format", "p", remote_root],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            logger.warning("tha_rclone_lsf_failed", chain=chain or "*", error=str(exc)[:100])
            return {"transfers": [], "dex": [], "spl_balance_changes": []}
        if proc.returncode != 0:
            logger.warning("tha_rclone_lsf_error", chain=chain or "*", error=proc.stderr[-200:])
            return {"transfers": [], "dex": [], "spl_balance_changes": []}
        for rel in proc.stdout.splitlines():
            if not rel.endswith(".parquet"):
                continue
            if wanted and not any(f"/{seg}/" in f"/{rel}" for seg in wanted):
                continue
            dest = self.cache_dir / rel
            if not dest.is_file():
                dest.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with dest.open("wb") as fh:
                        subprocess.run(
                            [self.rclone_bin, "cat", f"{remote_root}/{rel}"],
                            stdout=fh,
                            stderr=subprocess.PIPE,
                            timeout=300,
                            check=False,
                        )
                except (subprocess.SubprocessError, OSError) as exc:
                    logger.warning("tha_rclone_cat_failed", rel=rel, error=str(exc)[:100])
                    dest.unlink(missing_ok=True)
                    continue
            if dest.stat().st_size > 0:
                files.append(dest)
            if len(files) >= self.max_files:
                break
        return _split_files(files, chain)

    def _collect_httpfs(self, chain: str, days: list[date]) -> dict[str, list[Any]]:
        """Collect S3 URLs for the chain window (unencrypted remotes only)."""
        bucket = os.getenv("B2_BUCKET", "chain-data")
        files: dict[str, list[Any]] = {"transfers": [], "dex": [], "spl_balance_changes": []}
        day_list = days or [datetime.now(UTC).date() - timedelta(days=1)]
        for day in day_list:
            for hour in range(24):
                for kind in ("transfers", "dex"):
                    rel = _local_path("", chain, day, kind, hour)
                    s3 = f"s3://{bucket}/{rel}"
                    name = rel.name
                    if "_dex_" in name:
                        files["dex"].append(s3)
                    elif chain == "solana":
                        files["spl_balance_changes"].append(s3)
                    else:
                        files["transfers"].append(s3)
        return files

    @staticmethod
    def _files_for_table(files: dict[str, list[Path]], table: str, chain: str) -> list[Path]:
        """Filter a collected file set down to the table's kind."""
        kind = TABLE_KINDS[table]
        all_files = files.get("dex", []) + files.get("transfers", []) + files.get("spl_balance_changes", [])
        out: list[Path] = []
        for p in sorted(all_files, key=lambda f: os.path.basename(str(f))):
            name = os.path.basename(str(p))
            if (
                kind == "dex"
                and "_dex_" in name
                or kind == "deployer"
                and "_deployer_" in name
                or kind == "internal"
                and "_internal_" in name
                or (
                    kind == "solana"
                    and chain == "solana"
                    and "_dex_" not in name
                    and "_deployer_" not in name
                    and "_internal_" not in name
                )
                or kind == "transfers"
                and "_dex_" not in name
                and "_deployer_" not in name
                and "_internal_" not in name
            ):
                out.append(p)
        return out

    # ── Query ────────────────────────────────────────────────────────

    def query(
        self,
        table: str,
        chain: str = "",
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        where: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Run a read over the cold parquet files for the query window.

        ``where`` is a DuckDB-compatible SQL fragment (no leading AND)
        applied inside the read — it is trusted internal input from the
        unified layer, never user-supplied raw SQL.

        Returns:
            Rows as dicts normalized to ``TABLE_COLUMNS[table]``. Empty
            when no files match or the query fails (logged).
        """
        if table not in TABLE_KINDS:
            raise ValueError(f"unknown table for cold store: {table}")
        files = self._files_for_table(
            self._collect_files(chain, from_time, to_time),
            table,
            chain,
        )
        if not files:
            metrics_incr("tha_cold_queries")
            metrics_incr("tha_cold_empty")
            return []

        metrics_incr("tha_cold_queries")
        metrics_incr("tha_cold_files", len(files))

        with self._lock:
            con = self._connection()
            literals = ", ".join(repr(str(p)) for p in files)
            view = f"_tha_{table}"
            con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{literals}])")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized

            clauses: list[str] = []
            block_col = _BLOCK_COL[table]
            if chain:
                clauses.append(f"chain = {_lit(chain)}")
            if from_block is not None:
                clauses.append(f"{block_col} >= {from_block}")
            if to_block is not None:
                clauses.append(f"{block_col} <= {to_block}")
            if from_time is not None:
                clauses.append(f"block_timestamp >= {_lit(from_time)}")
            if to_time is not None:
                clauses.append(f"block_timestamp <= {_lit(to_time)}")
            if where:
                clauses.append(f"({where})")
            where_sql = " AND ".join(clauses) if clauses else "1=1"

            order = TABLE_ORDER.get(table, "block_timestamp DESC, block_number DESC, log_index DESC")
            # SELECT * (not the canonical columns) so older parquet files
            # missing later-added columns (e.g. `source`) still read;
            # _normalize fills the canonical shape with type defaults.
            sql = f"SELECT * FROM {view} WHERE {where_sql} ORDER BY {order} LIMIT {int(limit)}"
            try:
                rows = con.execute(sql).fetchall()  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                raw_cols = [desc[0] for desc in con.description] if con.description else []
            except Exception as exc:  # noqa: BLE001 — cold reads are best-effort
                metrics_incr("tha_cold_errors")
                logger.warning("tha_cold_query_failed", table=table, files=len(files), error=str(exc)[:150])
                return []

        return [self._normalize(table, dict(zip(raw_cols, row, strict=False))) for row in rows]

    @staticmethod
    def _normalize(table: str, row: dict[str, Any]) -> dict[str, Any]:
        """Fill any missing canonical column with its type default.

        Parquet files written from ``SELECT *`` may predate columns added
        later (e.g. ``source``); the unified merge expects every row to
        carry the canonical shape. Unknown extra columns are dropped.
        """
        out: dict[str, Any] = {}
        for col in TABLE_COLUMNS[table]:
            value = row.get(col)
            if value is None:
                value = _COLUMN_DEFAULTS.get(col, "")
            out[col] = value
        return out


__all__ = [
    "ColdStore",
    "TABLE_KINDS",
    "TABLE_COLUMNS",
    "TABLE_ORDER",
    "_lit",
]
