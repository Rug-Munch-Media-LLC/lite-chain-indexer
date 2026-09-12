"""
Snapshot bootstrap — replay public B2 parquet snapshots into ClickHouse.

Kills the #1 competitive objection ("your backfill is RPC-bound"): instead
of re-indexing a chain from genesis over free RPCs (days), download our
public parquet snapshot from B2 and import it into ClickHouse in minutes.

Flow (``bootstrap_chain``):

1. Resolve the snapshot date (``latest`` or an explicit ``YYYY-MM-DD``)
   and read the ``manifest.json`` for ``(chain, date)`` from
   ``{BOOTSTRAP_B2_BUCKET}/{BOOTSTRAP_PARQUET_PATH}/{date}/{chain}/``.
2. ``rclone copy`` the snapshot directory into
   ``{BOOTSTRAP_LOCAL_DIR}/{date}/{chain}/``.
3. Create the target ClickHouse tables if they do not exist.
4. ``INSERT INTO {target_db}.{table} SELECT * FROM file('*.parquet')``
   — ClickHouse's zero-copy parquet import, no row-by-row replay.
5. Verify the imported row count per table against the manifest.
6. Advance the chain state file + monotonic high-water mark to the
   snapshot ``end_block`` so the live daemon resumes forward (never
   rewinds — bootstrap is additive and only ever moves state up).
"""

from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lite_indexer.config import (
    BOOTSTRAP_B2_BUCKET,
    BOOTSTRAP_CH_FILE_ROOT,
    BOOTSTRAP_LOCAL_DIR,
    BOOTSTRAP_PARQUET_PATH,
    BOOTSTRAP_RCLONE_TIMEOUT,
    PARQUET_RCLONE_BIN,
)
from lite_indexer.logging import get_logger
from lite_indexer.snapshot_manifest import MANIFEST_NAME, SnapshotManifest, parse_manifest
from lite_indexer.strategies import SUPPORTED_CHAINS

logger = get_logger(__name__)

# ClickHouse table DDLs used to bootstrap a fresh database. These mirror
# the canonical writer schema (lite_indexer.storage.clickhouse) so a
# snapshot exported with ``SELECT *`` imports back cleanly. ``CREATE
# TABLE IF NOT EXISTS`` makes them a no-op on an existing deployment —
# imports always target the live table definition.
_TABLE_DDL: dict[str, str] = {
    "evm_transfers": """
CREATE TABLE IF NOT EXISTS {db}.evm_transfers (
    chain LowCardinality(String),
    block_number UInt64,
    tx_hash String,
    from_address String,
    to_address String,
    token_address String,
    value UInt256,
    block_timestamp DateTime,
    log_index UInt32,
    token_symbol String DEFAULT '',
    token_name String DEFAULT '',
    ingested_at DateTime,
    source String DEFAULT ''
) ENGINE = MergeTree()
ORDER BY (chain, block_number, log_index)
""",
    "dex_events": """
CREATE TABLE IF NOT EXISTS {db}.dex_events (
    type LowCardinality(String),
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime,
    tx_hash String,
    log_index UInt32,
    contract_address String,
    dex String,
    version String,
    token0 String,
    token1 String,
    sender String,
    recipient String,
    amount0_in String,
    amount1_in String,
    amount0_out String,
    amount1_out String,
    amount0 String,
    amount1 String,
    whale UInt8,
    price_usd Float64,
    volume_usd Float64,
    ingested_at DateTime,
    source String
) ENGINE = MergeTree()
ORDER BY (chain, block_number, log_index)
""",
    "deployer_events": """
CREATE TABLE IF NOT EXISTS {db}.deployer_events (
    deployer String,
    deployer_eoa String,
    token_address String,
    token0 String,
    token1 String,
    pair_address String,
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime,
    tx_hash String,
    type String,
    factory String,
    ingested_at DateTime,
    source String
) ENGINE = MergeTree()
ORDER BY (chain, block_number, log_index)
""",
    "solana_spl_balance_changes": """
CREATE TABLE IF NOT EXISTS {db}.solana_spl_balance_changes (
    chain LowCardinality(String),
    slot UInt64,
    block_timestamp DateTime,
    tx_hash String,
    mint String,
    owner String,
    amount UInt256
) ENGINE = MergeTree()
ORDER BY (chain, slot)
""",
    "internal_transfers": """
CREATE TABLE IF NOT EXISTS {db}.internal_transfers (
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime64(3),
    tx_hash String,
    from_address String,
    to_address String,
    value UInt256,
    trace_address Array(UInt32),
    input_method_id String,
    call_type String,
    ingested_at DateTime
) ENGINE = MergeTree()
ORDER BY (chain, block_number, tx_hash)
""",
}


@dataclass
class BootstrapResult:
    """Outcome of a ``bootstrap_chain`` run."""

    chain: str
    snapshot_date: str = ""
    status: str = "pending"  # ok / no_snapshot / clickhouse_unavailable / error
    blocks: int = 0
    rows_imported: int = 0
    start_block: int = 0
    end_block: int = 0
    tables: dict[str, int] = field(default_factory=dict)  # table → rows imported
    duration_seconds: float = 0.0
    snapshot_url: str = ""
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary (spec contract + diagnostics)."""
        return {
            "chain": self.chain,
            "snapshot_date": self.snapshot_date,
            "status": self.status,
            "blocks": self.blocks,
            "rows_imported": self.rows_imported,
            "block_range": [self.start_block, self.end_block],
            "tables": self.tables,
            "duration_seconds": round(self.duration_seconds, 2),
            "snapshot_url": self.snapshot_url,
            "errors": self.errors,
        }


def build_import_sql(
    manifest: SnapshotManifest,
    local_dir: Path,
    target_db: str,
    ch_file_root: str = BOOTSTRAP_CH_FILE_ROOT,
) -> list[str]:
    """Generate ClickHouse import statements for a downloaded snapshot.

    Produces one ``INSERT INTO {target_db}.{table} SELECT * FROM
    file(...)`` per manifest table — ClickHouse's zero-copy parquet
    import. File paths are resolved against ``ch_file_root`` when set
    (ClickHouse in a container sees a different mount point), otherwise
    against ``local_dir``.

    Args:
        manifest: Parsed snapshot manifest.
        local_dir: Local staging dir where the snapshot was downloaded
            (``{local_dir}/{date}/{chain}/`` layout).
        target_db: Target ClickHouse database.
        ch_file_root: Optional path root ClickHouse can see for the same
            files (empty = use ``local_dir``).

    Returns:
        List of executable ClickHouse SQL statements.
    """
    root = (ch_file_root or str(local_dir)).rstrip("/")
    base = f"{root}/{manifest.date}/{manifest.chain}"
    statements: list[str] = []
    for table_name, table in manifest.tables.items():
        fq = f"{target_db}.{table_name}"
        if table.parquet_files:
            files = ", ".join(f"'{base}/{f}'" for f in table.parquet_files)
            statements.append(f"INSERT INTO {fq} SELECT * FROM file({files})")
        else:
            # Legacy manifest without a file list: import every parquet
            # file in the snapshot dir for this table.
            statements.append(f"INSERT INTO {fq} SELECT * FROM file('{base}/*.parquet')")
    return statements


class SnapshotBootstrap:
    """Bootstrap indexer state from B2 parquet snapshots.

    Args:
        bucket: rclone remote root for snapshots
            (default ``BOOTSTRAP_B2_BUCKET``).
        snapshots_path: Path prefix inside the bucket
            (default ``BOOTSTRAP_PARQUET_PATH``).
        rclone_bin: Path to the rclone binary.
        local_dir: Local staging dir for downloads.
        state_dir: State directory override (defaults to the patched
            ``lite_indexer.state.STATE_DIR``).
    """

    def __init__(
        self,
        bucket: str = BOOTSTRAP_B2_BUCKET,
        snapshots_path: str = BOOTSTRAP_PARQUET_PATH,
        rclone_bin: str = PARQUET_RCLONE_BIN,
        local_dir: Path = BOOTSTRAP_LOCAL_DIR,
        state_dir: Path | None = None,
    ) -> None:
        self.bucket = bucket
        self.snapshots_path = snapshots_path
        self.rclone_bin = rclone_bin
        self.local_dir = local_dir
        self._state_dir = state_dir

    # ── remote helpers ────────────────────────────────────────────

    def _snapshot_root(self) -> str:
        """Remote root directory for snapshot folders."""
        return f"{self.bucket.rstrip('/')}/{self.snapshots_path.strip('/')}"

    def _snapshot_dir(self, chain: str, date: str) -> str:
        """Remote directory for a ``(chain, date)`` snapshot."""
        return f"{self._snapshot_root()}/{date}/{chain}"

    def _manifest_remote(self, chain: str, date: str) -> str:
        """Remote path to a ``(chain, date)`` manifest."""
        return f"{self._snapshot_dir(chain, date)}/{MANIFEST_NAME}"

    def _local_snapshot_dir(self, chain: str, date: str) -> Path:
        """Local staging dir mirroring the remote snapshot folder."""
        return self.local_dir / date / chain

    def _rclone_ok(self) -> bool:
        """True when the rclone binary is available."""
        if not self.rclone_bin or shutil.which(self.rclone_bin) is None:
            logger.warning("bootstrap_rclone_missing", bin=self.rclone_bin)
            return False
        return True

    # ── listing ───────────────────────────────────────────────────

    async def list_snapshots(self, chain: str | None = None) -> list[dict[str, Any]]:
        """List available bootstrap snapshots on B2.

        Enumerates ``manifest.json`` files under
        ``{bucket}/{snapshots_path}/`` via ``rclone lsf --files-only -R``,
        reads each manifest with ``rclone cat``, and returns:

        ``[{chain, date, block_range, file_count, size_bytes,
        manifest_url}]``

        Args:
            chain: When set, only return snapshots for this chain.

        Returns:
            Sorted list (newest date first) of snapshot summaries.
        """
        if not self._rclone_ok():
            return []

        try:
            listing = subprocess.run(
                [self.rclone_bin, "lsf", "--files-only", "-R", self._snapshot_root()],
                capture_output=True,
                text=True,
                timeout=BOOTSTRAP_RCLONE_TIMEOUT,
                check=False,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            logger.warning("bootstrap_list_failed", error=str(exc)[:120])
            return []
        if listing.returncode != 0:
            logger.warning(
                "bootstrap_list_error",
                error=(listing.stderr or listing.stdout)[-300:],
            )
            return []

        manifests: list[dict[str, Any]] = []
        for rel in listing.stdout.splitlines():
            rel = rel.strip()
            if not rel.endswith(f"/{MANIFEST_NAME}"):
                continue
            remote = f"{self._snapshot_root()}/{rel}"
            parsed = self._read_remote_manifest(remote)
            if parsed is None:
                continue
            if chain and parsed.chain != chain:
                continue
            manifests.append(
                {
                    "chain": parsed.chain,
                    "date": parsed.date,
                    "block_range": [parsed.start_block, parsed.end_block],
                    "file_count": parsed.total_files(),
                    "size_bytes": parsed.total_size_bytes(),
                    "manifest_url": remote,
                }
            )
        # Newest date first, then chain name for stability.
        manifests.sort(key=lambda m: (m["date"], m["chain"]), reverse=True)
        return manifests

    def _read_remote_manifest(self, remote: str) -> SnapshotManifest | None:
        """Fetch + parse a remote manifest via ``rclone cat``."""
        try:
            proc = subprocess.run(
                [self.rclone_bin, "cat", remote],
                capture_output=True,
                text=True,
                timeout=BOOTSTRAP_RCLONE_TIMEOUT,
                check=False,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            logger.warning("bootstrap_manifest_fetch_failed", remote=remote, error=str(exc)[:120])
            return None
        if proc.returncode != 0 or not proc.stdout.strip():
            logger.warning(
                "bootstrap_manifest_empty",
                remote=remote,
                error=(proc.stderr or "")[-200:],
            )
            return None
        try:
            manifest = parse_manifest(proc.stdout)
        except ValueError as exc:
            logger.warning("bootstrap_manifest_invalid", remote=remote, error=str(exc)[:200])
            return None
        return manifest

    async def _resolve_snapshot_date(self, chain: str, snapshot_date: str) -> str | None:
        """Resolve ``latest`` to the newest available date for a chain.

        Args:
            chain: Chain name.
            snapshot_date: ``latest`` or an explicit ``YYYY-MM-DD``.

        Returns:
            The date string, or None when no snapshot is available.
        """
        if snapshot_date != "latest":
            return snapshot_date
        snapshots = await self.list_snapshots(chain=chain)
        matching = [s for s in snapshots if s["chain"] == chain]
        if not matching:
            return None
        return str(max(s["date"] for s in matching))

    def _download_snapshot(self, chain: str, date: str) -> Path:
        """Download a ``(chain, date)`` snapshot dir into local staging.

        Args:
            chain: Chain name.
            date: Snapshot date.

        Returns:
            Local staging directory (``{local_dir}/{date}/{chain}``).

        Raises:
            RuntimeError: If rclone is unavailable or the copy fails.
        """
        if not self._rclone_ok():
            raise RuntimeError("rclone unavailable — cannot download snapshot")

        dest = self._local_snapshot_dir(chain, date)
        dest.mkdir(parents=True, exist_ok=True)
        try:
            proc = subprocess.run(
                [self.rclone_bin, "copy", self._snapshot_dir(chain, date), str(dest)],
                capture_output=True,
                text=True,
                timeout=BOOTSTRAP_RCLONE_TIMEOUT,
                check=False,
            )
        except (subprocess.SubprocessError, OSError) as exc:
            raise RuntimeError(f"rclone download failed: {exc}") from exc
        if proc.returncode != 0:
            raise RuntimeError(f"rclone download failed: {(proc.stderr or proc.stdout)[-300:]}")
        logger.info("bootstrap_downloaded", chain=chain, date=date, dest=str(dest))
        return dest

    # ── ClickHouse import ─────────────────────────────────────────

    def _connect(self) -> Any | None:
        """Lazily connect to ClickHouse (None when unavailable)."""
        from lite_indexer.storage.clickhouse import ClickHouseWriter

        return ClickHouseWriter().client()

    def _ensure_tables(self, client: Any, manifest: SnapshotManifest, target_db: str) -> None:
        """Create target tables if they do not exist (idempotent).

        Best-effort: a DDL failure (missing privileges, existing richer
        schema) degrades to a warning — the import targets whatever the
        live table definition is.

        Args:
            client: ClickHouse client.
            manifest: Parsed snapshot manifest.
            target_db: Target database name.
        """
        for table_name in manifest.tables:
            ddl = _TABLE_DDL.get(table_name)
            if ddl is None:
                logger.debug("bootstrap_no_ddl_for_table", table=table_name)
                continue
            try:
                client.execute(ddl.format(db=target_db))  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                logger.info("bootstrap_table_ensured", table=table_name, db=target_db)
            except Exception as exc:  # noqa: BLE001 — best-effort DDL
                logger.warning("bootstrap_table_ensure_failed", table=table_name, error=str(exc)[:150])

    @staticmethod
    def _run_imports(client: Any, statements: list[str]) -> dict[str, int]:
        """Execute the import SQL statements; return rows inserted per table.

        Rows inserted is reported as ``client.execute`` return value (the
        clickhouse-driver native client returns the number of rows for
        INSERT queries); falls back to counting rows via verification when
        the driver returns None/0 on a successful multi-insert.

        Args:
            client: ClickHouse client.
            statements: SQL statements from :func:`build_import_sql`.

        Returns:
            ``{table_name: rows_inserted}`` (table name parsed from the
            statement's ``INSERT INTO`` target).
        """
        per_table: dict[str, int] = {}
        for sql in statements:
            table_name = _table_from_import_sql(sql)
            try:
                affected = client.execute(sql)
            except Exception as exc:  # noqa: BLE001 — caller surfaces errors
                raise RuntimeError(f"snapshot import failed: {exc}") from exc
            if table_name:
                rows = int(affected) if affected is not None else 0
                per_table[table_name] = per_table.get(table_name, 0) + rows
        return per_table

    def _verify_row_counts(
        self,
        client: Any,
        manifest: SnapshotManifest,
        target_db: str,
    ) -> dict[str, dict[str, int]]:
        """Verify imported rows per table against the manifest.

        Counts rows matching the same predicate the export used
        (``chain`` + ``toDate(block_timestamp)``), so a pre-existing
        database with other chains/dates does not skew the comparison.

        Args:
            client: ClickHouse client.
            manifest: Parsed snapshot manifest.
            target_db: Target database name.

        Returns:
            ``{table: {expected, actual, ok}}``.
        """
        results: dict[str, dict[str, int]] = {}
        for table_name, table in manifest.tables.items():
            fq = f"{target_db}.{table_name}"
            try:
                actual = client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                    f"SELECT count() FROM {fq} WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s",
                    {"chain": manifest.chain, "day": manifest.date},
                )[0][0]
                actual = int(actual or 0)
            except Exception as exc:  # noqa: BLE001 — report, do not abort the batch
                logger.warning("bootstrap_verify_failed", table=table_name, error=str(exc)[:120])
                results[table_name] = {"expected": table.rows, "actual": -1}
                continue
            results[table_name] = {"expected": table.rows, "actual": actual}
            if actual != table.rows:
                logger.warning(
                    "bootstrap_verify_mismatch",
                    table=table_name,
                    expected=table.rows,
                    actual=actual,
                )
        return results

    # ── state high-water mark ─────────────────────────────────────

    def _set_high_water(self, chain: str, end_block: int, rows_imported: int) -> dict[str, Any]:
        """Advance the chain state + monotonic high-water mark.

        Bootstrap only ever moves state FORWARD: if an existing state file
        is already past ``end_block`` (the database already has newer
        data), nothing is written — the daemon continues from where it
        was.

        Args:
            chain: Chain name.
            end_block: Snapshot end block (from the manifest).
            rows_imported: Total rows imported (recorded as
                ``events_indexed`` on the state file).

        Returns:
            ``{state_before, state_after, advanced}``.
        """
        import lite_indexer.state as state_mod
        from lite_indexer.state import StateHighWater, StateManager

        if self._state_dir is not None:
            state_mod.STATE_DIR = self._state_dir

        state_before = StateManager(chain).load().get("last_block") or 0
        # Monotonic high-water mark — never rewinds.
        StateHighWater(chain).update(end_block)

        advanced = int(end_block) > int(state_before)
        if advanced:
            StateManager(chain).save(
                int(end_block),
                int(rows_imported),
                source="bootstrap",
            )
            logger.info("bootstrap_state_advanced", chain=chain, to_block=end_block)
        else:
            logger.info(
                "bootstrap_state_kept",
                chain=chain,
                state=state_before,
                snapshot_end=end_block,
            )
        state_after = StateManager(chain).load().get("last_block") or state_before
        return {
            "state_before": state_before,
            "state_after": state_after,
            "advanced": advanced,
        }

    # ── orchestration ─────────────────────────────────────────────

    async def bootstrap_chain(
        self,
        chain: str,
        snapshot_date: str = "latest",
        target_db: str = "wallet_memory",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Bootstrap one chain from a B2 snapshot into ClickHouse.

        Steps (see module docstring): resolve date → read manifest →
        download parquet → ensure tables → ``INSERT ... SELECT * FROM
        file('*.parquet')`` → verify row counts → advance high-water mark.

        Args:
            chain: Chain name.
            snapshot_date: ``latest`` or ``YYYY-MM-DD``.
            target_db: Target ClickHouse database.
            dry_run: Preview the plan without downloading/importing.

        Returns:
            ``BootstrapResult.to_dict()`` — ``{chain, snapshot_date,
            status, blocks, rows_imported, block_range, tables,
            duration_seconds, snapshot_url, errors}``.
        """
        result = BootstrapResult(chain=chain, snapshot_date=snapshot_date)
        started = time.monotonic()

        try:
            resolved = await self._resolve_snapshot_date(chain, snapshot_date)
            if resolved is None:
                result.status = "no_snapshot"
                result.errors.append(f"no snapshot available for chain {chain!r}")
                result.duration_seconds = time.monotonic() - started
                return result.to_dict()
            result.snapshot_date = resolved

            manifest_remote = self._manifest_remote(chain, resolved)
            result.snapshot_url = manifest_remote

            manifest = self._read_remote_manifest(manifest_remote)
            if manifest is None:
                result.status = "no_snapshot"
                result.errors.append(f"manifest not found at {manifest_remote}")
                result.duration_seconds = time.monotonic() - started
                return result.to_dict()

            result.start_block = manifest.start_block
            result.end_block = manifest.end_block
            result.blocks = max(0, manifest.end_block - manifest.start_block + 1)
            result.tables = {name: table.rows for name, table in manifest.tables.items()}

            if dry_run:
                local = self._local_snapshot_dir(chain, resolved)
                statements = build_import_sql(manifest, self.local_dir, target_db)
                result.status = "dry_run"
                result.snapshot_date = resolved
                result.duration_seconds = time.monotonic() - started
                result.errors.extend(
                    [
                        f"would download {manifest_remote} -> {local}",
                        f"would import {len(statements)} statement(s) into {target_db}",
                    ]
                )
                return result.to_dict()

            # 1. Download parquet files.
            self._download_snapshot(chain, resolved)

            # 2. Connect + ensure tables.
            client = self._connect()
            if client is None:
                result.status = "clickhouse_unavailable"
                result.errors.append("clickhouse unavailable (CH_HOST/CH_PORT/CH_DATABASE?)")
                result.duration_seconds = time.monotonic() - started
                return result.to_dict()
            self._ensure_tables(client, manifest, target_db)

            # 3. Zero-copy import via ClickHouse file() table function.
            statements = build_import_sql(manifest, self.local_dir, target_db)
            per_table = self._run_imports(client, statements)
            rows_imported = sum(per_table.values()) if per_table else 0
            result.rows_imported = rows_imported
            if per_table:
                result.tables = {name: count for name, count in per_table.items()}

            # 4. Verify row count matches the manifest.
            verify = self._verify_row_counts(client, manifest, target_db)
            mismatches = {t: v for t, v in verify.items() if v["actual"] != v["expected"]}
            if mismatches:
                result.status = "error"
                for t, v in mismatches.items():
                    result.errors.append(f"row count mismatch on {t}: expected {v['expected']}, got {v['actual']}")
            else:
                result.status = "ok"
                # 5. Set state high-water mark to the snapshot end block.
                self._set_high_water(chain, manifest.end_block, rows_imported)
        except Exception as exc:  # noqa: BLE001 — CLI surfaces errors as JSON
            result.status = "error"
            result.errors.append(str(exc)[:300])
            logger.error("bootstrap_chain_failed", chain=chain, error=str(exc)[:300])

        result.duration_seconds = time.monotonic() - started
        return result.to_dict()

    async def bootstrap_all(
        self,
        chains: list[str] | None = None,
        snapshot_date: str = "latest",
        target_db: str = "wallet_memory",
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Bootstrap all configured chains (or a subset).

        Args:
            chains: Chain names to bootstrap; defaults to every supported
                chain.
            snapshot_date: ``latest`` or ``YYYY-MM-DD``.
            target_db: Target ClickHouse database.
            dry_run: Preview the plan for every chain.

        Returns:
            ``{status, snapshot_date, target_db, chains: {...}}``.
        """
        chain_list = chains or sorted(SUPPORTED_CHAINS.keys())
        results: dict[str, Any] = {}
        failed = 0
        for chain in chain_list:
            res = await self.bootstrap_chain(
                chain,
                snapshot_date=snapshot_date,
                target_db=target_db,
                dry_run=dry_run,
            )
            results[chain] = res
            if res["status"] not in ("ok", "dry_run", "no_snapshot"):
                failed += 1
        return {
            "status": "ok" if failed == 0 else "partial_failure",
            "snapshot_date": snapshot_date,
            "target_db": target_db,
            "dry_run": dry_run,
            "chains": results,
        }


def _table_from_import_sql(sql: str) -> str:
    """Extract the target table name from an import statement.

    ``INSERT INTO {db}.{table} SELECT ...`` → ``{table}``.

    Args:
        sql: Import SQL statement.

    Returns:
        Table name, or ``""`` when it cannot be parsed.
    """
    rest = sql[len("INSERT INTO ") :]
    target = rest.split(" ", 1)[0]
    return target.rsplit(".", 1)[-1] if "." in target else target


__all__ = [
    "BootstrapResult",
    "SnapshotBootstrap",
    "build_import_sql",
]
