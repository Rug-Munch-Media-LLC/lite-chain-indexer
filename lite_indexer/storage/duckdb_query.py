"""
DuckDB query layer for the B2 parquet archive.

The lite indexer exports ClickHouse data to Parquet on B2 hourly
(``lite-indexer-parquet-daily.sh``). This module lets you query that
archive directly with DuckDB — no node, no ClickHouse round-trip.

Read path
---------
The production B2 remote (``b2-crypt:``) is encrypted with rclone-crypt,
so DuckDB's httpfs extension cannot read it directly. Use one of:

* **rclone mount** (recommended for repeated queries)::

      rclone mount b2-crypt:chain-data/parquet /mnt/parquet &

  then query with ``mount_dir="/mnt/parquet"`` (the default).

* **rclone cat** (one-off): the module shells out to ``rclone cat`` per
  file into a local cache dir, then queries the cache.

* **httpfs** (unencrypted remotes only): pass an S3-compatible endpoint
  and credentials.

Example queries
---------------
Count ETH transfers yesterday::

    SELECT count(*) AS n FROM transfers
    WHERE toDate(block_timestamp) = '2026-07-31'

Top DEX by volume::

    SELECT dex, count(*) AS swaps
    FROM dex
    GROUP BY dex
    ORDER BY swaps DESC
    LIMIT 10

Deployer activity (addresses deploying the most pools)::

    SELECT to_address AS deployer, count(*) AS pools
    FROM transfers
    WHERE to_address != ''
    GROUP BY to_address
    ORDER BY pools DESC
    LIMIT 20

SPL balance changes on Solana::

    SELECT mint, count(*) AS changes
    FROM spl_balance_changes
    GROUP BY mint
    ORDER BY changes DESC
    LIMIT 10
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import duckdb
except ImportError:  # pragma: no cover - optional dependency
    duckdb = None  # type: ignore[assignment]

# B2 path layout (hour-granular since 2026-08-03):
# {chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet
# and {chain}_dex_{YYYY-MM-DD}.parquet for DEX events. The filename is
# date-based; the hour lives in the directory component so each hour's
# export lands in its own folder (produced by lite-indexer-parquet-daily.sh).
_REMOTE_BASE = "b2-crypt-v2:chain-data/parquet"  # v2 = live producer (2026-09-21)

_VIEWS = ("transfers", "dex", "spl_balance_changes")


def _as_date(value: str | date | None) -> date:
    """Coerce an ISO string or date to a ``datetime.date`` (UTC today default)."""
    if value is None:
        return datetime.now(UTC).date() - timedelta(days=1)
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _date_range(start: str | date, end: str | date) -> list[date]:
    """Return every day in [start, end] inclusive."""
    start_d, end_d = _as_date(start), _as_date(end)
    days = []
    d = start_d
    while d <= end_d:
        days.append(d)
        d += timedelta(days=1)
    return days


def _local_path(mount_dir: str | Path, chain: str, day: date, kind: str, hour: int = 0) -> Path:
    """Build the local (mounted) path for a chain/date/hour file.

    Hour-granular layout: ``{chain}/{YYYY}/{MM}/{DD}/{HH}/{fname}``. The
    filename stays date-based (``{chain}_{YYYY-MM-DD}.parquet``); the hour
    lives in the directory component so each hour's export from
    ``lite-indexer-parquet-daily.sh`` lands in its own folder.
    """
    y, m, d = f"{day.year:04d}", f"{day.month:02d}", f"{day.day:02d}"
    date_str = f"{y}-{m}-{d}"
    if kind == "dex":
        fname = f"{chain}_dex_{date_str}.parquet"
    else:
        fname = f"{chain}_{date_str}.parquet"
    return Path(mount_dir) / chain / y / m / d / f"{hour:02d}" / fname


def _remote_path(chain: str, day: date, kind: str, hour: int = 0) -> str:
    """Build the B2 remote path for a chain/date/hour file."""
    return f"{_REMOTE_BASE}/{_local_path('', chain, day, kind, hour)}"


def _register_views(con: Any, files_by_view: dict[str, list[Path]]) -> None:
    """Register each parquet file set as a DuckDB view.

    Views with no files are registered as empty (single dummy column) so
    ``SELECT count(*)`` still returns 0 instead of erroring; queries
    referencing real columns on empty views fail with a clear error.
    """
    for view, paths in files_by_view.items():
        if paths:
            literals = ", ".join(repr(str(p)) for p in paths)
            con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{literals}])")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        else:
            con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM (SELECT 1 AS __dummy) WHERE 1 = 0")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized


def _split_files(files: Iterable[Path], chain: str) -> dict[str, list[Path]]:
    """Classify parquet files into logical views (dex / spl / transfers)."""
    out: dict[str, list[Path]] = {"transfers": [], "dex": [], "spl_balance_changes": []}
    for p in sorted(files):
        name = p.name
        if "_dex_" in name:
            out["dex"].append(p)
        elif chain == "solana" and "_dex_" not in name:
            out["spl_balance_changes"].append(p)
        else:
            out["transfers"].append(p)
    return out


def _collect_mount_files(
    mount_dir: str | Path,
    chain: str,
    start: str | date,
    end: str | date,
) -> dict[str, list[Path]]:
    """Collect existing parquet files under a mounted rclone directory."""
    files = []
    for day in _date_range(start, end):
        for hour in range(24):
            for kind in ("transfers", "dex"):
                p = _local_path(mount_dir, chain, day, kind, hour)
                if p.is_file():
                    files.append(p)
    return _split_files(files, chain)


def _collect_rclone_cat_files(
    chain: str,
    start: str | date,
    end: str | date,
    cache_dir: str | Path | None = None,
    rclone_bin: str = "rclone",
) -> dict[str, list[Path]]:
    """Fetch each parquet file via ``rclone cat`` into a cache dir."""
    cache = Path(cache_dir) if cache_dir else Path(tempfile.mkdtemp(prefix="duckdb-parquet-"))
    cache.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []
    for day in _date_range(start, end):
        for hour in range(24):
            for kind in ("transfers", "dex"):
                dest = _local_path(cache, chain, day, kind, hour)
                if dest.is_file():
                    files.append(dest)
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                remote = _remote_path(chain, day, kind, hour)
                with dest.open("wb") as fh:
                    proc = subprocess.run(
                        [rclone_bin, "cat", remote],
                        stdout=fh,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                if proc.returncode != 0 or dest.stat().st_size == 0:
                    dest.unlink(missing_ok=True)
                    continue
                files.append(dest)
    return _split_files(files, chain)


def _ensure_duckdb() -> Any:
    if duckdb is None:
        raise RuntimeError("duckdb is not installed — run: pip install duckdb")
    return duckdb.connect()


def query_parquet(
    sql: str,
    chain: str = "ethereum",
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    mount_dir: str | Path = "/mnt/parquet",
    fetch: bool = False,
    rclone_bin: str = "rclone",
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Run ``sql`` over the parquet archive for ``chain`` in a date range.

    Args:
        sql: DuckDB SQL. Logical views available: ``transfers``,
            ``dex`` (DEX swaps) and ``spl_balance_changes`` (Solana).
        chain: chain name matching the B2 directory (ethereum, base, ...).
        start: inclusive start date (ISO string or date). Defaults to yesterday.
        end: inclusive end date. Defaults to ``start``.
        mount_dir: local directory where the B2 remote is mounted.
        fetch: if True, fetch files via ``rclone cat`` instead of reading
            the mount (useful when no mount is running).
        rclone_bin: path to the rclone binary.
        cache_dir: cache directory for fetched files (ignored unless fetch).

    Returns:
        Query result rows as a list of dicts.
    """
    # Resolve documented defaults: start defaults to yesterday, end to start.
    start_d: str | date = start if start is not None else (date.today() - timedelta(days=1))
    end_d: str | date = end if end is not None else start_d

    if fetch:
        files = _collect_rclone_cat_files(chain, start_d, end_d, cache_dir, rclone_bin)
    else:
        files = _collect_mount_files(mount_dir, chain, start_d, end_d)

    con = _ensure_duckdb()
    _register_views(con, files)
    rows = con.execute(sql).fetchall()
    cols = [desc[0] for desc in con.description] if con.description else []
    return [dict(zip(cols, row, strict=False)) for row in rows]


def query_httpfs(
    sql: str,
    chain: str = "ethereum",
    start: str | date | None = None,
    end: str | date | None = None,
    *,
    endpoint: str | None = None,
    region: str | None = None,
    access_key_id: str | None = None,
    secret_access_key: str | None = None,
    bucket: str = "chain-data",
) -> list[dict[str, Any]]:
    """Run ``sql`` over parquet on an unencrypted S3-compatible remote.

    NOTE: the production ``b2-crypt:`` remote is rclone-crypt encrypted and
    CANNOT be read via httpfs. Use this only for unencrypted remotes.

    Credentials come from args or the environment: ``B2_ACCOUNT_ID`` /
    ``B2_APPLICATION_KEY`` (Backblaze), or ``AWS_ACCESS_KEY_ID`` /
    ``AWS_SECRET_ACCESS_KEY``.
    """
    ak = access_key_id or os.getenv("B2_ACCOUNT_ID") or os.getenv("AWS_ACCESS_KEY_ID")
    sk = secret_access_key or os.getenv("B2_APPLICATION_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
    ep = endpoint or os.getenv("B2_S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com")
    rg = region or os.getenv("B2_S3_REGION", "us-west-004")
    if not ak or not sk:
        raise RuntimeError("httpfs needs access keys (B2_ACCOUNT_ID / B2_APPLICATION_KEY)")

    # Resolve documented defaults: start defaults to yesterday, end to start.
    start_d: str | date = start if start is not None else (date.today() - timedelta(days=1))
    end_d: str | date = end if end is not None else start_d

    con = _ensure_duckdb()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint = '{ep}'")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    con.execute(f"SET s3_region = '{rg}'")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    con.execute(f"SET s3_access_key_id = '{ak}'")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    con.execute(f"SET s3_secret_access_key = '{sk}'")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized

    files: dict[str, list[str]] = {"transfers": [], "dex": [], "spl_balance_changes": []}
    for day in _date_range(start_d, end_d):
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

    for view, paths in files.items():
        if not paths:
            continue
        literals = ", ".join(repr(p) for p in paths)
        con.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_parquet([{literals}])")  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized

    rows = con.execute(sql).fetchall()
    cols = [desc[0] for desc in con.description] if con.description else []
    return [dict(zip(cols, row, strict=False)) for row in rows]


def example_queries() -> dict[str, str]:
    """Return the documented example queries keyed by name."""
    return {
        "count_transfers": "SELECT count(*) AS n FROM transfers",
        "top_dex_by_volume": ("SELECT dex, count(*) AS swaps FROM dex GROUP BY dex ORDER BY swaps DESC LIMIT 10"),
        "deployer_activity": (
            "SELECT to_address AS deployer, count(*) AS pools FROM transfers "
            "WHERE to_address != '' GROUP BY to_address ORDER BY pools DESC LIMIT 20"
        ),
        "solana_spl_changes": (
            "SELECT mint, count(*) AS changes FROM spl_balance_changes GROUP BY mint ORDER BY changes DESC LIMIT 10"
        ),
    }


__all__ = [
    "query_parquet",
    "query_httpfs",
    "example_queries",
    "_REMOTE_BASE",
]
