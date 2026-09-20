"""Backfill base/arbitrum history from the AWS v1.1 sonarx lake into ClickHouse.

Operator strategy: bulk history comes from free lakes, never from RPC
hammering. This module loops (chain, date, table), streams mapped rows
from :class:`SonarxLakeSource`, and writes them through the existing
ClickHouse writers. Missing partitions are logged and skipped; one bad
date never aborts the run. No RPC anywhere.

Progress is logged per (chain, date, table) under ``lake_backfill_progress``.
"""

from __future__ import annotations

from datetime import date as _date
from datetime import timedelta
from typing import Any

from lite_indexer.ingest.lake_sonarx import CHAINS, TABLES, SonarxLakeSource
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_READERS = {"transfers": "read_transfers", "traces": "read_traces"}


def expand_dates(from_date: str, to_date: str) -> list[str]:
    """Expand an inclusive YYYY-MM-DD range into a list of dates."""
    start = _date.fromisoformat(from_date)
    end = _date.fromisoformat(to_date)
    if end < start:
        return []
    return [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]


def _write_batch(writer: Any, chain: str, table: str, batch: list[dict[str, Any]]) -> int:
    """Write one mapped batch through the appropriate ClickHouse writer."""
    if table == "transfers":
        return int(writer.write_transfers(chain, batch))
    client = writer.client()
    if client is None:
        return 0
    from lite_indexer.storage.trace_store import write_internal_transfers

    return int(write_internal_transfers(client, chain, batch))


def _write_partition(src: Any, wh: Any, chain: str, d: str, table: str, batch_rows: int) -> int:
    """Stream one partition from the lake and write it in batches."""
    reader = getattr(src, _READERS[table])
    total = 0
    batch: list[dict[str, Any]] = []
    for row in reader(chain, d):
        batch.append(row)
        if len(batch) >= batch_rows:
            total += _write_batch(wh, chain, table, batch)
            batch = []
    if batch:
        total += _write_batch(wh, chain, table, batch)
    return total


def backfill_chain_from_lake(
    chain: str,
    dates: list[str],
    tables: tuple[str, ...] = TABLES,
    dry_run: bool = False,
    batch_rows: int = 5000,
    *,
    source: Any | None = None,
    writer: Any | None = None,
) -> dict[str, int]:
    """Backfill (chain, dates, tables) from the sonarx lake into ClickHouse.

    Args:
        chain: ``base`` or ``arbitrum``.
        dates: ``YYYY-MM-DD`` partitions to ingest.
        tables: Subset of ``("transfers", "traces")``.
        dry_run: Count rows without writing.
        batch_rows: Rows per writer batch.
        source: DI seam — a SonarxLakeSource-like object (tests).
        writer: DI seam — a ClickHouseWriter-like object (tests).

    Returns:
        Mapping ``"<chain>/<date>/<table>" → row count`` for every
        partition attempted (missing/failed partitions count 0).
    """
    if chain not in CHAINS:
        raise ValueError(f"unsupported sonarx chain: {chain!r} (expected one of {CHAINS})")
    src = source if source is not None else SonarxLakeSource()
    if not src.available():
        logger.warning("lake_backfill_unavailable", chain=chain)
        return {}
    wh = writer if writer is not None else _default_writer()

    counts: dict[str, int] = {}
    for d in dates:
        for table in tables:
            key = f"{chain}/{d}/{table}"
            if _READERS.get(table) is None:
                logger.warning("lake_backfill_bad_table", chain=chain, table=table)
                counts[key] = 0
                continue
            try:
                if dry_run:
                    total = int(src.count(chain, table, d))
                else:
                    total = _write_partition(src, wh, chain, d, table, batch_rows)
            except Exception as exc:  # noqa: BLE001 — one bad date must not abort the run
                logger.warning(
                    "lake_backfill_partition_failed",
                    chain=chain,
                    date=d,
                    table=table,
                    error=str(exc)[:120],
                )
                counts[key] = 0
                continue
            counts[key] = total
            logger.info("lake_backfill_progress", chain=chain, date=d, table=table, rows=total, dry_run=dry_run)
    return counts


def _default_writer() -> Any:
    """Build the real ClickHouseWriter."""
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    return ClickHouseWriter()
