"""Backfill base/arbitrum history from the AWS v1.1 sonarx lake into ClickHouse.

Operator strategy: bulk history comes from free lakes, never from RPC
hammering. This module loops (chain, date, table), streams mapped rows
from :class:`SonarxLakeSource`, and writes them through the existing
ClickHouse writers. Missing partitions are logged and skipped; one bad
date never aborts the run. No RPC anywhere.

Progress is logged per (chain, date, table) under ``lake_backfill_progress``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date as _date
from datetime import timedelta
from typing import Any

from lite_indexer.ingest.lake_sonarx import CHAINS, TABLES, SonarxLakeSource
from lite_indexer.lakes.router import LakeProviderRouter
from lite_indexer.logging import get_logger
from lite_indexer.storage.clickhouse import ClickHouseUnavailable

logger = get_logger(__name__)

_READERS = {"transfers": "read_transfers", "traces": "read_traces"}

#: Documented fallback when LAKE_BACKFILL_BATCH_ROWS is unset or unparsable.
_DEFAULT_BATCH_ROWS = 1000


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
        raise ClickHouseUnavailable(f"ClickHouse unreachable (chain={chain})")
    from lite_indexer.storage.trace_store import write_internal_transfers

    return int(write_internal_transfers(client, chain, batch))


def _batch_rows_default(raw: str | None) -> int:
    """Return the lake backfill batch bound for the raw env value.

    Pure: takes the raw string, reads no environment, so the operator-facing
    bound is testable with a string argument instead of by mutating
    ``os.environ`` and reloading the module. An unset or unparsable value
    keeps the 1k default that commit 0e35831 chose for a live stall.
    """
    if raw is None or not raw.strip():
        return _DEFAULT_BATCH_ROWS
    try:
        return int(raw)
    except ValueError as exc:
        logger.warning("lake_backfill_batch_rows_unparsable", error=str(exc)[:60], raw=str(raw)[:24])
        return _DEFAULT_BATCH_ROWS


@dataclass
class LakeBackfillRequest:
    """One lake backfill run: chain, dates, tables, batching."""

    chain: str
    dates: list[str]
    tables: tuple[str, ...] = TABLES
    dry_run: bool = False
    # 1k rows avoids ClickHouse driver receive timeouts on wide transfer
    # batches (commit 0e35831 lowered 5000 -> 1000 for a live stall). The
    # bound an operator had to ship a code change to move is now
    # LAKE_BACKFILL_BATCH_ROWS, matching PRICING_BATCH_SIZE and
    # VECTOR_INGESTION_BATCH_SIZE, which are already env-driven.
    batch_rows: int = _batch_rows_default(os.environ.get("LAKE_BACKFILL_BATCH_ROWS"))


def _write_partition(src: Any, wh: Any, chain: str, partition: tuple[str, str], batch_rows: int) -> int:
    """Stream one ``(date, table)`` partition from the lake and write it in batches."""
    d, table = partition
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
    req: LakeBackfillRequest,
    *,
    source: Any | None = None,
    writer: Any | None = None,
    router: LakeProviderRouter | None = None,
) -> dict[str, int]:
    """Backfill ``req`` (chain, dates, tables) from the sonarx lake into ClickHouse.

    ``source`` / ``writer`` / ``router`` are DI seams (tests); defaults are
    live objects. The router gates every partition: no available provider →
    the partition is skipped with a log (quota/health aware).
    """
    if req.chain not in CHAINS:
        raise ValueError(f"unsupported sonarx chain: {req.chain!r} (expected one of {CHAINS})")
    src = source if source is not None else SonarxLakeSource()
    if not src.available():
        logger.warning("lake_backfill_unavailable", chain=req.chain)
        return {}
    wh = writer if writer is not None else _default_writer()
    rt = router if router is not None else LakeProviderRouter()

    counts: dict[str, int] = {}
    for d in req.dates:
        for table in req.tables:
            key = f"{req.chain}/{d}/{table}"
            if _READERS.get(table) is None:
                logger.warning("lake_backfill_bad_table", chain=req.chain, table=table)
                counts[key] = 0
                continue
            try:
                prov = rt.select(req.chain, table)
                if prov is None:
                    logger.warning("lake_backfill_no_provider", chain=req.chain, date=d, table=table)
                    counts[key] = 0
                    continue
                if prov.name != f"aws-sonarx-{req.chain}":
                    # Rotation picked a fallback provider; no reader wired for it yet.
                    logger.warning(
                        "lake_backfill_provider_unreadable", provider=prov.name, chain=req.chain, table=table
                    )
                    counts[key] = 0
                    continue
                if req.dry_run:
                    total = int(src.count(req.chain, table, d))
                else:
                    total = _write_partition(src, wh, req.chain, (d, table), req.batch_rows)
                # bytes: parquet metadata size would cost an extra HTTP round-trip
                # per partition — not cheap, so 0 until a metadata probe is wired.
                rt.report_success(prov.name, bytes_read=0)
            except ClickHouseUnavailable:
                # Fail CLOSED: ClickHouse down is NOT a "bad date" — abort the
                # whole run instead of skipping every remaining partition.
                logger.error("lake_fill_aborted_clickhouse_unavailable", chain=req.chain, date=d, table=table)
                raise
            except Exception as exc:  # noqa: BLE001 — one bad date must not abort the run
                rt.report_failure(f"aws-sonarx-{req.chain}", error=str(exc))
                logger.warning(
                    "lake_backfill_partition_failed",
                    chain=req.chain,
                    date=d,
                    table=table,
                    error=str(exc)[:120],
                )
                counts[key] = 0
                continue
            counts[key] = total
            logger.info("lake_backfill_progress", chain=req.chain, date=d, table=table, rows=total, dry_run=req.dry_run)
    return counts


def _default_writer() -> Any:
    """Build the real ClickHouseWriter."""
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    return ClickHouseWriter()
