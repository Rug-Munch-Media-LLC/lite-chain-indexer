"""Fill ClickHouse solana history from the GCP BigLake public dataset.

Doctrine: free RPC first → freemium fallback → BigLake to fill gaps. Reads
``bigquery-public-data.crypto_solana_mainnet_us`` via
:class:`~lite_indexer.query.bigquery_source.BigQuerySource` (DAY partitions,
never a full-table scan) and writes through the SAME ClickHouse writer the
live solana indexer uses (``SyncMixin.write_clickhouse`` →
``wallet_memory.solana_spl_balance_changes``).

``max_rows`` hard-stops the run; ``dry_run`` counts only. One bad partition
is logged and skipped; a CH outage aborts (fail closed). The table is a
plain ``MergeTree`` — re-fills append duplicates, so covered dates are
skipped unless ``force`` is set.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.bigquery_source import BigQuerySource
from lite_indexer.query.bigquery_spec import BigQuerySpec
from lite_indexer.query.bigquery_common import BigQueryQuotaError
from lite_indexer.ingest.lake_fill_common import (
    _estimated_scan_gib,
    _existing_day_count,
    fill_summary,
)
from lite_indexer.storage.clickhouse import ClickHouseUnavailable

logger = get_logger(__name__)

_PAGE_ROWS = 1000  # per-BQ-read page size (the SQL builder caps LIMIT at 1000)
FILLABLE_TABLES: tuple[str, ...] = ("token_transfers",)  # only tables with a CH writer

def _to_writer_event(row: dict[str, Any]) -> dict[str, Any]:
    """Map a BigQuery solana row to the ``write_clickhouse`` event shape."""
    ts = row.get("block_timestamp")
    ts_str = ts.isoformat() if isinstance(ts, datetime) else str(ts or "")
    return {
        "slot": row.get("slot", 0),
        "blockTimestamp": ts_str,
        "signature": row.get("tx_hash", ""),
        "token_account": row.get("token_account", ""),
        "mint": row.get("mint", ""),
        "owner": row.get("owner", ""),
        "amount_pre": row.get("pre_amount", "0"),
        "amount_post": row.get("post_amount", "0"),
        "change": row.get("delta", "0"),
        "decimals": row.get("decimals", 0),
        "symbol": row.get("token_symbol", ""),
        "token_name": row.get("token_name", ""),
    }

@dataclass
class SolanaBigQueryFillRequest:
    """One BigLake fill run: dates, tables, batching and volume guards."""

    dates: list[str]
    tables: tuple[str, ...] = ("token_transfers",)
    dry_run: bool = False
    batch_rows: int = 5000
    max_rows: int = 200_000
    mints: tuple[str, ...] = ()
    force: bool = False
    # Scan-budget guard: a day-partition read scans the whole partition
    # (~tens of GiB). Refuse runs whose ESTIMATED scan exceeds the budget
    # unless force_budget is set — the free quota is 1 TiB/month.
    max_scan_gib: float = 60.0
    force_budget: bool = False

@dataclass
class _FillCtx:
    """Per-run context: source, writers, request (keeps arities small)."""

    src: BigQuerySource
    writer: Any
    ch: Any
    req: SolanaBigQueryFillRequest

def _build_writer() -> Any:
    """Lake writer adapter: only the mixins ``write_clickhouse`` needs."""
    from lite_indexer.strategies.solana.strategy.address_activity import AddressActivityMixin
    from lite_indexer.strategies.solana.strategy.sync import SyncMixin
    from lite_indexer.strategies.solana.strategy.traces import TracesMixin
    from lite_indexer.strategies.solana.strategy.writes import WritesMixin

    class _Writer(WritesMixin, AddressActivityMixin, SyncMixin, TracesMixin):
        # drains need empty buffers; class attrs suffice (never mutated in place)
        _trace_rows: list[dict[str, Any]] = []
        _address_activity_rows: list[dict[str, Any]] = []

    return _Writer()

def _read_pages(ctx: _FillCtx, table: str, day: str, *, sink: Any | None) -> int:
    """Stream one (date, table) partition; ``sink=None`` counts only (dry run)."""
    total = 0
    batch: list[dict[str, Any]] = []
    offset = 0
    while True:
        rows = ctx.src.query(
            BigQuerySpec(
                table, chain="solana", dates=[day], limit=_PAGE_ROWS,
                mints=list(ctx.req.mints) or None, offset=offset or None,
            )
        )
        if not rows:
            break
        total += len(rows)
        if sink is not None:
            batch.extend(_to_writer_event(r) for r in rows)
            while len(batch) >= ctx.req.batch_rows:
                sink.write_clickhouse(batch[: ctx.req.batch_rows], ctx.ch)
                batch = batch[ctx.req.batch_rows :]
        offset += len(rows)
        if len(rows) < _PAGE_ROWS:
            break
    if sink is not None and batch:
        sink.write_clickhouse(batch, ctx.ch)
    return total

def backfill_solana_from_bigquery(
    req: SolanaBigQueryFillRequest,
    *,
    source: BigQuerySource | None = None,
    ch_writer: Any | None = None,
) -> dict[str, int]:
    """Fill ``req`` from BigLake into ClickHouse; returns ``{key: rows}``."""

    est_gib = _estimated_scan_gib(len(req.dates), len(req.tables))
    if est_gib > req.max_scan_gib and not req.force_budget:
        raise ValueError(
            f"estimated scan {est_gib:.0f} GiB exceeds --max-scan-gib {req.max_scan_gib:.0f} "
            f"(a day-partition read scans the whole partition; free quota = 1 TiB/month). "
            f"Lower the date range, or pass --force-budget to override."
        )

    bad = [t for t in req.tables if t not in FILLABLE_TABLES]
    if bad:
        raise ValueError(f"unsupported solana fill table(s): {bad} (fillable: {FILLABLE_TABLES})")
    src = source or BigQuerySource()
    if not src.available():
        logger.warning("bigquery_fill_unavailable")
        return {}
    wh = ch_writer if ch_writer is not None else _default_ch_writer()
    ctx = _FillCtx(src=src, writer=_build_writer(), ch=wh, req=req)

    counts: dict[str, int] = {}
    written_total = 0
    for day in req.dates:
        for table in req.tables:
            key = f"solana/{day}/{table}"
            if written_total >= req.max_rows:
                logger.warning("bigquery_fill_max_rows_reached", max_rows=req.max_rows)
                return counts
            try:
                total = _read_pages(ctx, table, day, sink=None) if req.dry_run else _guarded_write(ctx, table, day)
            except ClickHouseUnavailable:
                logger.error("bigquery_fill_aborted_clickhouse_unavailable", date=day, table=table)
                raise
            except BigQueryQuotaError:
                logger.error("bigquery_fill_aborted_quota", date=day, table=table)
                raise
            except Exception as exc:  # noqa: BLE001 — one bad date must not abort the run
                logger.warning("bigquery_fill_partition_failed", date=day, table=table, error=str(exc)[:120])
                counts[key] = 0
                continue
            counts[key] = total
            written_total += total
            logger.info(
                "bigquery_fill_progress", date=day, table=table,
                rows=total, dry_run=req.dry_run, total=written_total,
            )
    if written_total >= req.max_rows:
        logger.warning("bigquery_fill_max_rows_reached", max_rows=req.max_rows)
    return counts

def _guarded_write(ctx: _FillCtx, table: str, day: str) -> int:
    """Write one partition, skipping days ClickHouse already covers."""
    client = ctx.ch.client()
    if client is None:
        raise ClickHouseUnavailable("ClickHouse unreachable (chain=solana)")
    existing = _existing_day_count(client, day)
    if existing and not ctx.req.force:
        logger.warning("bigquery_fill_day_already_present", date=day, rows=existing)
        return 0
    return _read_pages(ctx, table, day, sink=ctx.writer)

def _default_ch_writer() -> Any:
    """Build the real ClickHouseWriter (same client the indexer uses)."""
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    return ClickHouseWriter()

