"""Shared helpers for lake fills (scan estimate, guards, summary)."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from lite_indexer.logging import get_logger

if TYPE_CHECKING:
    from lite_indexer.ingest.lake_bigquery_solana import SolanaBigQueryFillRequest

logger = get_logger(__name__)


def _estimated_scan_gib(days: int, tables: int) -> float:
    """Rough scan estimate: day-partition reads scan the whole partition.

    Calibrated from the 2026-09-21 usage: ~150 probe/fill jobs scanned
    ~981 GiB. Override per-day size with ``BIGQUERY_EST_DAY_GIB``.
    """
    import os as _os

    per_day = float(_os.environ.get("BIGQUERY_EST_DAY_GIB") or 45.0)
    return max(1, days) * max(1, tables) * per_day


def _existing_day_count(client: Any, day: str) -> int | None:
    """Rows already in CH for ``day`` (None = check unavailable)."""
    try:
        rows = client.execute(
            "SELECT count() FROM wallet_memory.solana_spl_balance_changes WHERE toDate(block_timestamp) = %(day)s",
            {"day": day},
        )
    except Exception as exc:  # noqa: BLE001 — guard is best-effort
        logger.debug("bigquery_fill_day_count_failed", day=day, error=str(exc)[:100])
        return None
    return int(rows[0][0]) if rows else 0


def fill_summary(req: SolanaBigQueryFillRequest, counts: dict[str, int]) -> dict[str, Any]:
    """Machine-readable run summary for the CLI."""
    return {
        "chain": "solana",
        "dry_run": req.dry_run,
        "max_rows": req.max_rows,
        "rows": sum(counts.values()),
        "counts": counts,
    }


def to_writer_event(row: dict[str, Any]) -> dict[str, Any]:
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


def build_writer() -> Any:
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


def parse_since(value: str | None) -> Any:
    """Parse a --since-ts ISO string once (None disables the filter)."""
    if not value:
        return None
    from datetime import UTC, datetime

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def row_ts(raw: dict[str, Any]) -> Any:
    """Row block_timestamp as an aware datetime (accepts ms epoch)."""
    from lite_indexer.query.bigquery_common import ms_to_dt

    value = raw.get("block_timestamp")
    if hasattr(value, "tzinfo"):
        return value
    return ms_to_dt(value)


def rest_transport() -> bool:
    """True unless the OFFSET/CLI path is explicitly requested.

    REST is the default: OFFSET paging re-scans the partition per page
    (2026-09-22 incident). ``BIGQUERY_TRANSPORT=cli`` opts back in for
    tests/fakes; real fills use one job with free result pages.
    """
    import os

    return os.environ.get("BIGQUERY_TRANSPORT", "rest").strip().lower() != "cli"


def read_full_day(ctx: Any, table: str, day: str, sink: Any | None) -> int:
    """One REST job per day; result pages are free (no partition re-scan)."""
    from lite_indexer.query.bigquery_cli import bq_accounts
    from lite_indexer.query.bigquery_rest import stream_query
    from lite_indexer.query.bigquery_solana import map_solana_row
    from lite_indexer.query.bigquery_spec import BigQuerySpec
    from lite_indexer.query.bigquery_tables import build_query

    project, creds = bq_accounts()[0]
    spec = BigQuerySpec(
        table,
        chain="solana",
        dates=[day],
        limit=ctx.req.max_rows,
        mints=list(ctx.req.mints) or None,
        since_ts=ctx.req.since_ts,
        until_ts=ctx.req.until_ts,
    )
    sql, params = build_query(spec)
    total = 0
    batch: list[dict[str, Any]] = []
    for page in stream_query(sql, params, project=project, creds_path=creds):
        for raw in page:
            ts = row_ts(raw)
            if ctx.since_dt is not None and ts < ctx.since_dt:
                continue
            if ctx.until_dt is not None and ts > ctx.until_dt:
                continue
            total += 1
            if sink is None:
                continue
            batch.append(to_writer_event(map_solana_row(table, raw)))
            if len(batch) >= ctx.req.batch_rows:
                sink.write_clickhouse(batch[: ctx.req.batch_rows], ctx.ch)
                batch = batch[ctx.req.batch_rows :]
    if sink is not None and batch:
        sink.write_clickhouse(batch, ctx.ch)
    return total
