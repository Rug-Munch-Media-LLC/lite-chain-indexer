"""Shared helpers for lake fills (scan estimate, guards, summary)."""
from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

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
        "chain": "solana", "dry_run": req.dry_run, "max_rows": req.max_rows,
        "rows": sum(counts.values()), "counts": counts,
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
