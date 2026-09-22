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

