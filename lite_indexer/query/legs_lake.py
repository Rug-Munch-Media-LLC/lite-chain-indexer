"""AWS BigLake + date-partition leg for :class:`QueryRouter`.

Derives date partitions for deep block ranges, queries the AWS public
Ethereum archive, and applies router filter semantics to its rows. Bound
as ``QueryRouter`` methods by the facade so the public surface is unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.leg_request import LegRequest

logger = get_logger(__name__)


def filter_rows(
    table: str,  # noqa: ARG001 — kept for future per-table filter semantics
    rows: list[dict[str, Any]],
    filters: dict[str, Any] | list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Apply router filter semantics (dict=AND eq, list=OR eq) to rows."""
    if not filters:
        return rows

    def _matches(row: dict[str, Any], cond: dict[str, Any]) -> bool:
        return all(str(row.get(col, "")).lower() == str(val).lower() for col, val in cond.items())

    if isinstance(filters, dict):
        return [r for r in rows if _matches(r, filters)]
    return [r for r in rows if any(_matches(r, item) for item in filters)]


async def lake_dates(router: Any, chain: str, from_block: int | None, to_block: int | None) -> list[str] | None:
    """Derive date partitions (``date=`` / ``dt=``) for a block range.

    The external lakes are date-partitioned and carry no block→date
    index, so deep queries need the calendar span of the block range.
    Resolves it with two Envio block lookups and caps the window at
    31 days.

    Returns:
        ``YYYY-MM-DD`` list, or None when it cannot be derived (callers
        then fall back to their own defaults).
    """
    if from_block is None or to_block is None:
        return None
    if not router.envio.available():
        return None
    try:
        lo = await asyncio.to_thread(router.envio.call, chain, "eth_getBlockByNumber", [hex(int(from_block)), False])
        hi = await asyncio.to_thread(router.envio.call, chain, "eth_getBlockByNumber", [hex(int(to_block)), False])
        if not lo or not hi or "timestamp" not in lo or "timestamp" not in hi:
            return None
        start = datetime.fromtimestamp(int(lo["timestamp"], 16), tz=UTC).date()
        end = datetime.fromtimestamp(int(hi["timestamp"], 16), tz=UTC).date()
        if end < start:
            start, end = end, start
        if (end - start).days > 31:
            start = end - timedelta(days=31)
        return [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]
    except Exception as exc:  # noqa: BLE001 — falls back to the default scope
        logger.warning("biglake_dates_failed", error=str(exc)[:100])
        return None


async def biglake_route(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Try the AWS public lake for ranges the indexed tiers could not serve.

    Derives date partitions for deep block ranges so historical queries
    hit the right files (the lake has no block→date index of its own).
    Best-effort: any failure returns an empty list so routing falls
    through to the RPC leg as before.
    """
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if not router.biglake.available():
        return []
    address = router._address_from_filters(filters) or router._single_address(filters)
    dates = await router._lake_dates(chain, from_block, to_block)
    try:
        rows = await asyncio.to_thread(
            router.biglake.query,
            table,
            chain,
            from_block=from_block,
            to_block=to_block,
            address=address,
            dates=dates,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
        logger.warning("biglake_route_failed", error=str(exc)[:120])
        return []
    filtered = router._filter_rows(table, rows, filters)
    if filtered:
        metrics_incr("biglake_routes")
    return filtered
