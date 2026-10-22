"""AWS BigLake + date-partition leg for :class:`QueryRouter`.

Derives date partitions for deep block ranges, queries the AWS public
Ethereum archive, and applies router filter semantics to its rows. Bound
as ``QueryRouter`` methods by the facade so the public surface is unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from lite_indexer.config import CHAIN_BLOCK_TIMES, DEFAULT_BLOCK_TIME
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.metrics import incr_labeled as metrics_labeled
from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.legs_common import CH_TABLE_REF

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


async def _envio_ts(router: Any, chain: str, block: int) -> int | None:
    """Exact block timestamp from Envio (free HyperRPC)."""
    if not router.envio.available():
        return None
    try:
        blk = await asyncio.to_thread(
            router.envio.call, chain, "eth_getBlockByNumber", [hex(int(block)), False]
        )
    except Exception as exc:  # noqa: BLE001 — estimate path takes over
        logger.warning("envio_call_failed", chain=chain, error=str(exc)[:120])
        return None
    if not blk or "timestamp" not in blk:
        return None
    raw = blk["timestamp"]
    return int(raw, 16) if isinstance(raw, str) else int(raw)


def _anchor(router: Any, chain: str) -> tuple[int, int] | None:
    """(block, epoch) of the newest indexed row — the estimate's fixed point."""
    client = router._ch_client()
    if client is None:
        return None
    table = CH_TABLE_REF["evm_transfers"]
    try:
        # ``execute`` returns a LIST of row tuples (not ``(rows, types)``
        # unless ``with_column_types=True``). Unpacking it as a 2-tuple
        # raised ValueError for this single-row aggregate, the exception
        # was swallowed by the ``except`` below, and every deep range
        # resolved to "dates_unresolved" — the lake was never read.
        rows = client.execute(
            f"SELECT max(block_number), max(block_timestamp) FROM {table} WHERE chain = %(chain)s",
            {"chain": chain},
        )
    except Exception as exc:  # noqa: BLE001 — no anchor means "unknown"
        logger.warning("lake_anchor_failed", chain=chain, error=str(exc)[:100])
        return None
    if not rows or rows[0][0] is None or rows[0][1] is None:
        logger.warning("lake_anchor_empty", chain=chain)
        return None
    block, ts = int(rows[0][0]), rows[0][1]
    if isinstance(ts, datetime):
        ts = ts if ts.tzinfo is not None else ts.replace(tzinfo=UTC)
        epoch = int(ts.timestamp())
    else:
        epoch = int(ts)
    return block, epoch


async def _block_ts(router: Any, chain: str, block: int) -> tuple[int, bool] | None:
    """(epoch, exact) for ``block``: Envio first, then a calibrated estimate.

    The estimate extrapolates from the newest indexed row with the chain's
    average block time (``CHAIN_BLOCK_TIMES``). It is only used to pick
    ``date=`` partitions, so a few minutes of error is harmless — callers
    pad the window. Returning None (rather than a guess) is what stops the
    lake from being read at the wrong month.
    """
    exact = await _envio_ts(router, chain, block)
    if exact is not None:
        return exact, True
    anchor = await asyncio.to_thread(_anchor, router, chain)
    if anchor is None:
        return None
    anchor_block, anchor_epoch = anchor
    rate = CHAIN_BLOCK_TIMES.get(chain, DEFAULT_BLOCK_TIME)
    return int(anchor_epoch - (anchor_block - int(block)) * rate), False


async def lake_dates(router: Any, chain: str, from_block: int | None, to_block: int | None) -> list[str] | None:
    """Derive date partitions (``date=`` / ``dt=``) for a block range.

    The external lakes are date-partitioned and carry no block→date
    index, so deep queries need the calendar span of the block range.
    Resolves it exactly via Envio, else by extrapolating from the newest
    indexed row with the chain's average block time (padded one day each
    side because an estimate can land either side of midnight).

    Returns:
        ``YYYY-MM-DD`` list, or None when it cannot be derived — callers
        MUST treat None as "do not read the lake", never as "today".
    """
    if from_block is None or to_block is None:
        return None
    lo = await _block_ts(router, chain, from_block)
    hi = await _block_ts(router, chain, to_block)
    if lo is None or hi is None:
        return None
    pad = timedelta(days=0) if (lo[1] and hi[1]) else timedelta(days=1)
    start = datetime.fromtimestamp(lo[0], tz=UTC).date() - pad
    end = datetime.fromtimestamp(hi[0], tz=UTC).date() + pad
    if end < start:
        start, end = end, start
    if (end - start).days > 31:
        start = end - timedelta(days=31)
    return [(start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)]


async def biglake_route(router: Any, req: LegRequest) -> tuple[list[dict[str, Any]], bool]:
    """Try the AWS public lake for ranges the indexed tiers could not serve.

    Returns ``(rows, truncated)``.

    Derives date partitions for deep block ranges so historical queries
    hit the right files (the lake has no block→date index of its own).
    Best-effort: any failure returns an empty list so routing falls
    through to the RPC leg as before.
    """
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    if not router.biglake.available():
        return [], False
    address = router._address_from_filters(filters) or router._single_address(filters)
    dates = await router._lake_dates(chain, from_block, to_block)
    if not dates:
        # Never fall back to the scope month: reading the CURRENT month for
        # an August range returns zero rows and looks like "no history",
        # which is the failure this tier exists to prevent.
        metrics_incr("biglake_no_dates")
        logger.info("biglake_skipped", reason="dates_unresolved", chain=chain,
                    from_block=from_block, to_block=to_block)
        return [], False
    try:
        rows, truncated = await asyncio.to_thread(
            router.biglake.query,
            table,
            chain,
            from_block=from_block,
            to_block=to_block,
            address=address,
            dates=dates,
            limit=limit,
        )
    except ValueError:
        raise
    except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
        logger.warning("biglake_route_failed", error=str(exc)[:120])
        return [], False
    filtered = router._filter_rows(table, rows, filters)
    if filtered:
        metrics_incr("biglake_routes")
    if truncated:
        # Surfaced on the leg return so the router result carries it: a
        # caller must be able to tell "the lake has 40 rows" from "the lake
        # has 40,000 and I saw 1000".
        metrics_labeled("biglake_truncated", table=table, limit=str(limit))
    return filtered, truncated
