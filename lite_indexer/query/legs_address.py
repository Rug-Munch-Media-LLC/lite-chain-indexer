"""Address-activity leg for :class:`QueryRouter`.

Wallet history served from the ClickHouse keystone table with a lazy
first-touch backfill from the free RPC pool. Bound as ``QueryRouter``
methods by the facade so the public surface is unchanged.
"""

from __future__ import annotations

import asyncio
from typing import Any

from lite_indexer.config import CH_ADDRESS_ACTIVITY_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.address_backfill import AddressBackfill, BackfillResult
from lite_indexer.query.lake_limits import reject_oversize_limit

logger = get_logger(__name__)


def get_backfill(router: Any) -> AddressBackfill:
    """Return the shared :class:`AddressBackfill` (lazily constructed)."""
    if router._backfill is None:
        router._backfill = AddressBackfill(ch_client_factory=router._ch_client)
    return router._backfill


def query_address_activity(router: Any, chain: str, address: str, limit: int) -> list[dict[str, Any]]:
    """Read one address's rows from the keystone table ([] on failure)."""
    client = router._ch_client()
    if client is None:
        return []
    sql = (
        "SELECT address, tx_signature, chain, slot, block_time, involved_accounts, program_ids, is_signer "
        f"FROM {CH_ADDRESS_ACTIVITY_TABLE} FINAL "
        "WHERE address = %(address)s AND chain = %(chain)s "
        "ORDER BY block_time DESC, slot DESC "
        f"LIMIT {int(limit)}"
    )
    try:
        rows, column_types = client.execute(sql, {"address": address, "chain": chain}, with_column_types=True)
    except Exception as exc:  # noqa: BLE001 — reads are best-effort
        metrics_incr("ondemand_ch_errors")
        logger.debug("ondemand_address_activity_failed", error=str(exc)[:100])
        return []
    names = [name for name, _ in column_types]
    return [dict(zip(names, row, strict=False)) for row in rows]


async def backfill_address(
    router: Any,
    chain: str,
    address: str,
    *,
    force: bool = False,
    max_tx: int | None = None,
) -> BackfillResult:
    """Backfill one address's history into ``address_activity`` (guarded).

    Delegates to :class:`AddressBackfill`; the never-re-fetch guard makes
    repeated calls a no-op once the address is indexed. Best-effort —
    never raises.
    """
    try:
        return await router._get_backfill().backfill_address(address, chain=chain, force=force, max_tx=max_tx)
    except Exception as exc:  # noqa: BLE001 — backfill is best-effort
        logger.warning(
            "ondemand_backfill_failed",
            chain=chain,
            address=str(address)[:12],
            error=str(exc)[:120],
        )
        return BackfillResult(address=str(address or ""), chain=chain, skipped=True, reason="error")


async def get_address_activity(
    router: Any,
    chain: str,
    address: str,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Serve a wallet's transaction history, backfilling on first touch.

    Lazy backfill: on a cache miss the address is fetched once from the
    free RPC pool and written to ``wallet_memory.address_activity``, then
    re-read from ClickHouse. Every later call is a local read. Gated by
    ``on_demand_enabled`` and the backfill's own guard.
    """
    addr = str(address or "").strip()
    if not addr:
        return []
    # Reject an oversize limit instead of clamping to 1000: clamping here
    # returned the newest 1000 rows as if they were the whole answer. This
    # is a local ClickHouse read, but the failure mode is identical to the
    # lake legs, so it gets the same treatment.
    capped = reject_oversize_limit(limit or router.limit_default)
    rows = await asyncio.to_thread(router._query_address_activity, chain, addr, capped)
    if rows or not router.on_demand_enabled:
        return rows
    try:
        await router.backfill_address(chain, addr)
    except Exception as exc:  # noqa: BLE001 — lazy backfill must not break reads
        logger.warning("ondemand_lazy_backfill_failed", chain=chain, error=str(exc)[:120])
        return rows
    return await asyncio.to_thread(router._query_address_activity, chain, addr, capped)
