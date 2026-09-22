"""On-demand RPC leg for :class:`QueryRouter`.

Last-resort live fetch (cached, budget-guarded) for anything the indexed
and external tiers could not answer. Bound as ``QueryRouter`` methods by
the facade so the public surface is unchanged.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.leg_request import LegRequest

logger = get_logger(__name__)



async def rpc_route(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Route a query to the on-demand RPC fetcher, cached."""
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    cache_key = req.cache_key()
    cached = router.cache.get(cache_key)
    if cached is not None:
        return list(cached)

    rows: list[dict[str, Any]] = []
    try:
        if table == "evm_transfers":
            if isinstance(filters, dict) and filters.get("tx_hash"):
                row = await router.rpc.fetch_transfer(chain, str(filters["tx_hash"]))
                rows = [row] if row else []
            else:
                addr = router._address_from_filters(filters)
                if addr is not None:
                    rows = await router.rpc.fetch_address_transfers(chain, addr, from_block, to_block, limit=limit)
        elif table == "deployer_events":
            contract = router._single_address(filters)
            if contract is not None:
                row = await router.rpc.fetch_deployer(chain, contract)
                rows = [row] if row else []
    except Exception as exc:  # noqa: BLE001 — RPC is best-effort, never a query-killer
        metrics_incr("ondemand_rpc_errors")
        logger.warning("ondemand_rpc_failed", table=table, chain=chain, error=str(exc)[:100])
        return []

    if rows:
        router.cache.set(cache_key, list(rows))
    return rows
