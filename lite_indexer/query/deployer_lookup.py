"""Deployer lookup for :class:`lite_indexer.query.router.QueryRouter`.

Extracted from ``query/router.py`` (shrink-only size ratchet). Kept as a free
function taking the router so the class method stays a one-line delegate —
callers (the API, the CLI, tests) keep using ``router.get_deployer_info()``,
and a test that overrides the method on a fake router still works.

Route: ClickHouse → cold store → on-demand RPC, with the creation-tx hint.
"""

from __future__ import annotations

from typing import Any, cast

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)


async def get_deployer_info(router: Any, chain: str, address: str) -> dict[str, Any] | None:
    """Get deployer info for a contract. Routes: CH → Cold → RPC.

    Matches ``deployer_events`` by pair/token/deployer address. When the
    indexed store has a row whose EOA was never resolved, the row's tx hash
    is passed to the RPC leg as the creation-tx hint so
    ``eth_getTransactionByHash`` can resolve the deployer on demand.

    Args:
        router: The :class:`QueryRouter` owning the tiers.
        chain: Chain name.
        address: Contract / pair / deployer address.

    Returns:
        A deployer row (``deployer_events`` shape), or None when not found.
    """
    addr = str(address or "").strip().lower()
    if not addr:
        return None
    rows = await router._route_query(
        "deployer_events",
        chain,
        filters=[
            {"col": "pair_address", "value": addr},
            {"col": "token_address", "value": addr},
            {"col": "deployer_eoa", "value": addr},
        ],
        limit=5,
        use_rpc=False,
    )
    if rows:
        best = next((r for r in rows if r.get("deployer")), None)
        if best is not None:
            return cast("dict[str, Any]", best)
        hint = rows[0].get("tx_hash") or None
    else:
        hint = None

    if not router.on_demand_enabled:
        return rows[0] if rows else None

    cache_key = ("deployer", chain, addr)
    cached = router.cache.get(cache_key)
    if cached is not None:
        return cast("dict[str, Any]", dict(cached))
    try:
        result = await router.rpc.fetch_deployer(chain, addr, creation_tx=hint)
    except Exception as exc:  # noqa: BLE001 — RPC is best-effort
        metrics_incr("ondemand_rpc_errors")
        logger.warning("ondemand_deployer_failed", chain=chain, address=addr, error=str(exc)[:100])
        return rows[0] if rows else None
    if result:
        router.cache.set(cache_key, result)
        return cast("dict[str, Any]", result)
    return rows[0] if rows else None


__all__ = ["get_deployer_info"]
