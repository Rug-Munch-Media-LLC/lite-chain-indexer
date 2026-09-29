"""Pick an endpoint for RPC calls that need a block hash.

``EVMChainStrategy.reorg_stage`` asks ONE endpoint for a TIP BLOCK HASH
(``eth_getBlockByNumber``) and fails CLOSED when it gets ``None`` — that is
the ``reorg_status_unknown`` stall. :func:`get_best_endpoint` is the wrong
selector for that job:

* it ends in a random power-of-two-choices draw, so pool ORDER alone does not
  decide which endpoint is handed out;
* it does not consult the logs-capability quarantine, and a getLogs-
  incapable endpoint is frequently a *tip-hash*-incapable one too.

Hyperliquid is the live case: its OFFICIAL RPC answers ``eth_blockNumber``
(so it passes the liveness probe and is never quarantined) but refuses
every ``eth_getBlockByNumber`` with ``-32005 "More than 3000 archived blocks
queried in one day"``. Handing it to ``reorg_stage`` stalls the chain.

So the reorg probe uses this helper: it ranks the pool by TIP-HASH
capability (endpoints known tip-hash capable first, unknown after) and only
then lets the normal health/rotation scoring pick, so a chain that has never
probed its pool is still served rather than refused.
"""

from __future__ import annotations

from collections.abc import Sequence

from lite_indexer.logging import get_logger
from lite_indexer.rpc.pool import get_best_endpoint

logger = get_logger(__name__)

#: Endpoints that are known NOT to serve a block hash, keyed by chain.
#: Seeded with the live-verified 2026-09-26 finding; the registry is
#: advisory and never removes an endpoint from the pool — it only stops
#: the reorg probe from being handed an endpoint that cannot answer it.
TIP_HASH_INCAPABLE: dict[str, frozenset[str]] = {
    "hyperliquid": frozenset({"https://rpc.hyperliquid.xyz/evm"}),
}


def tip_hash_capable_endpoints(chain: str, endpoints: Sequence[str]) -> list[str]:
    """Return ``endpoints`` ordered tip-hash-capable first.

    Args:
        chain: Chain name (selects the known-incapable set).
        endpoints: The chain's configured RPC pool.

    Returns:
        A new list: every known-capable endpoint, then every unclassified one.
        Endpoints recorded as tip-hash incapable are dropped from the
        head but still returned last, so the caller can fall back to them
        if the chain has no capable endpoint at all.
    """
    incapable = TIP_HASH_INCAPABLE.get(chain, frozenset())
    if not incapable:
        return list(endpoints)
    capable = [ep for ep in endpoints if ep not in incapable]
    incapable_present = [ep for ep in endpoints if ep in incapable]
    logger.debug(
        "reorg_tip_capable_pool_ordered",
        chain=chain,
        capable=len(capable),
        dropped_to_tail=len(incapable_present),
    )
    return capable + incapable_present


def get_tip_hash_endpoint(chain: str, endpoints: list[str]) -> str:
    """Return the pool endpoint to use for a TIP BLOCK HASH read.

    Tips are still preferred when the chain has at least one known-capable
    endpoint, so a temporarily broken preferred endpoint falls through to a
    healthy peer instead of hard-failing the reorg stage.
    """
    incapable = TIP_HASH_INCAPABLE.get(chain, frozenset())
    if incapable:
        known_capable = [ep for ep in endpoints if ep not in incapable]
        if known_capable:
            return get_best_endpoint(known_capable)
    return get_best_endpoint(list(endpoints))


__all__ = [
    "TIP_HASH_INCAPABLE",
    "get_tip_hash_endpoint",
    "tip_hash_capable_endpoints",
]
