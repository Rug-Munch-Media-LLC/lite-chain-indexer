"""Per-method endpoint routing — tier classification and selection.

Heavy, throughput-oriented calls (``eth_getLogs``) prefer free public
tiers; small, latency-sensitive calls (``eth_getTransactionReceipt``,
``eth_call``) prefer dedicated (API-key) nodes.
:func:`select_endpoint_for_method` re-ranks the pool accordingly.

Split out of the former monolithic ``rpc/routing.py``.
"""

from __future__ import annotations

from collections.abc import Callable

from lite_indexer.config import RPC_METHOD_ROUTING
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Endpoint Tier Classification ────────────────────────────────────────

# Domain substrings identifying dedicated (API-key / paid) providers.
_DEDICATED_MARKERS = (
    "alchemy.com",
    "getblock.io",
    "chainstack.com",
    "quicknode",
    "infura.io",
    "helius.xyz",
    "blastapi.io",
)


def endpoint_tier(endpoint: str) -> str:
    """Classify an endpoint as ``public``, ``dedicated``, or ``unknown``.

    Dedicated = API-key/paid providers that are good for small
    latency-sensitive calls; public = free tiers that handle heavy
    log queries. The classification is domain-based and conservative.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        One of ``"public"``, ``"dedicated"``, ``"unknown"``.
    """
    from urllib.parse import urlparse

    domain = urlparse(endpoint).hostname or endpoint
    for marker in _DEDICATED_MARKERS:
        if marker in domain:
            return "dedicated"
    if any(m in domain for m in ("drpc.org", "publicnode.com", "tenderly.co", "gateway.tatum.io")):
        return "public"
    return "unknown"


def preferred_tier_for_method(method: str) -> str:
    """Return the preferred endpoint tier for an RPC method.

    Args:
        method: JSON-RPC method name (e.g. ``eth_getLogs``).

    Returns:
        ``"public"`` or ``"dedicated"`` per :data:`RPC_METHOD_ROUTING`.
    """
    return RPC_METHOD_ROUTING.get(method, "public")


def select_endpoint_for_method(
    method: str,
    endpoints: list[str],
    *,
    chain: str | None = None,
    base_select: Callable[[list[str]], str] | None = None,
) -> str:
    """Pick the best endpoint for ``method`` using per-method routing.

    Endpoints matching the method's preferred tier are boosted; the
    pool's health scoring still applies. Falls back to the pool's
    default selection when no endpoints match the preferred tier.

    Args:
        method: JSON-RPC method name.
        endpoints: Candidate endpoint list.
        chain: Chain name (logging only).
        base_select: Optional selection function with the same
            signature as :func:`lite_indexer.rpc.pool.get_best_endpoint`
            (defaults to it). Injectable for tests.

    Returns:
        The selected endpoint URL.
    """
    from lite_indexer.rpc.pool import get_best_endpoint

    select = base_select or get_best_endpoint
    preferred = preferred_tier_for_method(method)

    if preferred == "dedicated":
        dedicated = [ep for ep in endpoints if endpoint_tier(ep) == "dedicated"]
        if dedicated:
            return select(dedicated)
    else:
        public = [ep for ep in endpoints if endpoint_tier(ep) != "dedicated"]
        if public:
            return select(public)

    if chain:
        logger.debug(
            "method_routing_fallback",
            chain=chain,
            method=method,
            preferred=preferred,
        )
    return select(endpoints)
