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
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.capabilities import (
    NoArchiveEndpointError,
    get_capability,
    is_archive_capable,
    method_requires_archive,
)

logger = get_logger(__name__)

# Chains/methods already warned about archive-endpoint scarcity (never spam).
_archive_warned: set[tuple[str, str]] = set()

# Warn when fewer than this many archive-capable endpoints remain for a chain.
MIN_ARCHIVE_ENDPOINTS = 2


def _warn_archive_scarcity(chain: str | None, method: str, capable: list[str]) -> None:
    """Warn (once per chain+method) when archive endpoints are scarce.

    Operating with <2 archive-capable endpoints per chain leaves no failover:
    one 429/outage and every ARCHIVE call fails closed. Emits a metric and a
    warning without raising (0 endpoints is handled by the caller's raise).
    """
    if len(capable) >= MIN_ARCHIVE_ENDPOINTS:
        return
    key = (chain or "", method)
    if key in _archive_warned:
        return
    _archive_warned.add(key)
    metrics_incr("rpc_archive_endpoints_low")
    logger.warning(
        "rpc_archive_endpoints_low",
        chain=chain,
        method=method,
        archive_endpoints=len(capable),
        required=MIN_ARCHIVE_ENDPOINTS,
    )


def _archive_candidates(method: str, endpoints: list[str], chain: str | None) -> list[str]:
    """Narrow archive-class candidates to archive-capable, fail-closed.

    Only endpoints with a KNOWN capability record are considered when any
    exist (an already-probed non-archive endpoint is excluded). When NO
    candidate has been probed yet, selection falls back to the legacy pool —
    this preserves the pre-verification behavior. If the registry is known
    and holds no archive-capable endpoint, raise :class:`NoArchiveEndpointError`
    (never silently route historical calls at a non-archive node).
    """
    known = [ep for ep in endpoints if get_capability(ep) is not None]
    if not known:
        return endpoints
    capable = [ep for ep in known if is_archive_capable(ep, method)]
    _warn_archive_scarcity(chain, method, capable)
    if not capable:
        raise NoArchiveEndpointError(method, chain, endpoints)
    return capable


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


def _tier_candidates(method: str, endpoints: list[str]) -> list[str]:
    """Endpoints matching the method's preferred tier (or the full pool)."""
    preferred = preferred_tier_for_method(method)
    if preferred == "dedicated":
        matching = [ep for ep in endpoints if endpoint_tier(ep) == "dedicated"]
    else:
        matching = [ep for ep in endpoints if endpoint_tier(ep) != "dedicated"]
    return matching or endpoints


def select_endpoint_for_method(
    method: str,
    endpoints: list[str],
    *,
    chain: str | None = None,
    base_select: Callable[[list[str]], str] | None = None,
) -> str:
    """Pick the best endpoint for ``method`` using per-method routing.

    Endpoints matching the method's preferred tier are boosted; the
    pool's health scoring still applies. ARCHIVE-class methods
    (``eth_getLogs``, ``eth_getProof``, ``debug_*``, ``trace_*``) are
    additionally restricted to archive-capable endpoints and fail closed
    (see :func:`_archive_candidates`).

    Args:
        method: JSON-RPC method name.
        endpoints: Candidate endpoint list.
        chain: Chain name (logging only).
        base_select: Optional selection function with the same
            signature as :func:`lite_indexer.rpc.pool.get_best_endpoint`
            (defaults to it). Injectable for tests.

    Returns:
        The selected endpoint URL.

    Raises:
        NoArchiveEndpointError: ARCHIVE-class method with known
            capabilities but no archive-capable candidate.
    """
    from lite_indexer.rpc.pool import get_best_endpoint

    select = base_select or get_best_endpoint
    candidates = endpoints
    if method_requires_archive(method):
        candidates = _archive_candidates(method, endpoints, chain)

    tiered = _tier_candidates(method, candidates)
    if chain and tiered is candidates:
        logger.debug(
            "method_routing_fallback",
            chain=chain,
            method=method,
            preferred=preferred_tier_for_method(method),
        )
    return select(tiered)


def select_archive_endpoint(
    method: str,
    endpoints: list[str],
    *,
    chain: str | None = None,
    base_select: Callable[[list[str]], str] | None = None,
) -> str:
    """Strictly select an archive-capable endpoint for ``method``.

    Unlike :func:`select_endpoint_for_method`, this never falls back when
    capabilities are unknown: it is the fail-closed entry point for
    callers that have already verified the pool. Raises
    :class:`NoArchiveEndpointError` when no candidate is archive-capable.
    """
    if not method_requires_archive(method):
        raise ValueError(f"{method} is not an archive-class method")
    capable = [ep for ep in endpoints if get_capability(ep) is not None and is_archive_capable(ep, method)]
    _warn_archive_scarcity(chain, method, capable)
    if not capable:
        raise NoArchiveEndpointError(method, chain, endpoints)
    select = base_select or _pool_best_endpoint()
    return select(capable)


def _pool_best_endpoint() -> Callable[[list[str]], str]:
    """Late import of the pool selector (avoids an import cycle)."""
    from lite_indexer.rpc.pool import get_best_endpoint

    return get_best_endpoint
