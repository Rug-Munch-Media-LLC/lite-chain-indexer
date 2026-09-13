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


def method_class(method: str) -> str:
    """Classify a method for spillover telemetry: ``archive`` or ``tip``."""
    return "archive" if method_requires_archive(method) else "tip"


def _is_endpoint_available(endpoint: str) -> bool:
    """Whether an endpoint may be routed to *now* (not tripped/cooling).

    Combines the existing health signals: hard ban, open circuit breaker,
    failed verification, and an active throttle backoff window. Unknown
    endpoints are treated as available (pre-verification default). Any
    signal lookup error degrades to available so selection can never crash.
    """
    try:
        from lite_indexer.rpc.health import is_endpoint_healthy
        from lite_indexer.rpc.pool import is_endpoint_banned
        from lite_indexer.rpc.routing.health import is_circuit_open
        from lite_indexer.rpc.throttle import RATE_TRACKER
    except Exception:  # noqa: BLE001 — availability must never crash selection
        return True
    if is_endpoint_banned(endpoint):
        return False
    if is_circuit_open(endpoint):
        return False
    if not is_endpoint_healthy(endpoint):
        return False
    return bool(RATE_TRACKER.allow(endpoint))


def _archive_candidates(method: str, endpoints: list[str], chain: str | None) -> list[str]:
    """Narrow archive-class candidates to HEALTHY archive-capable endpoints.

    Only endpoints with a KNOWN capability record are considered when any
    exist (an already-probed non-archive endpoint is excluded). When NO
    candidate has been probed yet, selection falls back to the legacy pool —
    this preserves the pre-verification behavior. When capabilities are
    known, candidates must be archive-capable AND currently healthy
    (free OR keyed); if none qualify the call fails closed with
    :class:`NoArchiveEndpointError` (never route historical calls at a
    non-archive node).
    """
    known = [ep for ep in endpoints if get_capability(ep) is not None]
    if not known:
        return endpoints
    capable = [ep for ep in known if is_archive_capable(ep, method) and _is_endpoint_available(ep)]
    _warn_archive_scarcity(chain, method, capable)
    if not capable:
        raise NoArchiveEndpointError(method, chain, endpoints)
    return capable


# ── Endpoint Tier Classification ────────────────────────────────────────


def endpoint_tier(endpoint: str) -> str:
    """Classify an endpoint as ``dedicated`` or ``public``.

    Generic, no provider allow/deny list: an endpoint carrying an API key
    (unresolved ``${VAR}`` placeholder or a derivable embedded key) is
    ``dedicated`` — good for small latency-sensitive calls under its account
    budget — while a keyless endpoint is ``public`` and handles heavy log
    queries. Unknown/empty URLs are ``unknown``.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        One of ``"public"``, ``"dedicated"``, or ``"unknown"``.
    """
    from lite_indexer.rpc.ledger import key_id_for_endpoint

    if not endpoint:
        return "unknown"
    if "${" in endpoint or key_id_for_endpoint(endpoint):
        return "dedicated"
    return "public"


def preferred_tier_for_method(method: str) -> str:
    """Return the preferred endpoint tier for an RPC method.

    Args:
        method: JSON-RPC method name (e.g. ``eth_getLogs``).

    Returns:
        ``"public"`` or ``"dedicated"`` per :data:`RPC_METHOD_ROUTING`.
    """
    return RPC_METHOD_ROUTING.get(method, "public")


def _tier_matches(endpoint: str, preferred: str) -> bool:
    """Whether ``endpoint`` belongs to the preferred routing tier."""
    is_dedicated = endpoint_tier(endpoint) == "dedicated"
    return is_dedicated if preferred == "dedicated" else not is_dedicated


def select_endpoint_for_method(
    method: str,
    endpoints: list[str],
    *,
    chain: str | None = None,
    base_select: Callable[[list[str]], str] | None = None,
) -> str:
    """Pick the best endpoint for ``method`` with tier-aware spillover.

    Endpoints matching the method's preferred tier are used first (free-first
    for heavy methods; keyed-first for latency-sensitive ones). ARCHIVE-class
    methods (``eth_getLogs``, ``eth_getProof``, ``debug_*``, ``trace_*``) are
    restricted to healthy archive-capable endpoints and fail closed.

    FREEMIUM SPILLOVER: when the preferred tier has NO healthy endpoint
    (all banned/tripped/cooling), selection escalates to the other tier
    rather than dead-ending — a keyed endpoint serves the call under its
    account caps. Emits an ``rpc_spillover`` metric + log.

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
            capabilities but no healthy archive-capable candidate in either
            tier.
    """
    from lite_indexer.rpc.pool import get_best_endpoint

    select = base_select or get_best_endpoint
    candidates = endpoints
    if method_requires_archive(method):
        candidates = _archive_candidates(method, endpoints, chain)

    preferred = preferred_tier_for_method(method)
    matching = [ep for ep in candidates if _tier_matches(ep, preferred)]
    other = [ep for ep in candidates if not _tier_matches(ep, preferred)]

    available_primary = [ep for ep in matching if _is_endpoint_available(ep)]
    if available_primary:
        return select(available_primary)

    available_other = [ep for ep in other if _is_endpoint_available(ep)]
    if available_other:
        metrics_incr("rpc_spillover")
        logger.warning(
            "rpc_spillover",
            chain=chain,
            method=method,
            method_class=method_class(method),
            preferred_tier=preferred,
            reason="primary_tier_exhausted",
            spillover_endpoints=len(available_other),
        )
        return select(available_other)

    # Neither tier healthy — preserve the legacy selection shape so callers
    # still get an endpoint (pool scoring / desperation probe handles it).
    fallback = matching or candidates
    if chain:
        logger.debug(
            "method_routing_fallback",
            chain=chain,
            method=method,
            preferred=preferred,
        )
    return select(fallback)


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
    capable = [
        ep
        for ep in endpoints
        if get_capability(ep) is not None and is_archive_capable(ep, method) and _is_endpoint_available(ep)
    ]
    _warn_archive_scarcity(chain, method, capable)
    if not capable:
        raise NoArchiveEndpointError(method, chain, endpoints)
    select = base_select or _pool_best_endpoint()
    return select(capable)


def _pool_best_endpoint() -> Callable[[list[str]], str]:
    """Late import of the pool selector (avoids an import cycle)."""
    from lite_indexer.rpc.pool import get_best_endpoint

    return get_best_endpoint
