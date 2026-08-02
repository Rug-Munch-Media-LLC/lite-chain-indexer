"""
Intelligent RPC routing — per-method routing, cost tracking, circuit breakers.

Three capabilities layered on top of the existing endpoint pool:

1. **Per-method routing** — heavy, throughput-oriented calls
   (``eth_getLogs``) prefer free public tiers; small, latency-sensitive
   calls (``eth_getTransactionReceipt``, ``eth_call``) prefer dedicated
   (API-key) nodes. :func:`select_endpoint_for_method` re-ranks the
   pool accordingly.

2. **Cost tracking** — an in-memory ledger records which endpoints are
   actually serving data vs. returning errors/rate-limits, keyed by
   method, so operators can see where free-tier budget is being spent.
   :func:`log_rpc_usage` emits a periodic summary.

3. **Per-endpoint circuit breaker** — unlike the existing failure
   backoff (which only penalizes), this breaker *opens* an endpoint
   after a burst of failures (skipping it entirely), half-opens after
   a cooldown to probe, and closes again on success. Integrates with
   :func:`lite_indexer.rpc.pool.get_best_endpoint` scoring.

All state is in-process and safe under asyncio (single-threaded event
loop, no locks required).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config import (
    CIRCUIT_COOLDOWN_SECONDS,
    CIRCUIT_FAILURE_THRESHOLD,
    CIRCUIT_WINDOW_SECONDS,
    RPC_METHOD_ROUTING,
    RPC_USAGE_MAX_ENDPOINTS,
)
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


# ── Cost / Usage Tracking ───────────────────────────────────────────────


@dataclass
class EndpointUsage:
    """Per-endpoint usage ledger entry."""

    endpoint: str
    calls: int = 0
    errors: int = 0
    rate_limits: int = 0
    bytes_received: int = 0
    last_error: str = ""
    last_ok_at: float = 0.0
    last_error_at: float = 0.0
    by_method: dict[str, int] = field(default_factory=dict)


class RpcUsageTracker:
    """Bounded in-memory ledger of per-endpoint RPC usage/cost.

    Records every call (success or failure) so operators can see which
    endpoints are actually serving data vs. burning budget on errors.
    The ledger is bounded to :data:`RPC_USAGE_MAX_ENDPOINTS` entries.
    """

    def __init__(self, max_endpoints: int = RPC_USAGE_MAX_ENDPOINTS) -> None:
        self._max_endpoints = max_endpoints
        self._ledger: dict[str, EndpointUsage] = {}

    def record(
        self,
        endpoint: str,
        method: str,
        *,
        ok: bool,
        bytes_received: int = 0,
        error: str = "",
        rate_limited: bool = False,
    ) -> None:
        """Record the outcome of one RPC call.

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method name.
            ok: Whether the call succeeded (returned usable data).
            bytes_received: Approximate response bytes (cost proxy).
            error: Error message on failure.
            rate_limited: Whether the call hit a 429.
        """
        now = time.time()
        usage = self._ledger.get(endpoint)
        if usage is None:
            if len(self._ledger) >= self._max_endpoints:
                # Evict the least-recently-used entry (oldest last_ok/error).
                oldest = min(
                    self._ledger,
                    key=lambda ep: min(
                        self._ledger[ep].last_ok_at or self._ledger[ep].last_error_at,
                        self._ledger[ep].last_error_at or self._ledger[ep].last_ok_at,
                    ),
                )
                del self._ledger[oldest]
            usage = EndpointUsage(endpoint=endpoint)
            self._ledger[endpoint] = usage

        usage.calls += 1
        usage.by_method[method] = usage.by_method.get(method, 0) + 1
        usage.bytes_received += max(0, bytes_received)
        if ok:
            usage.last_ok_at = now
        else:
            usage.errors += 1
            usage.last_error = (error or "unknown")[:200]
            usage.last_error_at = now
        if rate_limited:
            usage.rate_limits += 1

    def usage_for(self, endpoint: str) -> EndpointUsage | None:
        """Return the ledger entry for an endpoint, or None."""
        return self._ledger.get(endpoint)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Return the full ledger as plain dicts (for logging/metrics)."""
        return {
            ep: {
                "calls": u.calls,
                "errors": u.errors,
                "rate_limits": u.rate_limits,
                "bytes": u.bytes_received,
                "last_error": u.last_error,
                "methods": dict(u.by_method),
            }
            for ep, u in self._ledger.items()
        }

    def worst_endpoints(self, limit: int = 10) -> list[tuple[str, float]]:
        """Return the endpoints with the highest error ratio.

        Args:
            limit: Max entries to return.

        Returns:
            List of ``(endpoint, error_ratio)`` sorted worst-first.
        """
        ranked: list[tuple[str, float]] = []
        for ep, u in self._ledger.items():
            ratio = (u.errors / u.calls) if u.calls else 0.0
            if u.calls:
                ranked.append((ep, ratio))
        ranked.sort(key=lambda item: item[1], reverse=True)
        return ranked[:limit]

    def reset(self) -> None:
        """Clear the ledger."""
        self._ledger.clear()


# Module-level tracker shared across the codebase.
USAGE_TRACKER = RpcUsageTracker()


def record_rpc_usage(
    endpoint: str,
    method: str,
    *,
    ok: bool,
    bytes_received: int = 0,
    error: str = "",
    rate_limited: bool = False,
) -> None:
    """Record usage in the shared tracker (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
        ok: Whether the call succeeded.
        bytes_received: Approximate response bytes.
        error: Error message on failure.
        rate_limited: Whether the call hit a 429.
    """
    USAGE_TRACKER.record(
        endpoint,
        method,
        ok=ok,
        bytes_received=bytes_received,
        error=error,
        rate_limited=rate_limited,
    )


def log_rpc_usage(limit: int = 10) -> dict[str, dict[str, Any]]:
    """Log a summary of RPC usage (worst error ratios first) and return it.

    Args:
        limit: Max endpoints to include in the summary log.

    Returns:
        The full usage snapshot dict.
    """
    snapshot = USAGE_TRACKER.snapshot()
    if not snapshot:
        logger.info("rpc_usage_empty")
        return snapshot

    total_calls = sum(u["calls"] for u in snapshot.values())
    total_errors = sum(u["errors"] for u in snapshot.values())
    total_bytes = sum(u["bytes"] for u in snapshot.values())
    worst = USAGE_TRACKER.worst_endpoints(limit)
    logger.info(
        "rpc_usage_summary",
        endpoints=len(snapshot),
        total_calls=total_calls,
        total_errors=total_errors,
        total_bytes_mb=round(total_bytes / 1e6, 2),
        worst=[{"endpoint": ep[:60], "error_ratio": round(r, 3)} for ep, r in worst],
    )
    return snapshot


# ── Per-Endpoint Circuit Breaker ────────────────────────────────────────


class RpcCircuitBreaker:
    """Per-endpoint circuit breaker (open / half-open / closed).

    An endpoint is tripped *open* when it records
    :data:`CIRCUIT_FAILURE_THRESHOLD` failures within
    :data:`CIRCUIT_WINDOW_SECONDS`. While open it is skipped entirely
    (``allow`` returns False). After :data:`CIRCUIT_COOLDOWN_SECONDS` it
    half-opens: one probe call is allowed; success closes the circuit,
    another failure re-opens it.

    This is complementary to the exponential backoff in the pool —
    backoff slows retries, the breaker stops wasting calls on dead
    endpoints entirely.
    """

    def __init__(
        self,
        failure_threshold: int = CIRCUIT_FAILURE_THRESHOLD,
        window_seconds: float = CIRCUIT_WINDOW_SECONDS,
        cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS,
    ) -> None:
        self._threshold = failure_threshold
        self._window = window_seconds
        self._cooldown = cooldown_seconds
        # endpoint -> (state, opened_at, failures)
        self._state: dict[str, tuple[str, float, list[float]]] = {}

    def _entry(self, endpoint: str) -> tuple[str, float, list[float]]:
        state = self._state.get(endpoint)
        if state is None:
            state = ("closed", 0.0, [])
            self._state[endpoint] = state
        return state

    def state(self, endpoint: str) -> str:
        """Return the breaker state: ``closed``, ``open``, or ``half_open``.

        Args:
            endpoint: RPC endpoint URL.
        """
        state, opened_at, _ = self._entry(endpoint)
        if state == "open" and time.time() - opened_at >= self._cooldown:
            return "half_open"
        return state

    def allow(self, endpoint: str) -> bool:
        """Whether a call to this endpoint may proceed now.

        Closed → allowed. Half-open → exactly one probe allowed (the
        caller flips it to "open" on failure via :meth:`record_failure`).
        Open → denied until the cooldown elapses.

        Args:
            endpoint: RPC endpoint URL.

        Returns:
            True when the call may proceed.
        """
        st = self.state(endpoint)
        if st == "closed":
            return True
        if st == "half_open":
            # Allow one probe, but flip to a probing state so concurrent
            # callers don't all hammer it: we mark it as "open" with a
            # zero cooldown; record_success/failure resolve it.
            self._state[endpoint] = ("probe", 0.0, self._entry(endpoint)[2])
            return True
        return False

    def record_success(self, endpoint: str) -> None:
        """Close the circuit after a successful call/probe.

        Args:
            endpoint: RPC endpoint URL.
        """
        self._state[endpoint] = ("closed", 0.0, [])

    def record_failure(self, endpoint: str) -> None:
        """Record a failure; trip the circuit open when the threshold is met.

        Args:
            endpoint: RPC endpoint URL.
        """
        st, opened_at, failures = self._entry(endpoint)
        now = time.time()
        failures = [f for f in failures if now - f < self._window]
        failures.append(now)

        if st == "probe" or len(failures) >= self._threshold:
            self._state[endpoint] = ("open", now, failures)
            logger.warning("rpc_circuit_opened", endpoint=endpoint[:60], failures=len(failures))
            return
        self._state[endpoint] = ("closed", 0.0, failures)

    def is_open(self, endpoint: str) -> bool:
        """Whether calls to this endpoint should currently be skipped.

        Args:
            endpoint: RPC endpoint URL.
        """
        return self.state(endpoint) == "open"


# Module-level breaker shared across the codebase.
CIRCUIT_BREAKER = RpcCircuitBreaker()


def is_circuit_open(endpoint: str) -> bool:
    """Check the shared breaker for an endpoint (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        True when the circuit is open (skip this endpoint).
    """
    return CIRCUIT_BREAKER.is_open(endpoint)


__all__ = [
    "RpcUsageTracker",
    "RpcCircuitBreaker",
    "USAGE_TRACKER",
    "CIRCUIT_BREAKER",
    "endpoint_tier",
    "preferred_tier_for_method",
    "select_endpoint_for_method",
    "record_rpc_usage",
    "log_rpc_usage",
    "is_circuit_open",
]
