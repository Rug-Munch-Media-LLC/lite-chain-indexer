"""
RPC endpoint pool — best-endpoint selection, failure tracking, backoff.

Provides load-balanced endpoint selection, exponential backoff on
failures, reactive rate-limit handling after 429 responses, and a
per-endpoint circuit breaker (open after a failure burst) layered on
top of the classic scoring. 429 rate limits trigger backoff only — they
do NOT trip the circuit breaker or hard-ban (see
:func:`record_rate_limit` / :func:`handle_rate_limit`).
"""

from __future__ import annotations

import asyncio
import random
import time

from lite_indexer.config import (
    BACKOFF_CAP,
    ENDPOINT_BAN_BASE_SECONDS,
    ENDPOINT_BAN_CAP_SECONDS,
    ENDPOINT_COOLDOWN,
    MAX_FAILURES,
    RATE_LIMIT_WAIT_CAP,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ── State ──────────────────────────────────────────────────────────────
_endpoint_failures: dict[str, list[float]] = {}
# Hard bans for 403/429 responses: url -> banned_until (epoch seconds).
# These providers explicitly rate-limit / blacklist the endpoint for a
# while — hammering them only deepens the ban, so they get an
# exponentially growing "do not touch" window instead of the plain
# ENDPOINT_COOLDOWN used for generic failures.
_endpoint_bans: dict[str, float] = {}
# url -> ban count (feeds the exponential backoff curve).
_endpoint_ban_level: dict[str, int] = {}


# ── Hard Ban (403/429) ───────────────────────────────────────────────


def ban_endpoint(endpoint: str, reason: str = "") -> None:
    """Ban an endpoint that returned 403/429 for an exponential duration.

    Each consecutive ban doubles the window: base * 2^level, capped at
    ``ENDPOINT_BAN_CAP_SECONDS``. A single success calls
    :func:`unban_endpoint` to clear the ban.

    Args:
        endpoint: RPC endpoint URL.
        reason: Human-readable reason (e.g. ``http_429``) for logging.
    """
    level = _endpoint_ban_level.get(endpoint, 0)
    duration = min(ENDPOINT_BAN_BASE_SECONDS * (2**level), ENDPOINT_BAN_CAP_SECONDS)
    _endpoint_bans[endpoint] = time.time() + duration
    _endpoint_ban_level[endpoint] = level + 1
    metrics_incr("endpoints_banned")
    logger.warning(
        "endpoint_banned",
        endpoint=endpoint[:60],
        reason=reason,
        level=level,
        duration_s=duration,
    )


def unban_endpoint(endpoint: str) -> None:
    """Clear a hard ban after a successful call (ban level resets)."""
    if endpoint in _endpoint_bans:
        _endpoint_bans.pop(endpoint, None)
        _endpoint_ban_level.pop(endpoint, None)
        logger.debug("endpoint_unbanned", endpoint=endpoint[:60])


def is_endpoint_banned(endpoint: str) -> bool:
    """Whether an endpoint is currently under a 403/429 hard ban.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        True while the ban window is active (the endpoint should be
        skipped entirely).
    """
    banned_until = _endpoint_bans.get(endpoint)
    if banned_until is None:
        return False
    if time.time() >= banned_until:
        # Ban window elapsed — clear it lazily.
        _endpoint_bans.pop(endpoint, None)
        return False
    return True


def pool_fully_down(endpoints: list[str]) -> bool:
    """Whether every endpoint in a pool is banned or circuit-open.

    This is the pool-wide outage signal that triggers the desperation
    probe: all endpoints are currently unusable, so the fetch path must
    not hammer the whole pool every cycle. Instead it force-probes the
    least-banned endpoint at most once per desperation interval.

    Args:
        endpoints: Pool candidate URLs (may be empty).

    Returns:
        True when the pool is non-empty and every endpoint is banned or
        circuit-open.
    """
    if not endpoints:
        return False
    from lite_indexer.rpc.routing import is_circuit_open

    return all(is_endpoint_banned(ep) or is_circuit_open(ep) for ep in endpoints)


def least_banned_endpoint(endpoints: list[str]) -> str:
    """Return the endpoint with the lowest combined pool penalty.

    Mirrors the :func:`get_best_endpoint` scoring (hard ban + circuit
    open + recent failures + list position) but deterministically picks
    the single least-penalized endpoint instead of rotating among the
    healthy subset. Used by the desperation probe to choose which
    endpoint to force-probe after a pool-wide outage.

    Args:
        endpoints: Pool candidate URLs.

    Returns:
        The least-banned endpoint URL.
    """
    if not endpoints:
        raise ValueError("least_banned_endpoint requires a non-empty endpoint list")

    from lite_indexer.rpc.routing import is_circuit_open

    now = time.time()
    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        penalty = len(failures) * 1000 + i
        if is_endpoint_banned(ep):
            penalty += 50000
        if is_circuit_open(ep):
            penalty += 20000
        scored.append((penalty, ep))
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def force_probe_endpoint(endpoint: str) -> bool:
    """Bypass the circuit ban for a single request to ``endpoint``.

    Used by the desperation probe: after a pool-wide outage the
    least-banned endpoint is force-probed (once per
    :data:`CIRCUIT_DESPERATION_PROBE_INTERVAL`) even though its circuit
    is open. A successful probe closes the circuit (via
    :func:`record_endpoint_success`); a failure re-opens it.

    Args:
        endpoint: RPC endpoint URL to probe.

    Returns:
        True — the probe may proceed.
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    return CIRCUIT_BREAKER.force_probe(endpoint)


def desperation_probe_due(endpoints: list[str], now: float | None = None) -> bool:
    """Whether a pool-wide desperation probe may fire for ``endpoints``.

    Args:
        endpoints: Pool candidate URLs (pool key = chain name is not
            available here, so the first endpoint doubles as the pool
            key; callers that know the chain should prefer the breaker's
            own method with the chain name).
        now: Epoch time override (tests).

    Returns:
        True when the desperation interval has elapsed since the last
        probe for this pool.
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    if not endpoints:
        return False
    return CIRCUIT_BREAKER.desperation_probe_due(endpoints[0], now=now)


def mark_desperation_probe(endpoints: list[str], now: float | None = None) -> None:
    """Record that a desperation probe fired for ``endpoints``.

    Args:
        endpoints: Pool candidate URLs.
        now: Epoch time override (tests).
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    if not endpoints:
        return
    CIRCUIT_BREAKER.mark_desperation_probe(endpoints[0], now=now)


# ── Best Endpoint Selection ──────────────────────────────────────────


def get_best_endpoint(endpoints: list[str]) -> str:
    """Return the healthiest endpoint with load-balanced rotation.

    Scoring:
      - Hard ban (403/429): +50000 penalty (skip — provider rate-limited)
      - Circuit open: +20000 penalty (skip — breaker tripped)
      - Failed verification: +10000 penalty (worst)
      - Each recent failure adds 1000 penalty
      - Original list position adds 1..N penalty (prefers earlier tiers)
      - Bottom 30% of healthy list is ignored for rotation diversity
      - Top 2 healthy endpoints are chosen randomly for load distribution

    Fallthrough: if all endpoints are in cooldown, clear and retry the first.
    """
    now = time.time()

    from lite_indexer.rpc.routing import is_circuit_open

    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        _endpoint_failures[ep] = failures

        penalty = len(failures) * 1000 + i

        # Hard ban: a 403/429 endpoint is provider-blacklisted for a
        # while — skip it entirely, not just deprioritized.
        if is_endpoint_banned(ep):
            penalty += 50000

        # Circuit breaker: an open circuit fully skips the endpoint.
        if is_circuit_open(ep):
            penalty += 20000

        # Add verification penalty (unverified endpoints deprioritized)
        from lite_indexer.rpc.health import is_endpoint_healthy

        if not is_endpoint_healthy(ep):
            penalty += 10000

        scored.append((penalty, ep))

    scored.sort(key=lambda x: x[0])

    healthy = [ep for penalty, ep in scored if penalty < MAX_FAILURES * 1000]

    if not healthy:
        # Pool-wide outage. Do NOT clear failure state — that reset was
        # the stall: every cycle re-hammered endpoints[0] (which is still
        # banned/circuit-open) and deepened the bans. Return the
        # least-banned endpoint instead; the fetch path's desperation
        # probe gate decides whether it is dialed now or on the next
        # probe tick.
        logger.warning("all_endpoints_down", count=len(endpoints))
        return least_banned_endpoint(endpoints)

    pool_size = max(2, len(healthy) // 2)
    return random.choice(healthy[:pool_size])  # noqa: S311


# ── Failure Tracking ────────────────────────────────────────────────


def record_endpoint_failure(endpoint: str) -> None:
    """Record a failure for this endpoint (feeds backoff + circuit breaker).

    Use for genuine failures (timeouts, connection errors, bad responses).
    For 429 rate limits use :func:`record_rate_limit` instead — they must
    NOT trip the circuit breaker.
    """
    _endpoint_failures.setdefault(endpoint, []).append(time.time())
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_failure(endpoint)


def record_rate_limit(endpoint: str) -> None:
    """Record a 429 rate limit for backoff WITHOUT tripping the circuit.

    429 means the provider is temporarily throttling us — the correct
    reaction is backoff: the recent-failure timestamp deprioritizes the
    endpoint in :func:`get_best_endpoint` scoring and drives the
    exponential sleep in :func:`apply_endpoint_backoff` /
    :func:`handle_rate_limit`. It must NOT trip the per-endpoint circuit
    breaker (which fully skips the endpoint for a cooldown) or hard-ban it
    (``ENDPOINT_BAN_*`` exponential window) — rate limits are transient and
    the endpoint usually recovers within seconds; banning it wastes healthy
    capacity that free-tier providers expect us to retry after backoff.

    Args:
        endpoint: RPC endpoint URL that returned 429.
    """
    _endpoint_failures.setdefault(endpoint, []).append(time.time())
    metrics_incr("rpc_rate_limits")


def record_endpoint_success(endpoint: str) -> None:
    """Record a success for this endpoint (closes its circuit + unban)."""
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_success(endpoint)
    unban_endpoint(endpoint)


async def apply_endpoint_backoff(url: str) -> None:
    """Apply exponential backoff if this endpoint has recent failures."""
    recent_failures = [t for t in _endpoint_failures.get(url, []) if time.time() - t < ENDPOINT_COOLDOWN]
    if recent_failures:
        backoff = 2 ** len(recent_failures)
        await asyncio.sleep(min(backoff, BACKOFF_CAP))


async def handle_rate_limit(url: str) -> None:
    """Handle a 429: backoff only — no circuit trip, no hard ban.

    429 means the provider explicitly throttled this endpoint. Unlike
    genuine failures, rate limits are transient: the endpoint usually
    recovers in seconds, so we record the failure for the pool penalty and
    sleep with exponential backoff, but we do NOT trip the circuit breaker
    and do NOT hard-ban (both would skip the endpoint entirely and waste
    healthy capacity after the throttle lifts).

    Args:
        url: RPC endpoint URL that returned 429.
    """
    record_rate_limit(url)
    wait = min(2 ** len(_endpoint_failures.get(url, [])), RATE_LIMIT_WAIT_CAP)
    logger.debug("rpc_rate_limited", url=url[:50], backoff=wait)
    await asyncio.sleep(wait)
