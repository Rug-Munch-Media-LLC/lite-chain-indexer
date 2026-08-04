"""
RPC endpoint pool — best-endpoint selection, failure tracking, backoff.

Provides load-balanced endpoint selection, exponential backoff on
failures, reactive rate-limit handling after 429 responses, and a
per-endpoint circuit breaker (open after a failure burst) layered on
top of the classic scoring.
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
        for ep in endpoints:
            _endpoint_failures[ep] = []
        logger.warning("all_endpoints_cooldown_reset", count=len(endpoints))
        return endpoints[0]

    pool_size = max(2, len(healthy) // 2)
    return random.choice(healthy[:pool_size])  # noqa: S311


# ── Failure Tracking ────────────────────────────────────────────────


def record_endpoint_failure(endpoint: str) -> None:
    """Record a failure for this endpoint (feeds backoff + circuit breaker)."""
    _endpoint_failures.setdefault(endpoint, []).append(time.time())
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_failure(endpoint)


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
    """Record a rate-limit failure and sleep with exponential backoff.

    429 means the provider explicitly throttled this endpoint — record
    the failure for the pool penalty AND hard-ban the endpoint for an
    exponentially growing window (see :func:`ban_endpoint`) so it is
    skipped entirely instead of being retried every cycle.

    Args:
        url: RPC endpoint URL that returned 429.
    """
    record_endpoint_failure(url)
    ban_endpoint(url, reason="http_429")
    metrics_incr("rpc_rate_limits")
    wait = min(2 ** len(_endpoint_failures.get(url, [])), RATE_LIMIT_WAIT_CAP)
    logger.debug("rpc_rate_limited", url=url[:50], backoff=wait)
    await asyncio.sleep(wait)
