"""
RPC endpoint pool — best-endpoint selection, failure tracking, backoff.

Provides load-balanced endpoint selection, exponential backoff on
failures, and reactive rate-limit handling after 429 responses.
"""

from __future__ import annotations

import asyncio
import random
import time

from lite_indexer.config import BACKOFF_CAP, ENDPOINT_COOLDOWN, MAX_FAILURES, RATE_LIMIT_WAIT_CAP
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ── State ──────────────────────────────────────────────────────────────
_endpoint_failures: dict[str, list[float]] = {}


# ── Best Endpoint Selection ──────────────────────────────────────────


def get_best_endpoint(endpoints: list[str]) -> str:
    """Return the healthiest endpoint with load-balanced rotation.

    Scoring:
      - Failed verification: +10000 penalty (worst)
      - Each recent failure adds 1000 penalty
      - Original list position adds 1..N penalty (prefers earlier tiers)
      - Bottom 30% of healthy list is ignored for rotation diversity
      - Top 2 healthy endpoints are chosen randomly for load distribution

    Fallthrough: if all endpoints are in cooldown, clear and retry the first.
    """
    now = time.time()

    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        _endpoint_failures[ep] = failures

        penalty = len(failures) * 1000 + i

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
    """Record a failure for this endpoint."""
    _endpoint_failures.setdefault(endpoint, []).append(time.time())


async def apply_endpoint_backoff(url: str) -> None:
    """Apply exponential backoff if this endpoint has recent failures."""
    recent_failures = [t for t in _endpoint_failures.get(url, []) if time.time() - t < ENDPOINT_COOLDOWN]
    if recent_failures:
        backoff = 2 ** len(recent_failures)
        await asyncio.sleep(min(backoff, BACKOFF_CAP))


async def handle_rate_limit(url: str) -> None:
    """Record a rate-limit failure and sleep with exponential backoff."""
    record_endpoint_failure(url)
    metrics_incr("rpc_rate_limits")
    wait = min(2 ** len(_endpoint_failures.get(url, [])), RATE_LIMIT_WAIT_CAP)
    logger.debug("rpc_rate_limited", url=url[:50], backoff=wait)
    await asyncio.sleep(wait)
