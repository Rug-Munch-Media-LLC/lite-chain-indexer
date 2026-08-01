"""
Per-provider token bucket rate limiting.

Provides proactive rate limiting by maintaining per-provider token
buckets. Applied before RPC requests to stay within provider limits.
"""

from __future__ import annotations

import time

from lite_indexer.chains import PROVIDER_RATE_LIMITS, get_provider_rate_limit
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Token Bucket Rate Limiters (per provider domain pattern) ─────────
_token_buckets: dict[str, dict[str, float]] = {}  # pattern -> {tokens, last_refill}


def _get_token_bucket(endpoint: str) -> dict[str, float]:
    """Get or create a token bucket for this endpoint's provider."""
    from urllib.parse import urlparse

    domain = urlparse(endpoint).hostname or endpoint
    # Find matching provider pattern
    provider = domain
    for pattern in PROVIDER_RATE_LIMITS:
        if pattern in domain:
            provider = pattern
            break

    if provider not in _token_buckets:
        limits = get_provider_rate_limit(endpoint)
        _token_buckets[provider] = {
            "tokens": float(limits["burst"]),
            "last_refill": time.time(),
            "max_tokens": float(limits["burst"]),
            "refill_rate": float(limits["req_per_sec"]),
        }
    return _token_buckets[provider]


def _check_rate_limit(endpoint: str) -> float:
    """Check rate limit and return wait time in seconds.

    Implements a token bucket algorithm per provider.
    Returns 0 if request can proceed immediately.
    Returns >0 seconds to wait if rate limited.
    """
    bucket = _get_token_bucket(endpoint)
    now = time.time()

    # Refill tokens based on elapsed time
    elapsed = now - bucket["last_refill"]
    bucket["tokens"] = min(
        bucket["max_tokens"],
        bucket["tokens"] + elapsed * bucket["refill_rate"],
    )
    bucket["last_refill"] = now

    if bucket["tokens"] >= 1.0:
        bucket["tokens"] -= 1.0
        return 0.0

    # Need to wait for refill
    wait = (1.0 - bucket["tokens"]) / bucket["refill_rate"]
    return wait


async def apply_provider_rate_limit(endpoint: str) -> None:
    """Apply token-bucket rate limiting for this endpoint's provider.

    Waits if the provider's token bucket is empty. Uses per-provider
    limits from PROVIDER_RATE_LIMITS configuration.

    Args:
        endpoint: RPC URL to rate-limit.
    """
    import asyncio

    from lite_indexer.config import RATE_LIMIT_WAIT_CAP

    wait = _check_rate_limit(endpoint)
    if wait > 0:
        logger.debug(
            "provider_rate_limited",
            endpoint=endpoint[:50],
            wait_seconds=round(wait, 2),
        )
        await asyncio.sleep(min(wait, RATE_LIMIT_WAIT_CAP))
