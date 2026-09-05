"""Redis-backed circuit breaker for RPC endpoints.

Trip logic:
    - 5 consecutive transient failures on an endpoint → circuit OPENS.
    - While open, calls to that endpoint are skipped for ``CB_OPEN_TTL_S``
      (60s default) — the Redis TTL is the open-state timer.
    - When the TTL expires the circuit is HALF-OPEN: exactly one probe
      call is allowed through. Success resets the circuit (counters
      cleared); failure re-trips it immediately (the failure counter is
      pre-seeded at ``threshold - 1`` when the circuit opens, so a single
      post-expiry failure reaches the threshold again).

State lives in Redis (``REDIS_URL``) with TTLs so multiple indexer
processes share breaker state and nothing leaks on restart. Fail-open:
if Redis is unavailable or ``RPC_CIRCUIT_ENABLED=0``, the breaker is a
no-op and calls proceed normally.
"""

from __future__ import annotations

import hashlib
import os
from typing import Any

from lite_indexer.config import REDIS_URL
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

CB_FAILURE_THRESHOLD = int(os.getenv("RPC_CIRCUIT_THRESHOLD", "5"))
CB_OPEN_TTL_S = int(os.getenv("RPC_CIRCUIT_OPEN_TTL", "60"))
CB_ENABLED = os.getenv("RPC_CIRCUIT_ENABLED", "1") != "0"

_redis: Any = None


async def _get_redis() -> Any:
    """Shared Redis client, or None when disabled/unavailable (fail-open)."""
    global _redis
    if not CB_ENABLED or not REDIS_URL:
        return None
    if _redis is None:
        try:
            import redis.asyncio as aioredis

            _redis = aioredis.from_url(REDIS_URL, socket_connect_timeout=1)
        except Exception as exc:  # noqa: BLE001 — breaker must never break RPC
            logger.debug("circuit_redis_init_failed", error=str(exc)[:60])
            return None
    return _redis


def _open_key(endpoint: str) -> str:
    return "rpc:cb:open:" + hashlib.sha256(endpoint.encode()).hexdigest()[:16]


def _fails_key(endpoint: str) -> str:
    return "rpc:cb:fails:" + hashlib.sha256(endpoint.encode()).hexdigest()[:16]


async def circuit_open(endpoint: str) -> bool:
    """True when the endpoint's circuit is open (skip the call)."""
    r = await _get_redis()
    if r is None:
        return False
    try:
        return bool(await r.exists(_open_key(endpoint)))
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.debug("circuit_check_failed", error=str(exc)[:60])
        return False


async def record_circuit_failure(endpoint: str) -> None:
    """Count a transient failure; trip the circuit at the threshold."""
    r = await _get_redis()
    if r is None:
        return
    try:
        fails = int(await r.incr(_fails_key(endpoint)))
        await r.expire(_fails_key(endpoint), CB_OPEN_TTL_S * 2)
        if fails >= CB_FAILURE_THRESHOLD:
            await r.setex(_open_key(endpoint), CB_OPEN_TTL_S, "open")
            # Pre-seed the counter one below threshold with a TTL that
            # outlives the open window: after the TTL expires (half-open),
            # a single failed probe re-trips immediately.
            await r.setex(_fails_key(endpoint), CB_OPEN_TTL_S + 5, CB_FAILURE_THRESHOLD - 1)
            logger.warning(
                "rpc_circuit_open",
                endpoint=endpoint[:50],
                consecutive_failures=fails,
                open_ttl_s=CB_OPEN_TTL_S,
            )
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.debug("circuit_record_failed", error=str(exc)[:60])


async def record_circuit_success(endpoint: str) -> None:
    """Reset the circuit: clear counters (half-open probe succeeded)."""
    r = await _get_redis()
    if r is None:
        return
    try:
        await r.delete(_fails_key(endpoint))
    except Exception as exc:  # noqa: BLE001 — fail-open
        logger.debug("circuit_reset_failed", error=str(exc)[:60])
