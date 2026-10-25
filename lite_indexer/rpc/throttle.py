"""Intelligent RPC throttling — header tracking, adaptive backoff,
pre-emptive throttling, per-method budgets, and disk-persisted state.

Facade over the cohesive throttle modules; every public name that existed
before the split is still importable from here:

* :mod:`lite_indexer.rpc.throttle_headers` — rate-limit header parsing
* :mod:`lite_indexer.rpc.throttle_base` — endpoint state + shared base
* :mod:`lite_indexer.rpc.throttle_state` — JSON persistence
* :mod:`lite_indexer.rpc.throttle_wait` — wait computation / pacing
* :mod:`lite_indexer.rpc.throttle_record` — response recording

Three layers make this distinct from the per-provider token bucket
(``rpc/ratelimit.py``) and the circuit breaker (``rpc/routing.py``):

1. **Rate-limit header tracking** — ``X-RateLimit-Remaining`` /
   ``X-RateLimit-Limit`` / ``Retry-After`` are captured per endpoint and
   honored exactly: a 429 that advertises ``Retry-After: 60`` sleeps 60s,
   not a guess. Header-less endpoints fall back to the token bucket.

2. **Pre-emptive throttling** — when an endpoint advertises a budget and
   we have consumed more than :data:`RPC_PREEMPTIVE_THRESHOLD_PCT`
   (default 80%), the next request is delayed so the budget is never
   exhausted — we slow down *before* the 429 arrives.

3. **Per-method budgets** — ``eth_getLogs`` is expensive on free tiers
   (3-10 req/s) while ``eth_blockNumber`` is nearly free. Each method has
   its own request/s budget (:data:`RPC_METHOD_REQ_PER_SEC`) so cheap
   calls never starve expensive ones (and vice versa).

All state is persisted to :data:`RPC_THROTTLE_FILE` (a JSON file) so a
restart mid-429-window does not reset the tracker — the daemon wakes up
still knowing which endpoints told it to back off and for how long.
"""

from __future__ import annotations

from collections.abc import Mapping

from lite_indexer.logging import get_logger
from lite_indexer.rpc.throttle_base import EndpointState, ThrottleBase
from lite_indexer.rpc.throttle_headers import extract_rate_limit
from lite_indexer.rpc.throttle_record import ThrottleRecordMixin
from lite_indexer.rpc.throttle_state import ThrottlePersistenceMixin
from lite_indexer.rpc.throttle_wait import ThrottleWaitMixin

logger = get_logger(__name__)


class RPCRateTracker(ThrottlePersistenceMixin, ThrottleWaitMixin, ThrottleRecordMixin):
    """Persisted per-endpoint rate-limit tracker.

    Single-writer, JSON-on-disk: every mutation bumps ``updated_at`` and
    the tracker is flushed to :data:`RPC_THROTTLE_FILE` when
    :meth:`persist` is called (the RPC layer calls it periodically and
    on shutdown). Load is best-effort — a corrupt file degrades to an
    empty tracker rather than crashing the indexer.
    """


# Module-level tracker shared across the codebase (persists to disk).
RATE_TRACKER = RPCRateTracker()


async def throttle_before(endpoint: str, method: str, *, max_wait: float | None = None) -> None:
    """Block until a request to ``endpoint`` for ``method`` may proceed.

    Waits for the exact Retry-After/backoff window when one is armed, or
    paces pre-emptively from the advertised budget / per-method rate.
    Uses the module-level :data:`RATE_TRACKER` so all RPC paths share one
    view of each endpoint.

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
        max_wait: Optional hard cap (seconds) on this sleep. A caller with an
            overall phase deadline passes its per-request timeout so a
            throttle wait can never outlive the operation it paces; the
            remaining wait is simply skipped (the request proceeds and the
            provider decides). Retry-After windows above the cap are clamped,
            not honored exactly — use only where a bounded phase matters more
            than absolute politeness.
    """
    import asyncio

    wait = RATE_TRACKER.wait_seconds_for(endpoint, method)
    if max_wait is not None and wait > max_wait:
        wait = max(0.0, max_wait)
    if wait > 0:
        logger.debug(
            "rpc_throttle_wait",
            endpoint=endpoint[:60],
            method=method,
            wait_s=round(wait, 2),
        )
        await asyncio.sleep(wait)
    RATE_TRACKER.mark_sent(endpoint, method)


def record_rpc_response(
    endpoint: str,
    method: str,
    status: int,
    headers: Mapping[str, str] | None = None,
) -> None:
    """Record a response in the shared tracker (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
        status: HTTP status code.
        headers: Response headers (rate-limit headers are parsed).
    """
    RATE_TRACKER.record_response(endpoint, method, status, headers)


__all__ = [
    "EndpointState",
    "RATE_TRACKER",
    "RPCRateTracker",
    "ThrottleBase",
    "extract_rate_limit",
    "record_rpc_response",
    "throttle_before",
]
