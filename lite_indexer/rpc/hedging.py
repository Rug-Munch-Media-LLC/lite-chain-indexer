"""Hedged RPC dispatch — NodeCore-style tail-latency mitigation.

Ported from gatewayorg/nodecore's hedging resilience layer (MIT), adapted to
our in-process pool: when the primary endpoint hasn't answered within
``RPC_HEDGE_DELAY`` seconds, a second request fires at the next-best
endpoint in parallel. First successful answer wins; the slow attempt is
left to finish (its outcome still feeds health tracking — cancelling
mid-flight would waste the already-spent provider quota).

This is the single biggest p99 win for degraded-but-alive endpoints: the
ones that answer *eventually* stop stalling the indexer, because a healthy
peer covers their slack.

Config:
    RPC_HEDGE_DELAY    seconds to wait before firing the hedge (default 1.5)
    RPC_HEDGE_ENABLED  set to "0" to disable (default on)

Fail-open: any error degenerates to plain sequential behaviour.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Callable, Coroutine
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.rpc.pool import get_best_endpoint

logger = get_logger(__name__)

HEDGE_DELAY_S = float(os.getenv("RPC_HEDGE_DELAY", "1.5"))
HEDGE_ENABLED = os.getenv("RPC_HEDGE_ENABLED", "1") != "0"


async def hedged_rpc(
    factory: Callable[[str], Coroutine[Any, Any, dict[str, Any] | None]],
    endpoints: list[str],
) -> dict[str, Any] | None:
    """Race an RPC operation across the pool with a delayed hedge.

    Args:
        factory: async callable ``factory(endpoint) -> response-dict|None``.
            Called once per leg; endpoint health telemetry is expected to
            land inside the factory (as it does in RpcClient methods).
        endpoints: candidate endpoints for the chain (health-ranked inside
            via :func:`lite_indexer.rpc.pool.get_best_endpoint`).

    Returns:
        The first successful response dict, or None when every attempt
        fails (same failure shape as the underlying client calls).
    """
    if not endpoints:
        return None

    primary = get_best_endpoint(endpoints)

    if not HEDGE_ENABLED or HEDGE_DELAY_S <= 0 or len(endpoints) < 2:
        return await factory(primary)

    # Hedge candidate: best endpoint that is NOT the primary.
    hedge_ep = _hedge_pick(endpoints, primary)
    if hedge_ep is None:
        return await factory(primary)

    started = time.perf_counter()
    primary_task = asyncio.create_task(factory(primary))
    done, _pending = await asyncio.wait({primary_task}, timeout=HEDGE_DELAY_S)
    if done:
        # Primary answered inside the delay window — no hedge spent.
        return primary_task.result()

    hedge_task = asyncio.create_task(factory(hedge_ep))
    logger.debug(
        "rpc_hedge_fired",
        primary=primary[:50],
        hedge=hedge_ep[:50],
        after_ms=round((time.perf_counter() - started) * 1000),
    )

    # Both in flight: RETURN ON FIRST SUCCESS — that is the entire point.
    # The losing leg keeps running detached so its latency/outcome
    # telemetry still lands inside the factory (cancelling mid-flight
    # would waste the already-spent provider quota); its exception (if
    # any) is swallowed by the callback.
    for fut in asyncio.as_completed([primary_task, hedge_task]):
        try:
            result = await fut
        except Exception:  # noqa: BLE001 — a failed leg must not kill the race
            continue
        if result is not None:
            return result
    return None


async def hedged_call(
    client: Any,
    endpoints: list[str],
    method: str,
    params: list[Any] | None = None,
    *,
    timeout: float | None = None,
) -> dict[str, Any] | None:
    """Convenience wrapper: hedge a plain JSON-RPC ``client.call``."""

    async def _factory(endpoint: str) -> dict[str, Any] | None:
        kwargs: dict[str, Any] = {"timeout": timeout} if timeout is not None else {}
        response: dict[str, Any] | None = await client.call(endpoint, method, params, **kwargs)
        return response

    return await hedged_rpc(_factory, endpoints)


def _hedge_pick(endpoints: list[str], primary: str) -> str | None:
    """Second-best endpoint: first non-primary entry (pool order is
    health-ranked by get_best_endpoint's scoring pass)."""
    for ep in endpoints:
        if ep != primary:
            return ep
    return None
