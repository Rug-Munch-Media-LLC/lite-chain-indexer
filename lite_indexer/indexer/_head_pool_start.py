"""Bounded WS head-pool start - fail OPEN, never hold the chain loop.

Split out of ``indexer/_sync_loop.py`` (2026-10-24). ``start_head_pool`` runs
BEFORE the chain loop, so it must never be able to hold a chain out of
``sync_loop``: endpoint resolution executes on a worker thread under
:data:`HEAD_POOL_START_TIMEOUT_S`, and a timeout or error fails OPEN (HTTP
polling continues, the WS pool is simply absent), logging the per-chain
duration that used to be invisible.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from lite_indexer.config import RPC_WS_HEAD_ENABLED
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Bound on WS head-pool endpoint resolution before the chain loop starts.
HEAD_POOL_START_TIMEOUT_S: float = 10.0


async def start_head_pool(owner: Any) -> None:
    """Start the EVM webSocket head subscriber (no-op when unconfigured).

    Runs BEFORE the chain loop, so it must never be able to hold a chain
    out of ``sync_loop``: endpoint resolution executes on a worker thread
    under :data:`HEAD_POOL_START_TIMEOUT_S`, and a timeout/error fails
    OPEN (HTTP polling continues, the WS pool is simply absent), logging
    the per-chain duration that used to be invisible (2026-10-24).
    """
    if owner._head_pool_task is not None or not owner._is_evm() or not RPC_WS_HEAD_ENABLED:
        return
    started = time.monotonic()
    try:
        endpoints = await asyncio.wait_for(
            asyncio.to_thread(owner._head_ws_endpoints),
            timeout=HEAD_POOL_START_TIMEOUT_S,
        )
    except TimeoutError:
        logger.error(
            "head_pool_start_timeout",
            chain=owner.chain,
            timeout_s=HEAD_POOL_START_TIMEOUT_S,
            duration_ms=round((time.monotonic() - started) * 1000, 1),
        )
        return
    except (OSError, ValueError) as exc:
        logger.warning("head_pool_start_failed", chain=owner.chain, error=str(exc)[:100])
        return
    if not endpoints:
        return
    from lite_indexer.rpc.head_pool import subscribe_heads

    owner._head_wakeup = asyncio.Event()
    owner._head_pool_task = asyncio.create_task(
        subscribe_heads(owner.chain, endpoints, owner._on_head),
        name=f"ws-head-{owner.chain}",
    )
    logger.info(
        "head_pool_started",
        chain=owner.chain,
        endpoints=len(endpoints),
        duration_ms=round((time.monotonic() - started) * 1000, 1),
    )
