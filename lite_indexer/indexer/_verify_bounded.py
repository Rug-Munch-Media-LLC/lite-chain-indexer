"""Bounded endpoint-verification resolution for one sync cycle.

A chain holds a concurrency slot from the moment it enters its cycle, and the
daemon's startup pass verifies all chains at once. Waiting on that pass
unbounded leaked all three slots on 2026-10-31: chains sat inside their FIRST
cycle before logging ``sync_start``, so 12 of 16 chains never started and the
fair rotation had nothing to grant (measured: 12 queued, 0 granted in 4.5 min).

Both waits are therefore bounded here. A timeout is not an error: the cycle
proceeds with the configured RPC pool instead of parking a slot on a probe.
"""

from __future__ import annotations

import asyncio
import os

from lite_indexer.logging import get_logger
from lite_indexer.rpc.verify import verify_chain_endpoints, wait_startup_result

logger = get_logger(__name__)

#: How long a chain waits for the parallel startup verification before it
#: stops holding a slot for it and uses the configured pool.
STARTUP_VERIFY_WAIT_S: float = float(os.environ.get("STARTUP_VERIFY_WAIT_S", "60"))

#: Hard bound on the in-loop verification fallback.
IN_LOOP_VERIFY_TIMEOUT_S: float = float(os.environ.get("IN_LOOP_VERIFY_TIMEOUT_S", "60"))


async def resolve_verified(indexer):
    """Verified endpoint set for ``indexer.chain``, or ``None`` to proceed.

    Args:
        indexer: The chain indexer (``chain`` and ``rpc`` attributes).

    Returns:
        The :class:`EndpointVerifyResult` when verification landed inside the
        bounds, else ``None`` — the caller syncs with the configured pool.
    """
    result = await wait_startup_result(indexer.chain, timeout=STARTUP_VERIFY_WAIT_S)
    if result is not None:
        return result
    try:
        async with asyncio.timeout(IN_LOOP_VERIFY_TIMEOUT_S):
            return await verify_chain_endpoints(indexer.chain, indexer.rpc)
    except TimeoutError:
        logger.warning(
            "endpoint_verify_timeout_proceed",
            chain=indexer.chain,
            timeout_s=IN_LOOP_VERIFY_TIMEOUT_S,
        )
        return None


__all__ = ["IN_LOOP_VERIFY_TIMEOUT_S", "STARTUP_VERIFY_WAIT_S", "resolve_verified"]
