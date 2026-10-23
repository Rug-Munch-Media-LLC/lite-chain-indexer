"""Cycle-permit lane: acquire a fairness slot and run ONE capped sync cycle.

Split out of ``cli/_daemon.py`` on 2026-10-24. The daemon's per-chain loop
had the exempt-chain rule, slot acquisition, the hard wall-clock cap and
the release-in-finally all inline, which pushed ``main_daemon`` and the
chain loop past their ceilings and the file past 450 lines.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Hard wall-clock cap on one sync cycle (default 600 s). Sits well above a
#: healthy cycle (measured median 72-130 s) and below the 900 s staleness
#: bound, so a pathological cycle costs one turn instead of the fleet.
SYNC_CYCLE_TIMEOUT_S: float = float(os.environ.get("SYNC_CYCLE_TIMEOUT_S", "600"))

#: 2026-09-21: chains exempt from the shared MAX_CONCURRENT_CHAINS semaphore
#: (dedicated lane). Solana measured ~14 slots/min effective because it
#: queued FIFO behind multi-minute EVM cycles; the chain produces ~150
#: slots/min. Memory gating is unchanged for these chains.
SEMAPHORE_EXEMPT_CHAINS: frozenset[str] = frozenset({"solana"})


async def run_cycle(rotation: Any, chain: str, indexer: Any) -> Any:
    """Run one sync cycle under the concurrency rotation, with a hard cap.

    Chains in :data:`SEMAPHORE_EXEMPT_CHAINS` (or a ``None`` rotation) run
    uncapped and unordered. Every other chain takes a least-served-first
    slot, released in ``finally`` so cancellation or shutdown mid-cycle can
    never leak capacity. The cap matters because a cycle that never returns
    holds its slot forever: 3 leaked slots starve every other chain no
    matter how fair the queue is. State is checkpointed per slice, so
    cancelling loses at most the slice in flight — the path SIGTERM uses.
    """
    if rotation is None or chain in SEMAPHORE_EXEMPT_CHAINS:
        return await indexer.sync_loop()

    hold_start = time.monotonic()
    await rotation.acquire(chain)
    try:
        async with asyncio.timeout(SYNC_CYCLE_TIMEOUT_S):
            return await indexer.sync_loop()
    except TimeoutError:
        logger.error(
            "sync_cycle_timeout",
            chain=chain,
            timeout_s=SYNC_CYCLE_TIMEOUT_S,
            hold_s=round(time.monotonic() - hold_start, 1),
        )
        return {"status": "cycle_timeout", "chain": chain}
    finally:
        logger.info(
            "sync_cycle_end",
            chain=chain,
            hold_s=round(time.monotonic() - hold_start, 1),
        )
        rotation.release(chain)
