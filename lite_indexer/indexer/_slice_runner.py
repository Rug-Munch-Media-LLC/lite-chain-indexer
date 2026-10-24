"""Per-slice execution loop for one sync cycle.

Split out of ``indexer/_sync_loop.py`` on 2026-10-24: the cycle body had
grown a slice loop, a wall-clock budget and the abort-status handling all
inline, pushing the file to 796 lines and the enclosing function past its
ceiling. One primary function lives here — :func:`run_slices` — so the
mixin keeps only range resolution and delegation.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from typing import Any

from lite_indexer.indexer._checkpoint import (
    HARD_ABORT_STATUSES,
    SOFT_ABORT_STATUSES,
    iter_slices,
    record_slice_checkpoint,
    slice_size,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Wall-clock budget for ONE cycle's slices before it yields its slot.
#: Sized so a full 16-chain round at MAX_CONCURRENT_CHAINS=3 stays under
#: the 900 s staleness bound (16 / 3 x 150 s = 800 s), while a healthy
#: cycle (median 72-130 s) still finishes in one turn.
SYNC_SLICE_BUDGET_S = float(os.environ.get("SYNC_SLICE_BUDGET_S", "60"))

#: Hard ceiling on ONE slice. The budget above is only checked BETWEEN slices,
#: so an unbounded slice set the real hold: bsc spent 300 s inside a single
#: 10-block slice on a trace-phase timeout and held its slot for 656 s. A turn
#: is now <= SYNC_SLICE_BUDGET_S + this, which is what makes the slot count
#: arithmetic (6 slots x 150 s < 900 s staleness) true instead of aspirational.
SYNC_SLICE_TIMEOUT_S = float(os.environ.get("SYNC_SLICE_TIMEOUT_S", "90"))

SliceRunner = Callable[[int, int, dict[str, Any], float], Awaitable[dict[str, Any]]]


async def run_slices(
    indexer: Any,
    sync_slice: SliceRunner,
    block_range: tuple[int, int],
    summary: dict[str, Any],
    start_time: float,
) -> dict[str, Any]:
    """Process the cycle's block range as checkpointed sub-slices.

    Each sub-slice persists state + high-water on completion, so a later
    phase timeout keeps every earlier slice instead of discarding the whole
    cycle's fetch. The time budget makes a cycle YIELD its concurrency slot
    instead of swallowing the whole backlog: a chain 3 h behind needs ~40
    slices, and unbounded it held one of three slots for minutes while the
    other 15 chains queued behind it (2026-10-31: 11 queued, 0 granted).
    Every slice checkpoints state, so stopping here is free.
    """
    from_block, to_block = block_range
    slices_done = 0
    last_from, last_to = from_block, from_block - 1
    for slice_from, slice_to in iter_slices(from_block, to_block, slice_size()):
        if slices_done and time.monotonic() - start_time > SYNC_SLICE_BUDGET_S:
            record_slice_checkpoint(indexer, last_from, last_to, slices_done)
            summary["status"] = "cycle_budget"
            summary["slices_done"] = slices_done
            summary["duration_seconds"] = time.monotonic() - start_time
            logger.info(
                "sync_cycle_budget_yield",
                chain=indexer.chain,
                slices=slices_done,
                budget_s=SYNC_SLICE_BUDGET_S,
                remaining_blocks=to_block - last_to,
            )
            return summary
        try:
            async with asyncio.timeout(SYNC_SLICE_TIMEOUT_S):
                result = await sync_slice(slice_from, slice_to, summary, start_time)
        except TimeoutError:
            # The slice is abandoned, not advanced: fail-closed on the first
            # slice, keep earlier checkpoints on a later one.
            summary["status"] = "slice_timeout"
            summary["slices_done"] = slices_done
            summary["duration_seconds"] = time.monotonic() - start_time
            logger.warning(
                "sync_slice_timeout",
                chain=indexer.chain,
                slice_from=slice_from,
                slice_to=slice_to,
                timeout_s=SYNC_SLICE_TIMEOUT_S,
                slices_done=slices_done,
            )
            if slices_done:
                record_slice_checkpoint(indexer, last_from, last_to, slices_done)
            return summary
        status = result.get("status")
        if status in HARD_ABORT_STATUSES:
            return result
        if status in SOFT_ABORT_STATUSES:
            # First slice failed → no advance (fail-closed). A later one
            # failed → earlier checkpoints are durable; stop here.
            if slices_done == 0:
                return result
            record_slice_checkpoint(indexer, last_from, last_to, slices_done)
            result["status"] = "slice_partial"
            return result
        slices_done += 1
        last_from, last_to = slice_from, slice_to
    record_slice_checkpoint(indexer, last_from, last_to, slices_done)
    return summary
