"""Per-slice sync checkpointing (forward-progress fix, 2026-09-20).

A sync cycle used to fetch its whole block range and only persist state at
the very end; a phase timeout mid-range discarded the fetched work and the
next cycle re-fetched the SAME range (base livelocked for 23 consecutive
cycles). The cycle range is now processed in ``SYNC_CHECKPOINT_SLICE``-block
sub-slices — each sub-slice runs through the normal completion path
(ClickHouse write + ``state.save`` + ``highwater.update``) before the next
slice starts, so a later failure keeps every earlier slice instead of
rewinding to the cycle start.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from lite_indexer import metrics
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Default sub-slice size for per-slice checkpointing. Overridable with the
# ``SYNC_CHECKPOINT_SLICE`` env var, read at call time so it can be retuned
# without a redeploy.
#: Blocks per slice = the BLOCK QUANTUM a turn can move. Sized from the
#: MEASURED per-block cost, not a guess: bsc/ethereum spend ~2.5 s of slot
#: time per block (span-limited getLogs at 10 blocks per call, CHUNK_PARALLELISM
#: = 2, plus hash/dex/insert overhead), so a 10-block slice is ~25 s and a
#: 100-block slice is ~250 s. 100 therefore blew through SYNC_SLICE_TIMEOUT_S
#: (90 s) on the first slice every turn and advanced NOTHING (measured 02:59Z).
#: 30 blocks x 2.5 s = ~75 s, inside the cap with margin, and moves 3x the
#: 10-block quantum that left bsc at +10 blocks per 655 s turn.
DEFAULT_SYNC_CHECKPOINT_SLICE = 30

# Statuses that always stop the cycle: the range itself is invalid/rewound
# or ClickHouse is unwritable — continuing to the next slice cannot help.
HARD_ABORT_STATUSES = frozenset(
    {"reorg_detected", "reorg_rollback", "reorg_status_unknown", "ch_unavailable", "no_rpc_available"}
)
# Statuses where earlier slice checkpoints are already durable: stop at the
# failed slice, keep the checkpoints, never rewind below the last good one.
SOFT_ABORT_STATUSES = frozenset({"fetch_failed", "ch_write_failed", "dex_write_failed", "writer_incomplete"})


def slice_size() -> int:
    """Resolve ``SYNC_CHECKPOINT_SLICE`` (default 10, floored at 1)."""
    raw = os.environ.get("SYNC_CHECKPOINT_SLICE")
    if raw is None:
        return DEFAULT_SYNC_CHECKPOINT_SLICE
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        logger.debug("checkpoint_slice_env_invalid", raw=raw)
        return DEFAULT_SYNC_CHECKPOINT_SLICE


def iter_slices(from_block: int, to_block: int, size: int) -> Iterator[tuple[int, int]]:
    """Yield inclusive ``(start, end)`` sub-slices covering the range."""
    step = max(1, size)
    start = from_block
    while start <= to_block:
        end = min(start + step - 1, to_block)
        yield start, end
        start = end + 1


def record_slice_checkpoint(host: Any, slice_from: int, slice_to: int, slices_done: int) -> None:
    """Log + count the per-cycle slice checkpoints (called once per cycle)."""
    logger.info(
        "sync_slice_checkpoint",
        chain=host.chain,
        slice_from=slice_from,
        slice_to=slice_to,
        slices_done=slices_done,
    )
    metrics.incr_labeled("lite_indexer_sync_slice_checkpoints", float(slices_done), chain=host.chain)


__all__ = [
    "DEFAULT_SYNC_CHECKPOINT_SLICE",
    "HARD_ABORT_STATUSES",
    "SOFT_ABORT_STATUSES",
    "iter_slices",
    "record_slice_checkpoint",
    "slice_size",
]
