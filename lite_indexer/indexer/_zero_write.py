"""Zero-write stall breaker (RC-3, 2026-09-19 audit).

A zero-write gate (fetched > 0, inserted == 0) aborts the sync cycle
without advancing state. When the write failure is persistent (ClickHouse
schema drift, poisoned row, disk full) the same range is replayed forever
— the chain is pinned and never makes progress.

This module tracks consecutive zero-write attempts per chain, keyed by
the range start. After ``MAX_CONSECUTIVE_ZERO_WRITES`` consecutive
failures for the SAME range the caller should:

1. log ``writer_stalled_range_deferred``,
2. increment the ``lite_indexer_writer_stalled`` metric,
3. enqueue the range into the persistent backfill queue (which re-attempts
   the data with its own retry budget), and
4. ADVANCE state past the range so the live loop is not pinned.

Counters reset on any successful write or on progress to a new range.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

MAX_CONSECUTIVE_ZERO_WRITES = 3


@dataclass
class _Attempt:
    """Consecutive zero-write attempts for one range start."""

    count: int = 0
    from_block: int = 0
    to_block: int = 0


@dataclass
class ZeroWriteTracker:
    """Per-chain consecutive zero-write attempt counters.

    Keyed by range start so a genuinely progressing chain (new ranges each
    cycle) never accumulates failures, while a pinned chain replaying the
    same range trips the breaker after ``MAX_CONSECUTIVE_ZERO_WRITES``.
    """

    chain: str
    max_attempts: int = MAX_CONSECUTIVE_ZERO_WRITES
    _attempts: dict[int, _Attempt] = field(default_factory=dict)

    def record_zero_write(self, from_block: int, to_block: int) -> bool:
        """Record a zero-write failure for the range.

        Returns True when the breaker trips (>= max consecutive attempts
        for the same range start) — the caller should defer the range to
        the backfill queue and advance state.
        """
        att = self._attempts.get(from_block)
        if att is None or att.to_block != to_block:
            att = _Attempt(count=0, from_block=from_block, to_block=to_block)
            self._attempts.clear()
            self._attempts[from_block] = att
        att.count += 1
        if att.count >= self.max_attempts:
            logger.warning(
                "writer_stalled_range_deferred",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
                consecutive_zero_writes=att.count,
            )
            self._attempts.pop(from_block, None)
            return True
        return False

    def record_success(self, from_block: int) -> None:
        """Reset the counter for a range start (write succeeded or range advanced)."""
        self._attempts.pop(from_block, None)


def tracker_for(host: Any) -> ZeroWriteTracker:
    """Lazily build the per-chain tracker on ``host`` (instance-level)."""
    trackers = getattr(host, "_zero_write_tracker_map", None)
    if trackers is None:
        trackers = {}
        host._zero_write_tracker_map = trackers
    if host.chain not in trackers:
        trackers[host.chain] = ZeroWriteTracker(host.chain)
    return trackers[host.chain]
