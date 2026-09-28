"""Rewind the launchpad watcher's in-memory state after a Solana reorg.

The launchpad watcher (``SyncMixin.poll_launchpad_blocks``) advances two
pieces of strategy-level mutable state as it scans:

* ``_launchpad_last_slot`` — the newest slot already scanned, used as
  ``start = max(start, _launchpad_last_slot + 1)``. Never rewound, so after
  a rollback the re-fetch restarted PAST the fork and never rescanned the
  orphaned range.
* ``_launchpad_seen_mints`` — a bounded insertion-ordered set of mints already
  buffered, checked at ``sync.py`` before a launch is accepted. Never
  rewound, so even a rewind of the cursor alone would re-scan the range and
  then SKIP every orphaned mint, because the set still held them.

Both must move together. ``_launchpad_last_slot`` is lowered to the fork
(common ancestor) so the next ``poll_launchpad_blocks`` computes a ``start``
at or below the fork; ``_launchpad_seen_mints`` is cleared so the re-scan
re-buffers the orphaned mints instead of deduplicating them away.

Mints that were discovered and promoted to ``_mint_set`` are NOT rolled back:
that set is the tracked-token set the whole transfer walk depends on, and
dropping it would also disable the dedup guard in the watcher. An orphaned
mint therefore stays tracked (its transfers are re-walked) but is re-detected
and re-written on the re-scan.
"""

from __future__ import annotations

from typing import Protocol

from lite_indexer.util.bounded import BoundedSet


class LaunchpadRewindable(Protocol):
    """The launchpad writer state a reorg has to rewind.

    ``_launchpad_seen_mints`` is typed as a concrete :class:`BoundedSet`
    rather than a generic container: the rewind must be able to enumerate
    and discard, and a narrower protocol would not type-check against the
    real strategy attribute.
    """

    _launchpad_last_slot: int
    _launchpad_seen_mints: BoundedSet[str]


def rewind_launchpad_state(strategy: LaunchpadRewindable, fork_slot: int) -> int:
    """Rewind ``strategy``'s launchpad state so the orphaned range is rescanned.

    Args:
        strategy: The Solana strategy (duck-typed — no import of the
            concrete class, so deps flow down and no cycle is formed).
        fork_slot: Common-ancestor slot. Rows above it were deleted by the
            rollback and must be re-fetched, so the cursor is lowered to it
            and the seen-mints set is emptied.

    Returns:
        Number of seen mints released, for the caller's log line.
    """
    strategy._launchpad_last_slot = fork_slot
    seen = strategy._launchpad_seen_mints
    released = len(seen)
    for mint in list(seen):
        seen.discard(mint)
    return released
