"""Why the Solana launch/bundle tables are deliberately ABSENT from the rollback.

``wallet_memory.solana_launches`` and ``wallet_memory.solana_bundles`` are
written on the live sync path at a slot height, so a reorg CAN orphan them.
They are still not in ``SOLANA_ROLLBACK_TABLES``, because a rollback is only
safe when a re-fetch of the orphaned range rebuilds the same rows. For these
two tables it demonstrably does NOT — each has a reorg-fatal writer state that
lives in memory and is never rewound. Rolling them back would delete rows that
nothing ever rebuilds, which is silent data loss, the exact failure the
rollback list exists to prevent.

The decision lives in a module (not a test comment) because it has to stay
true as the writers move. ``solana_reorg_launch_exclusions`` is the canonical
exclusion list; ``reorg_launch_writers`` are the writers that prove the
exclusions, and ``describe_reorg_exclusion`` turns one table into a
machine-checkable reason.
"""

from __future__ import annotations

from dataclasses import dataclass

SOLANA_LAUNCHES_TABLE = "wallet_memory.solana_launches"
SOLANA_BUNDLES_TABLE = "wallet_memory.solana_bundles"

# The two distinct reasons. Same outcome (never roll back), different cause:
# a re-fetch is driven by a cursor/seen-set that survived the reorg, so the
# orphaned range is skipped instead of replayed.
CURSOR_NEVER_REWOUND = "writer-cursor-not-rewound"
NO_REPLAY_PATH = "no-replay-path"


@dataclass(frozen=True)
class ReorgLaunchExclusion:
    """A slot-scoped Solana table held out of the reorg rollback.

    Attributes:
        table: Fully-qualified ClickHouse table name.
        height_column: Height column as named in the live DDL.
        reason: Machine-readable cause, one of the module constants.
        writer_owner: Module that owns the in-memory state gating the replay.
        evidence: Module-qualified ``file:line`` proving the claim.
    """

    table: str
    height_column: str
    reason: str
    writer_owner: str
    evidence: str


# Owner of the launchpad writer state both Solana exclusions turn on.
_LAUNCHPAD_WRITER_OWNER = "lite_indexer.strategies.solana.strategy.sync"


# CANONICAL EXCLUSION LIST. Read by the sync path's reorg check and asserted
# against the live DDL + the real writer state by the test suite.
SOLANA_REORG_LAUNCH_EXCLUSIONS: tuple[ReorgLaunchExclusion, ...] = (
    ReorgLaunchExclusion(
        table=SOLANA_LAUNCHES_TABLE,
        height_column="slot",
        reason=CURSOR_NEVER_REWOUND,
        writer_owner=_LAUNCHPAD_WRITER_OWNER,
        evidence=(
            "lite_indexer/strategies/solana/strategy/sync.py:179"
            " (poll_launchpad_blocks: start = max(start, _launchpad_last_slot + 1))"
            " with sync.py:216-217 (_launchpad_seen_mints.add / _last_launch_events.append)"
        ),
    ),
    ReorgLaunchExclusion(
        table=SOLANA_BUNDLES_TABLE,
        height_column="slot",
        reason=NO_REPLAY_PATH,
        writer_owner=_LAUNCHPAD_WRITER_OWNER,
        evidence=(
            "lite_indexer/strategies/solana/strategy/transfers.py:383-388"
            " (detect_bundle runs only over slot_txs gathered from _fetch_mint_transfers)"
            " with transfers.py:70-85 (mint list comes from _discover_new_tokens + tracked_tokens)"
        ),
    ),
)


@dataclass(frozen=True)
class ReorgLaunchWriter:
    """In-memory writer state that a reorg must rewind to replay a range.

    Attributes:
        owner: Module-qualified name of the strategy that owns the state.
        attribute: The attribute that gates re-scanning a slot range.
        reset_on_reorg: Whether the reorg path rewinds it today.
    """

    owner: str
    attribute: str
    reset_on_reorg: bool


# The writer state that makes a post-rollback re-fetch non-replaying. If any
# entry stops being true, its table's exclusion is no longer justified.
reorg_launch_writers: tuple[ReorgLaunchWriter, ...] = (
    ReorgLaunchWriter(
        owner="lite_indexer.strategies.solana.strategy.sync",
        attribute="_launchpad_last_slot",
        reset_on_reorg=False,
    ),
    ReorgLaunchWriter(
        owner="lite_indexer.strategies.solana.strategy.sync",
        attribute="_launchpad_seen_mints",
        reset_on_reorg=False,
    ),
)


def describe_reorg_exclusion(table: str) -> ReorgLaunchExclusion:
    """Return the recorded exclusion for ``table``.

    Args:
        table: Fully-qualified ClickHouse table name.

    Returns:
        The matching :class:`ReorgLaunchExclusion`.

    Raises:
        KeyError: When ``table`` is not a known exclusion — an unrecorded
            table is a gap in the decision, not a silent pass.
    """
    for exclusion in SOLANA_REORG_LAUNCH_EXCLUSIONS:
        if exclusion.table == table:
            return exclusion
    raise KeyError(table)


__all__ = [
    "CURSOR_NEVER_REWOUND",
    "NO_REPLAY_PATH",
    "ReorgLaunchExclusion",
    "ReorgLaunchWriter",
    "SOLANA_BUNDLES_TABLE",
    "SOLANA_LAUNCHES_TABLE",
    "SOLANA_REORG_LAUNCH_EXCLUSIONS",
    "describe_reorg_exclusion",
    "reorg_launch_writers",
]
