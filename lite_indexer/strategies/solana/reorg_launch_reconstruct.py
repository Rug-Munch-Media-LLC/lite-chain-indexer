"""Why ``wallet_memory.solana_bundles`` is still ABSENT from the rollback.

Round 3 excluded BOTH ``solana_launches`` and ``solana_bundles`` because the
launchpad watcher's writer state (``_launchpad_last_slot`` and
``_launchpad_seen_mints``) survived a reorg: the re-fetch restarted past the
fork and never rescanned the orphaned range. Both now rewind
(``strategies/solana/strategy/launchpad_rewind.rewind_launchpad_state``, called
from ``strategy/sync.py`` step 4), so the cursor argument is GONE and
``solana_launches`` is rollback-eligible and listed in
``SOLANA_ROLLBACK_TABLES``.

``solana_bundles`` stays excluded on a different, independent ground: its
writer has no replay path at all. ``detect_bundle`` runs only over the
per-slot tx set that ``_fetch_mint_transfers`` happens to collect while
walking a TRACKED mint's signatures
(``strategy/transfers.py:376-389``), and the collection is partial by
construction: ``getSignaturesForAddress`` returns signatures for ONE address,
so a buy from a wallet that is not a tracked mint is never fetched, and a tip
transfer to a Jito tip account (bundles.py ``find_tip_transfer``) is a plain
SOL transfer that the mint walk does not enumerate. The bundle predicate
needs the create tx, the fresh buyers AND the tip transfer in the SAME slot in
execution order; the fetch path reconstructs an arbitrary subset of that
slot, so re-running the scan can legitimately produce no bundle. A cursor
rewind makes the range get SCANNED; it cannot make the evidence RECOVERABLE.

The decision lives in a module (not a test comment) because it has to stay
true as the writers move. ``solana_bundles`` is the canonical exclusion;
``describe_reorg_exclusion`` turns it into a machine-checkable reason and
``reorg_launch_writers`` records the state a future fix would have to change.
"""

from __future__ import annotations

from dataclasses import dataclass

SOLANA_BUNDLES_TABLE = "wallet_memory.solana_bundles"

#: Now rollback-eligible (round 4): the launchpad cursor and seen-mints are
#: rewound, so a re-fetch rebuilds the rows. Kept as a name so callers and
#: tests can refer to the table they are asserting about.
SOLANA_LAUNCHES_TABLE = "wallet_memory.solana_launches"

# The remaining reason: not "the cursor survived" but "there is nothing to
# replay from" — the evidence a bundle row is built from is not queryable.
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


# Owner of the launchpad writer state — kept as the module that would have to
# change for a bundle replay path to exist.
_LAUNCHPAD_WRITER_OWNER = "lite_indexer.strategies.solana.strategy.sync"


# CANONICAL EXCLUSION LIST. Read by the test suite, which asserts it against
# the live DDL and the real writer state.
SOLANA_REORG_LAUNCH_EXCLUSIONS: tuple[ReorgLaunchExclusion, ...] = (
    ReorgLaunchExclusion(
        table=SOLANA_BUNDLES_TABLE,
        height_column="slot",
        reason=NO_REPLAY_PATH,
        writer_owner=_LAUNCHPAD_WRITER_OWNER,
        evidence=(
            "lite_indexer/strategies/solana/strategy/transfers.py:376-389"
            " (detect_bundle runs over slot_txs gathered only from"
            " _fetch_mint_transfers' getSignaturesForAddress walk over a"
            " TRACKED mint, so the slot's buyer/tip transfers are never"
            " enumerated) with bundles.py:296-330 (the predicate needs the"
            " create tx, fresh buyers AND the last-tx tip transfer)"
        ),
    ),
)


@dataclass(frozen=True)
class ReorgLaunchWriter:
    """In-memory writer state a reorg must rewind to replay a range.

    Attributes:
        owner: Module-qualified name of the strategy that owns the state.
        attribute: The attribute that gates re-scanning a slot range.
        reset_on_reorg: Whether the reorg path rewinds it today.
    """

    owner: str
    attribute: str
    reset_on_reorg: bool


# The writer state, now rewound. Recorded as a contract, not as an exclusion
# justification: if a future change stops rewinding either attribute, a
# rollback-eligible table becomes unsafe again and the suite fails.
reorg_launch_writers: tuple[ReorgLaunchWriter, ...] = (
    ReorgLaunchWriter(
        owner="lite_indexer.strategies.solana.strategy.launchpad_rewind",
        attribute="_launchpad_last_slot",
        reset_on_reorg=True,
    ),
    ReorgLaunchWriter(
        owner="lite_indexer.strategies.solana.strategy.launchpad_rewind",
        attribute="_launchpad_seen_mints",
        reset_on_reorg=True,
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
    "NO_REPLAY_PATH",
    "ReorgLaunchExclusion",
    "ReorgLaunchWriter",
    "SOLANA_BUNDLES_TABLE",
    "SOLANA_LAUNCHES_TABLE",
    "SOLANA_REORG_LAUNCH_EXCLUSIONS",
    "describe_reorg_exclusion",
    "reorg_launch_writers",
]
