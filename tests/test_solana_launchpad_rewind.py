"""A Solana reorg must RE-SCAN the orphaned launchpad range, not resume past it.

Round 3 found the launchpad watcher's writer state survived a reorg:
``poll_launchpad_blocks`` starts at ``_launchpad_last_slot + 1`` and skips any
mint already in ``_launchpad_seen_mints``, so a post-rollback re-fetch never
revisited the deleted range. This suite pins the fix at two levels:

1. BEHAVIOUR. A real ``SolanaStrategy`` that has already scanned past the fork
   is rolled back through ``reorg_stage``; the next ``poll_launchpad_blocks``
   must re-request the orphaned slots and re-buffer the orphaned mint. Two
   mutations are shown to break it — removing the rewind entirely, and keeping
   the rewind but only lowering the cursor (leaving the seen-mints set
   populated, so the re-scan finds the slot and then skips the mint).
2. THE SCOPE OF THE FIX. ``solana_launches`` is now rollback-eligible and
   listed; ``solana_bundles`` is still excluded, because rewinding a cursor
   makes a range get scanned but cannot make Jito-tip evidence recoverable
   from a partial signature walk.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.config.rollback_tables import SOLANA_ROLLBACK_TABLES
from lite_indexer.strategies.reorg_status import ReorgStatus
from lite_indexer.strategies.solana import SolanaStrategy
from lite_indexer.strategies.solana import reorg as sol_reorg
from lite_indexer.strategies.solana.launch_constants import PUMP_FUN_CREATE_DISCRIMINATOR
from lite_indexer.strategies.solana.launch_tx import _b58encode
from lite_indexer.strategies.solana.reorg_launch_reconstruct import (
    SOLANA_BUNDLES_TABLE,
    describe_reorg_exclusion,
)
from lite_indexer.strategies.solana.strategy import launchpad_rewind

REPO_ROOT = Path(__file__).resolve().parents[1]
STAGE_MODULE = "lite_indexer/strategies/solana/strategy/sync.py"

SOLANA_LAUNCHES_TABLE = "wallet_memory.solana_launches"

FORK_SLOT = 68
MINT = "LaunchpadRewindMint11111111111111111111111111111111"
MINT2 = "LaunchpadRewindMint22222222222222222222222222222222"

# A pump.fun-shaped create transaction. ``detect_launch`` matches pump.fun on
# the exact CREATE DISCRIMINATOR (launch_models.PUMP_FUN_CREATE_DISCRIMINATOR,
# 8 base58-decoded bytes) and takes the mint from meta.postTokenBalances[0].
_PUMP_FUN = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
# Derived from the constant itself so the fixture cannot drift from the model.
_PUMP_FUN_CREATE = _b58encode(PUMP_FUN_CREATE_DISCRIMINATOR)


def _create_tx(mint: str, slot: int) -> dict[str, Any]:
    """A jsonParsed getBlock transaction that ``detect_launch`` accepts.

    Args:
        mint: Mint address the create instruction mints.
        slot: Slot the (fake) block belongs to.

    Returns:
        A getBlock-shaped transaction dict.
    """
    return {
        "transaction": {
            "message": {
                "accountKeys": [{"pubkey": "deployer", "signer": True}],
                "instructions": [
                    {
                        "programId": _PUMP_FUN,
                        "data": _PUMP_FUN_CREATE,
                        "parsed": {
                            "type": "initializeMint2",
                            "info": {"decimals": 6, "mint": mint, "rent": "14616000"},
                        },
                    }
                ],
            },
            "signatures": [f"sig-{mint}-{slot}"],
        },
        "meta": {
            "err": None,
            "logMessages": [],
            "postTokenBalances": [
                {"accountIndex": 1, "mint": mint, "owner": "deployer", "uiTokenAmount": {"amount": "1"}}
            ],
        },
        "slot": slot,
    }


class FakeCH:
    """ClickHouse double that answers the checkpoint table and DELETEs."""

    def __init__(self, stored_blockhash: str = "stored") -> None:
        self.checkpoints: dict[int, str] = {}
        self.deletes: list[str] = []
        self.stored_blockhash = stored_blockhash

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        if query.startswith("CREATE TABLE IF NOT EXISTS wallet_memory.solana_checkpoints"):
            return 1
        if query.startswith("INSERT INTO wallet_memory.solana_checkpoints"):
            for row in params or []:
                self.checkpoints[int(row[0])] = str(row[1])
            return len(params or [])
        if "SELECT blockhash FROM wallet_memory.solana_checkpoints" in query:
            stored = self.checkpoints.get(int(params["slot"])) or self.stored_blockhash
            return [(stored,)]
        if query.startswith("DELETE FROM"):
            self.deletes.append(query)
            return 2
        return []


class RecordingStrategy(SolanaStrategy):
    """A Solana strategy that records which slots the launchpad scan reads.

    ``poll_launchpad_blocks`` is the only caller of the two pieces of state a
    reorg has to rewind, so recording the slots it requests is the observable
    proof that an orphaned range is rescanned rather than skipped.
    """

    def __init__(self, blocks: dict[int, list[dict[str, Any]]], latest: int) -> None:
        super().__init__()
        self.blocks = blocks
        self.latest = latest
        self.scanned: list[int] = []

    async def fetch_block_number(self) -> int:
        return self.latest

    async def _rpc_call_versioned(self, method: str, params: list[Any]) -> dict[str, Any] | None:
        if method != "getBlock":
            return None
        slot = int(params[0])
        self.scanned.append(slot)
        return {"result": {"blockhash": f"bh-{slot}", "transactions": self.blocks.get(slot, [])}}


def _run_rollback(strategy: SolanaStrategy, ch: FakeCH, fork: int) -> Any:
    """Drive ``reorg_stage`` to a confirmed REORG and its rollback.

    Args:
        strategy: The strategy under test.
        ch: ClickHouse double.
        fork: Slot to make the fork land on.

    Returns:
        The :class:`ReorgCheck` returned by ``reorg_stage``.
    """

    async def _current(_client: object, _slot: int) -> str:
        return "current"

    async def _hash(_client: object, slot: int) -> str:
        return f"bh-{slot}"

    with (
        patch("lite_indexer.strategies.solana.strategy.sync.REORG_PROTECTION_ENABLED", True),
        patch.object(sol_reorg, "_get_blockhash", _hash),
        patch.object(sol_reorg, "check_reorg", _fake_reorg(fork)),
        patch("lite_indexer.strategies.solana.strategy.sync.check_reorg", _fake_reorg(fork)),
    ):
        del _current
        return asyncio.run(strategy.reorg_stage(1, 100, ch))


def _fake_reorg(fork: int) -> Any:
    """Return a ``check_reorg`` stand-in that always reports a fork at ``fork``."""

    async def _check(_client: object, _slot: int, _ch: object) -> Any:
        from lite_indexer.strategies.reorg_status import ReorgCheck

        return ReorgCheck(ReorgStatus.REORG, fork_block=fork)

    return _check


def test_reorg_stage_rewinds_cursor_and_seen_mints() -> None:
    """The rollback path lowers the cursor AND empties the seen-mints set.

    Both, not one: a cursor-only rewind would re-scan the range and then skip
    every orphaned mint, because the set still holds them.
    """
    strategy = SolanaStrategy()
    strategy._launchpad_last_slot = 95
    for mint in (MINT, MINT2):
        strategy._launchpad_seen_mints.add(mint)

    ch = FakeCH()
    outcome = _run_rollback(strategy, ch, FORK_SLOT)

    assert outcome.status is ReorgStatus.REORG
    assert ch.deletes, "the rollback DELETE was never issued"
    assert strategy._launchpad_last_slot == FORK_SLOT
    assert len(strategy._launchpad_seen_mints) == 0


def test_post_rollback_scan_rescans_the_orphaned_range() -> None:
    """After a rollback the next launchpad poll reads the orphaned slots.

    This is the end-to-end claim: the re-fetch genuinely re-reads the fork's
    range and re-buffers the orphaned launch, so a rollback of
    ``solana_launches`` deletes rows that ARE rebuilt.
    """
    fork, tip = FORK_SLOT, 71
    blocks = {slot: [_create_tx(MINT, slot)] for slot in range(fork, tip + 1)}
    strategy = RecordingStrategy(blocks, tip)
    # Pretend the pre-reorg cycle already scanned up to the tip and buffered
    # the mint — exactly the state a rollback has to undo.
    asyncio.run(strategy.poll_launchpad_blocks())
    assert strategy._launchpad_last_slot == tip
    assert MINT in strategy._launchpad_seen_mints
    assert strategy._last_launch_events

    strategy._last_launch_events.clear()
    strategy.scanned.clear()

    outcome = _run_rollback(strategy, FakeCH(), fork)
    assert outcome.status is ReorgStatus.REORG

    buffered = asyncio.run(strategy.poll_launchpad_blocks())

    # Every slot ABOVE the fork was orphaned and must be re-requested; the
    # fork itself is the common ancestor (its rows are retained) and is not.
    assert {s for s in strategy.scanned if fork < s <= tip} == set(range(fork + 1, tip + 1)), (
        f"the orphaned range was not re-requested, scanned={sorted(strategy.scanned)}"
    )
    assert fork not in strategy.scanned, "the common ancestor is not orphaned and must not be re-read"
    assert buffered == 1, "the orphaned launch was not re-buffered"
    assert {e.mint for e in strategy._last_launch_events} == {MINT}


def test_rollback_purges_solana_launches_but_not_solana_bundles() -> None:
    """``solana_launches`` is rollback-eligible; ``solana_bundles`` is not.

    The launches writer rewinds, so its rows are rebuilt by the re-scan. The
    bundles writer is driven by a partial signature walk over a TRACKED mint,
    which never enumerates the slot's buyers or its Jito tip transfer — a
    cursor rewind makes the range get scanned but cannot make that evidence
    recoverable, so the table must stay out of the rollback.
    """
    covered = {target.table for target in SOLANA_ROLLBACK_TABLES}

    assert SOLANA_LAUNCHES_TABLE in covered, (
        "solana_launches is rollback-eligible again now that the launchpad cursor and seen-mints set rewind on a reorg"
    )
    assert SOLANA_BUNDLES_TABLE not in covered, (
        "solana_bundles still has no replay path: detect_bundle needs the "
        "create tx, the fresh buyers and the last-tx Jito tip transfer, and "
        "the mint signature walk only ever enumerates a TRACKED mint's txs"
    )
    assert describe_reorg_exclusion(SOLANA_BUNDLES_TABLE).reason == "no-replay-path"


def test_rewind_is_reachable_only_from_the_reorg_path() -> None:
    """``rewind_launchpad_state`` is called from the rollback path, not elsewhere.

    An AST pin, not a comment: a rewind added to the normal scan path would
    rewind the cursor every cycle and re-scan forever.
    """
    tree = ast.parse((REPO_ROOT / STAGE_MODULE).read_text(encoding="utf-8"))
    callers = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and any(isinstance(sub, ast.Name) and sub.id == "rewind_launchpad_state" for sub in ast.walk(node))
    }

    assert callers == {"reorg_stage"}, f"rewind_launchpad_state is now called from {sorted(callers)}"
    assert launchpad_rewind.rewind_launchpad_state.__module__.endswith("launchpad_rewind")


def test_rewind_module_rewinds_both_attributes() -> None:
    """The rewind writes the cursor AND discards every seen mint.

    An AST pin so a partial rewind (cursor only) is caught structurally and
    not only through the behavioural test above.
    """
    source = inspect.getsource(launchpad_rewind.rewind_launchpad_state)

    assert "_launchpad_last_slot = fork_slot" in source, "the cursor is no longer lowered to the fork"
    assert ".discard(" in source, "the seen-mints set is no longer emptied"


@pytest.mark.parametrize("attribute", ["_launchpad_last_slot", "_launchpad_seen_mints"])
def test_rewind_leaves_no_state_unrewound(attribute: str) -> None:
    """Both writer attributes are named in the rewind's declared contract.

    The registry is the canonical record of what a reorg must rewind; if it
    drops an attribute, the rewind is no longer proven complete.
    """
    from lite_indexer.strategies.solana.reorg_launch_reconstruct import reorg_launch_writers

    recorded = {w.attribute: w for w in reorg_launch_writers}

    assert attribute in recorded, f"{attribute} is not recorded in reorg_launch_writers"
    assert recorded[attribute].reset_on_reorg is True
    assert Path(REPO_ROOT / (recorded[attribute].owner.replace(".", "/") + ".py")).exists()
