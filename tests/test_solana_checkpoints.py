"""Regression tests for Solana reorg staging / fail-closed behaviour.

* ``checkpoint_slot`` persists the real finalized slot (not a counter).
* ``check_reorg`` / ``detect_reorg`` are tri-state: match → NO_REORG,
  mismatch → REORG, no checkpoint → UNKNOWN, lookup failure → UNKNOWN
  (never fail open as "no reorg").
* ``SyncMixin.reorg_stage`` fails CLOSED on UNKNOWN (returns UNKNOWN rather
  than proceeding), surfaces rollback failures, and normalises the benign
  no-checkpoint case to NO_REORG so catch-up does not stall.
"""

from __future__ import annotations

from unittest.mock import patch

import asyncio
from typing import Any
from unittest.mock import MagicMock

from lite_indexer.strategies.reorg_status import ReorgStatus
from lite_indexer.strategies.solana import SolanaStrategy
from lite_indexer.strategies.solana import reorg as sol_reorg

CHECKPOINTS = "wallet_memory.solana_checkpoints"


class FakeCH:
    """Minimal ClickHouse double for Solana reorg staging."""

    def __init__(self) -> None:
        self.checkpoints: dict[int, str] = {}
        self.deletes: list[str] = []
        self.fail_select = False
        self.fail_delete = False

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        if query.startswith(f"CREATE TABLE IF NOT EXISTS {CHECKPOINTS}"):
            return 1
        if query.startswith(f"INSERT INTO {CHECKPOINTS}"):
            for row in params or []:
                self.checkpoints[int(row[0])] = str(row[1])
            return len(params or [])
        if f"SELECT blockhash FROM {CHECKPOINTS}" in query:
            if self.fail_select:
                raise RuntimeError("connection refused")
            stored = self.checkpoints.get(int(params["slot"]))
            return [(stored,)] if stored else []
        if query.startswith("DELETE FROM"):
            if self.fail_delete:
                raise RuntimeError("privileges")
            self.deletes.append(query)
            return 3
        return []


def test_checkpoint_slot_persists_the_real_slot():
    slot = 443_718_051

    async def _fake_blockhash(_client: object, _slot: int) -> str:
        return "5" * 43

    with patch.object(sol_reorg, "_get_blockhash", _fake_blockhash):
        ch = MagicMock()
        ch.execute.return_value = 1

        ok = asyncio.run(sol_reorg.checkpoint_slot(object(), slot, ch))

    assert ok is True
    sql, params = ch.execute.call_args.args
    assert "INSERT INTO" in sql
    assert params[0][0] == slot
    assert params[0][1] == "5" * 43


def _strategy() -> SolanaStrategy:
    return SolanaStrategy()


def test_solana_check_reorg_tristate():
    ch = FakeCH()
    ch.checkpoints[68] = "stored"

    async def _current(_client: object, _slot: int) -> str:
        return "current"

    with patch.object(sol_reorg, "_get_blockhash", _current):
        check = asyncio.run(sol_reorg.check_reorg(object(), 100, ch))
        assert check.status is ReorgStatus.REORG
        assert check.fork_block == 68

        # Lookup failure → UNKNOWN with an error (never fail open).
        ch.fail_select = True
        check = asyncio.run(sol_reorg.check_reorg(object(), 100, ch))
        assert check.status is ReorgStatus.UNKNOWN
        assert check.error

        # No checkpoint → UNKNOWN, benign (no error).
        ch = FakeCH()
        check = asyncio.run(sol_reorg.check_reorg(object(), 100, ch))
        assert check.status is ReorgStatus.UNKNOWN
        assert check.error is None


def test_reorg_stage_no_checkpoint_is_benign():
    async def _hash(_client: object, _slot: int) -> str:
        return "h"

    with (
        patch("lite_indexer.strategies.solana.strategy.sync.REORG_PROTECTION_ENABLED", True),
        patch.object(sol_reorg, "_get_blockhash", _hash),
    ):
        ch = FakeCH()
        outcome = asyncio.run(_strategy().reorg_stage(1, 100, ch))

    assert outcome.status is ReorgStatus.NO_REORG
    assert ch.deletes == []


def test_reorg_stage_lookup_failure_is_unknown_and_does_not_rollback():
    async def _hash(_client: object, _slot: int) -> str:
        return "h"

    with (
        patch("lite_indexer.strategies.solana.strategy.sync.REORG_PROTECTION_ENABLED", True),
        patch.object(sol_reorg, "_get_blockhash", _hash),
    ):
        ch = FakeCH()
        ch.fail_select = True
        outcome = asyncio.run(_strategy().reorg_stage(1, 100, ch))

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.error
    assert ch.deletes == []  # did NOT proceed to rollback


def test_reorg_stage_detects_and_rolls_back():
    async def _current(_client: object, _slot: int) -> str:
        return "current"

    with (
        patch("lite_indexer.strategies.solana.strategy.sync.REORG_PROTECTION_ENABLED", True),
        patch.object(sol_reorg, "_get_blockhash", _current),
    ):
        ch = FakeCH()
        ch.checkpoints[68] = "stored"
        outcome = asyncio.run(_strategy().reorg_stage(1, 100, ch))

    assert outcome.status is ReorgStatus.REORG
    assert outcome.fork_block == 68
    assert ch.deletes  # rollback issued


def test_reorg_stage_rollback_failure_is_unknown():
    async def _current(_client: object, _slot: int) -> str:
        return "current"

    with (
        patch("lite_indexer.strategies.solana.strategy.sync.REORG_PROTECTION_ENABLED", True),
        patch.object(sol_reorg, "_get_blockhash", _current),
    ):
        ch = FakeCH()
        ch.checkpoints[68] = "stored"
        ch.fail_delete = True
        outcome = asyncio.run(_strategy().reorg_stage(1, 100, ch))

    assert outcome.status is ReorgStatus.UNKNOWN
    assert outcome.fork_block == 68
    assert outcome.error == "rollback failed"
