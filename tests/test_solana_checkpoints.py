"""Regression: Solana checkpoints persist the real finalized slot.

``wallet_memory.solana_checkpoints`` is keyed by the real slot the sync
loop passes to ``reorg_stage``. A checkpoint writer that invented its own
sequence (1..N) would make reorg detection meaningless; this guards that
the slot passed in is the slot persisted.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

from lite_indexer.strategies.solana import reorg


def test_checkpoint_slot_persists_the_real_slot(monkeypatch):
    slot = 443_718_051

    async def _fake_blockhash(_client: object, _slot: int) -> str:
        return "5" * 43

    monkeypatch.setattr(reorg, "_get_blockhash", _fake_blockhash)
    ch = MagicMock()
    ch.execute.return_value = 1

    ok = asyncio.run(reorg.checkpoint_slot(object(), slot, ch))

    assert ok is True
    sql, params = ch.execute.call_args.args
    assert "INSERT INTO" in sql
    assert params[0][0] == slot
    assert params[0][1] == "5" * 43
