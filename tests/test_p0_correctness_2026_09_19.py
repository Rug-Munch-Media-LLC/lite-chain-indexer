"""P0 correctness fixes from the 2026-09-19 forensic audit (§1).

RC-1 — deferred eth_getLogs returns None (failure shape), never [].
RC-2 — every sync phase is time-bound (RPC_PHASE_TIMEOUT_S).
RC-3 — zero-write stall breaker: 3 consecutive zero-write failures for the
       same range defer the range to the backfill queue and advance state.
SEC-1 — historical._lit uses SQL-standard quote doubling (DuckDB executes
       the rendered WHERE).
"""

from __future__ import annotations

import time
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from lite_indexer.indexer._zero_write import MAX_CONSECUTIVE_ZERO_WRITES, ZeroWriteTracker
from lite_indexer.rpc import client as rpc_client
from lite_indexer.storage import historical as historical_mod

# ── RC-1 — deferred getLogs is None, not [] ─────────────────────────────


@pytest.mark.asyncio
async def test_deferred_getlogs_returns_none(monkeypatch):
    """Quota-deferred eth_getLogs must return None (failure), never []."""
    monkeypatch.setattr(rpc_client, "_defer_reason_for", lambda *a, **k: "quota_level_2")
    client = rpc_client.RpcClient()
    out = await client.get_logs("https://rpc.example", 0, 500, "0xdeadbeef")
    assert out is None


@pytest.mark.asyncio
async def test_deferred_getlogs_none_is_not_empty_list(monkeypatch):
    """Guard the exact regression: [] was indistinguishable from no logs."""
    monkeypatch.setattr(rpc_client, "_defer_reason_for", lambda *a, **k: "soft_ban")
    client = rpc_client.RpcClient()
    out = await client.get_logs("https://rpc.example", 0, 500, "0xdeadbeef")
    assert out != []


# ── RC-2 — every sync phase is time-bound ───────────────────────────────


def test_phase_timeout_constant_exists():
    from lite_indexer.indexer._phase_guard import RPC_PHASE_TIMEOUT_S

    assert RPC_PHASE_TIMEOUT_S == 300.0


class _StubState:
    def __init__(self) -> None:
        self.saved: list[tuple[int, int]] = []

    def save(self, block: int, events: int, **kwargs: object) -> None:
        self.saved.append((block, events))

    def load(self) -> dict[str, int]:
        return {"last_block": self.saved[-1][0] if self.saved else 0}


class _StubHighWater:
    def __init__(self) -> None:
        self.marks: list[int] = []

    def update(self, block: int) -> None:
        self.marks.append(block)


class _StubQueue:
    def __init__(self) -> None:
        self.chunks: list[object] = []

    def enqueue(self, chunks: list[object]) -> int:
        self.chunks.extend(chunks)
        return len(chunks)


class _StubLoop:
    """Minimal SyncLoopMixin host for the RC-3 helpers."""

    chain = "testchain"

    def __init__(self) -> None:
        self.state = _StubState()
        self._highwater = _StubHighWater()
        self._backfill_queue = _StubQueue()

    _get_backfill_queue = lambda self: self._backfill_queue  # noqa: E731
    _clear_reorg_recovery = lambda self: None  # noqa: E731
    _enabled_event_types = lambda self: []  # noqa: E731
    _save_current_state = lambda self: None  # noqa: E731


def _stub() -> _StubLoop:
    stub = _StubLoop()
    # Bind the mixin methods onto the stub instance.
    from lite_indexer.indexer._sync_loop import SyncLoopMixin

    for name in ("_zero_write_tracker", "_defer_stalled_range"):
        setattr(stub, name, getattr(SyncLoopMixin, name).__get__(stub, _StubLoop))
    return stub


def test_zero_write_tracker_trips_after_three():
    tracker = ZeroWriteTracker("c")
    for _ in range(MAX_CONSECUTIVE_ZERO_WRITES - 1):
        assert tracker.record_zero_write(100, 200) is False
    assert tracker.record_zero_write(100, 200) is True


def test_zero_write_tracker_resets_on_success():
    tracker = ZeroWriteTracker("c")
    tracker.record_zero_write(100, 200)
    tracker.record_zero_write(100, 200)
    tracker.record_success(100)
    assert tracker.record_zero_write(100, 200) is False


def test_zero_write_tracker_new_range_resets():
    tracker = ZeroWriteTracker("c")
    tracker.record_zero_write(100, 200)
    tracker.record_zero_write(100, 200)
    # Progress to a new range — counter keyed by range start resets.
    assert tracker.record_zero_write(300, 400) is False


def test_defer_stalled_range_enqueues_and_advances():
    stub = _stub()
    summary: dict[str, object] = {"errors": []}
    out = stub._defer_stalled_range((100, 200), summary, time.monotonic(), "transfers")
    assert out["status"] == "writer_stalled_deferred"
    assert stub.state.saved and stub.state.saved[-1][0] == 200  # state advanced
    assert stub._highwater.marks == [200]
    assert len(stub._backfill_queue.chunks) == 1
    chunk = stub._backfill_queue.chunks[0]
    assert (chunk.chain, chunk.from_block, chunk.to_block) == ("testchain", 100, 200)


@pytest.mark.asyncio
async def test_dex_phase_timeout_aborts_without_advance():
    """RC-2: a hung dex fetch aborts the cycle with fetch_failed, no advance."""
    import tempfile
    from pathlib import Path

    from lite_indexer.indexer import LiteIndexer

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        orig_state_dir = None
        import lite_indexer.state as state_mod

        orig_state_dir = state_mod.STATE_DIR
        state_mod.STATE_DIR = tmp_path / "state"
        try:
            indexer = LiteIndexer(chain="ethereum", from_block=100, to_block=200, data_dir=tmp_path)
            indexer.state.save(99, 0)
            indexer.dex_enabled = True
            indexer.deployers_enabled = False
            indexer.traces_enabled = False
            strategy = indexer.strategy
            strategy.fetch_block_number = AsyncMock(return_value=500)
            strategy.fetch_transfers = AsyncMock(return_value=[])
            strategy.fetch_block_timestamps = AsyncMock(return_value={})
            strategy.fetch_swap_events = AsyncMock(side_effect=TimeoutError())
            indexer._enrich_timestamps = AsyncMock(side_effect=lambda evs: [dict(e) for e in evs])
            indexer.fetch_block_timestamps = AsyncMock(return_value={})
            monkey_ch = Mock()
            monkey_ch.write_wallet_features = AsyncMock(return_value={})
            indexer._ch_writer = monkey_ch
            indexer._ch_health = type("_H", (), {"ok": lambda self: True})()

            summary = await indexer._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic())
            assert summary["status"] == "fetch_failed"
            assert indexer.state.load()["last_block"] == 99
        finally:
            state_mod.STATE_DIR = orig_state_dir


# ── SEC-1 — SQL-standard quote doubling in historical._lit ──────────────


def test_lit_doubles_single_quotes():
    lit = historical_mod.HistoricalAccess._lit("it's")
    assert lit == "'it''s'"
    assert "\\'" not in lit


def test_lit_payload_with_quote_roundtrips_duckdb():
    """A string containing a quote must round-trip through a real DuckDB."""
    import duckdb

    payload = "0xabc'; DROP TABLE x; --"
    lit = historical_mod.HistoricalAccess._lit(payload)
    con = duckdb.connect(":memory:")
    out = con.execute(f"SELECT {lit}").fetchone()[0]
    assert out == payload
    con.close()


def test_lit_datetime_and_bool_unchanged():
    dt = historical_mod.HistoricalAccess._lit(datetime(2026, 9, 19, 12, 0, 0))
    assert dt == "'2026-09-19 12:00:00'"
    assert historical_mod.HistoricalAccess._lit(True) == "1"
