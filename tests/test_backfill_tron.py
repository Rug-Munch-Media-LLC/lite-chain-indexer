"""Tests for the TRON backfill executor + endpoint fallback.

TRON has no freemium ``eth_getLogs`` pool, so the EVM executor hard-fails
(``_is_evm()``) and the freemium selector returns None — which left
``backfill_queue_tron.json`` stuck at ``attempts: 0`` forever. The TRON
executor fetches through the strategy, and the default selector falls back
to the public TronGrid REST endpoint. Hermetic — no network.
"""

from __future__ import annotations

import asyncio

from lite_indexer.indexer import backfill as bf
from lite_indexer.indexer.backfill import BackfillChunk
from lite_indexer.indexer.backfill_executor import build_tron_backfill_executor
from lite_indexer.rpc import RpcFetchFailedError
from lite_indexer.strategies.reorg_status import ReorgStatus


class FakeReorgCheck:
    """Outcome shape the guarded-write seam reads."""

    def __init__(self, status: ReorgStatus = ReorgStatus.NO_REORG) -> None:
        self.status = status
        self.fork_block: int | None = None
        self.error: str | None = None


class FakeWriter:
    def client(self) -> object:
        return object()


class FakeStrategy:
    chain = "tron"

    def __init__(
        self,
        events: list[dict] | None = None,
        fail: bool = False,
        reorg: ReorgStatus = ReorgStatus.NO_REORG,
    ) -> None:
        self._events = events or []
        self._fail = fail
        self._reorg = reorg
        self.fetch_calls: list[tuple[int, int]] = []
        self.reorg_calls: list[tuple[int, int]] = []
        self.written: list[list[dict]] = []

    async def fetch_transfers(self, from_block: int, to_block: int) -> list[dict]:
        self.fetch_calls.append((from_block, to_block))
        if self._fail:
            raise RpcFetchFailedError("tron down")
        return self._events

    async def reorg_stage(self, from_block: int, to_block: int, ch_client: object = None) -> FakeReorgCheck:
        self.reorg_calls.append((from_block, to_block))
        return FakeReorgCheck(self._reorg)

    def write_clickhouse(self, events: list[dict], ch_writer: object) -> int:
        self.written.append(events)
        return len(events)


class FakeHost:
    chain = "tron"

    def __init__(self, strategy: FakeStrategy) -> None:
        self.strategy = strategy
        self.ch_writer = FakeWriter()


def test_tron_executor_fetches_then_writes() -> None:
    strategy = FakeStrategy(events=[{"txHash": "0x1"}])
    executor = build_tron_backfill_executor(FakeHost(strategy))

    ok = asyncio.run(executor(BackfillChunk("tron", 100, 199), "https://api.trongrid.io"))

    assert ok is True
    assert strategy.fetch_calls == [(100, 199)]
    assert strategy.written == [[{"txHash": "0x1"}]]


def test_tron_executor_reorg_checks_before_writing() -> None:
    """Every backfill write must pass the fork check, not write blind."""
    strategy = FakeStrategy(events=[{"txHash": "0x1"}])
    executor = build_tron_backfill_executor(FakeHost(strategy))

    asyncio.run(executor(BackfillChunk("tron", 100, 199), "https://api.trongrid.io"))

    assert strategy.reorg_calls == [(100, 199)]


def test_tron_executor_refuses_to_write_when_reorg_detected() -> None:
    strategy = FakeStrategy(events=[{"txHash": "0x1"}], reorg=ReorgStatus.REORG)
    executor = build_tron_backfill_executor(FakeHost(strategy))

    ok = asyncio.run(executor(BackfillChunk("tron", 100, 199), "https://api.trongrid.io"))

    assert ok is False
    assert strategy.written == []


def test_tron_executor_refuses_to_write_when_reorg_unknown() -> None:
    """UNKNOWN (cannot verify) refuses too — losing data beats writing blind."""
    strategy = FakeStrategy(events=[{"txHash": "0x1"}], reorg=ReorgStatus.UNKNOWN)
    executor = build_tron_backfill_executor(FakeHost(strategy))

    ok = asyncio.run(executor(BackfillChunk("tron", 100, 199), "https://api.trongrid.io"))

    assert ok is False
    assert strategy.written == []


def test_tron_executor_empty_range_is_success() -> None:
    strategy = FakeStrategy(events=[])
    executor = build_tron_backfill_executor(FakeHost(strategy))

    assert asyncio.run(executor(BackfillChunk("tron", 1, 2), "")) is True


def test_tron_executor_rpc_failure_returns_false_for_retry() -> None:
    strategy = FakeStrategy(fail=True)
    executor = build_tron_backfill_executor(FakeHost(strategy))

    assert asyncio.run(executor(BackfillChunk("tron", 1, 2), "")) is False


def test_default_selector_falls_back_to_tron_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(bf, "select_freemium_endpoint_for_chain", lambda chain: None)
    endpoint = bf._default_endpoint_selector("tron")
    assert endpoint is not None
    assert "trongrid" in endpoint


def test_default_selector_leaves_evm_without_freemium(monkeypatch) -> None:
    monkeypatch.setattr(bf, "select_freemium_endpoint_for_chain", lambda chain: None)
    assert bf._default_endpoint_selector("ethereum") is None
