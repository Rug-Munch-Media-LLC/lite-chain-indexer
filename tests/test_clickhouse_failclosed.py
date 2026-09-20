"""Fail-closed ClickHouse write semantics (2026-09-20 outage hole).

An unreachable ClickHouse must RAISE ``ClickHouseUnavailable`` so the sync
cycle aborts WITHOUT checkpointing state. Returning 0 was read as "nothing
to insert", the per-slice checkpoint advanced, and the outage window became
a permanent fleet-wide hole (~20 chains × 80 min).

Only connection-level failures raise; a live server rejecting the query
(``ServerException``) still returns 0, and a dedup-only batch (all rows
already durable) is a legitimate zero and returns 0 normally.

Hermetic: DI fakes / subclass seams, no network, no monkeypatch fixture.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from clickhouse_driver.errors import NetworkError, ServerException

from lite_indexer.indexer._sync_loop import SyncLoopMixin
from lite_indexer.storage.clickhouse import ClickHouseUnavailable, ClickHouseWriter


@contextmanager
def _env(**values: str) -> Iterator[None]:
    """Set env vars for the block, restoring the prior state."""
    originals = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, original in originals.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


class FakeClient:
    """Minimal ClickHouse client: dedup SELECT + INSERT, optional insert error."""

    def __init__(
        self,
        existing: list[tuple[Any, ...]] | None = None,
        insert_error: BaseException | None = None,
    ) -> None:
        self.existing = existing or []
        self.insert_error = insert_error
        self.inserts: list[Any] = []

    def execute(self, query: str, params: Any = None) -> Any:
        if query.strip().upper().startswith("INSERT"):
            if self.insert_error is not None:
                raise self.insert_error
            self.inserts.append((query, params))
            return None
        if "SELECT block_number, tx_hash, log_index" in query:
            return self.existing
        return []


class _UnavailableWriter(ClickHouseWriter):
    """Writer whose connection is refused (server down)."""

    def _connect(self) -> Any | None:
        return None


def _event(block: int, tx: str, idx: int) -> dict[str, Any]:
    return {
        "blockNumber": block,
        "txHash": tx,
        "logIndex": idx,
        "from": "TFrom",
        "to": "TTo",
        "tokenAddress": "TToken",
        "value": "1000",
        "blockTimestamp": "2023-11-14T22:13:20Z",
    }


# ── Unavailability raises (never 0) ─────────────────────────────────────


def test_write_transfers_unavailable_raises() -> None:
    with pytest.raises(ClickHouseUnavailable):
        _UnavailableWriter().write_transfers("tron", [_event(1, "0xaa", 0)])


def test_write_dex_events_unavailable_raises() -> None:
    dex = {"blockNumber": 1, "transactionHash": "0xaa", "logIndex": 0, "token0": "0xt", "token1": "0xu"}
    with pytest.raises(ClickHouseUnavailable):
        _UnavailableWriter().write_dex_events("tron", [dex])


@pytest.mark.asyncio
async def test_write_deployer_events_unavailable_raises() -> None:
    ev = {"blockNumber": 1, "transactionHash": "0xaa", "token0": "0xt"}
    with pytest.raises(ClickHouseUnavailable):
        await _UnavailableWriter().write_deployer_events("base", [ev])
    with pytest.raises(ClickHouseUnavailable):
        await _UnavailableWriter().write_solana_deployer_events("solana", [ev])


@pytest.mark.parametrize("err", [ConnectionRefusedError("refused"), NetworkError("eof"), TimeoutError("timed out")])
def test_insert_connection_errors_raise(err: BaseException) -> None:
    writer = ClickHouseWriter()
    writer._client = FakeClient(insert_error=err)
    with pytest.raises(ClickHouseUnavailable):
        writer.write_transfers("tron", [_event(1, "0xbb", 0)])


# ── Legitimate zero / data-error outcomes stay 0 ───────────────────────


def test_dedup_only_insert_returns_zero_success() -> None:
    writer = ClickHouseWriter()
    writer._client = FakeClient(existing=[(100, "0xaa", 0)])
    assert writer.write_transfers("tron", [_event(100, "0xaa", 0)]) == 0


def test_insert_server_exception_returns_zero() -> None:
    """A live server rejecting the query is data/schema drift, not an outage."""
    writer = ClickHouseWriter()
    writer._client = FakeClient(insert_error=ServerException("Code: 53. Type mismatch"))
    assert writer.write_transfers("tron", [_event(1, "0xcc", 0)]) == 0


# ── Sync slice: a raising writer aborts WITHOUT checkpointing ──────────


class _SliceHost:
    chain = "base"

    def __init__(self) -> None:
        self.saved: list[str] = []
        self.attempted: list[tuple[int, int]] = []

    async def _determine_block_range(self, summary: dict[str, Any]) -> tuple[int, int]:
        return (0, 149)

    def _save_current_state(self) -> None:
        self.saved.append("current_state")

    async def _sync_range_body(
        self, from_block: int, to_block: int, summary: dict[str, Any], start_time: float
    ) -> dict[str, Any]:
        self.attempted.append((from_block, to_block))
        raise ClickHouseUnavailable("server down")


def test_raising_writer_aborts_slice_without_checkpoint() -> None:
    import asyncio

    host = _SliceHost()
    host._sync_loop_body = SyncLoopMixin._sync_loop_body.__get__(host, _SliceHost)  # type: ignore[method-assign]
    with _env(SYNC_CHECKPOINT_SLICE="50"), pytest.raises(ClickHouseUnavailable):
        asyncio.run(host._sync_loop_body({"status": "running", "errors": []}, 0.0))

    assert host.saved == []  # no state.save — the slice can never be checkpointed
    assert host.attempted == [(0, 49)]  # stopped at the first (unavailable) slice


# ── Lake callers abort the run (never log-and-continue) ────────────────


class _NoClientWriter:
    def client(self) -> Any:
        return None


def test_lake_write_batch_raises_when_client_unavailable() -> None:
    from lite_indexer.ingest.lake_backfill import _write_batch

    with pytest.raises(ClickHouseUnavailable):
        _write_batch(_NoClientWriter(), "base", "traces", [{"x": 1}])


class _AbortSource:
    def available(self) -> bool:
        return True

    def read_transfers(self, chain: str, date: str) -> Any:
        return iter([{"blockNumber": 1}])


class _AbortProvider:
    name = "aws-sonarx-base"


class _AbortRouter:
    def select(self, chain: str, table: str) -> Any:
        return _AbortProvider()

    def report_success(self, *args: Any, **kwargs: Any) -> None:
        return None

    def report_failure(self, *args: Any, **kwargs: Any) -> None:
        return None


class _AbortWriter:
    def write_transfers(self, chain: str, batch: list[dict[str, Any]]) -> int:
        raise ClickHouseUnavailable("server down")


def test_lake_backfill_aborts_run_on_unavailable() -> None:
    from lite_indexer.ingest.lake_backfill import LakeBackfillRequest, backfill_chain_from_lake

    req = LakeBackfillRequest(chain="base", dates=["2026-09-08"], tables=("transfers",))
    with pytest.raises(ClickHouseUnavailable):
        backfill_chain_from_lake(req, source=_AbortSource(), writer=_AbortWriter(), router=_AbortRouter())
