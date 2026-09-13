"""ClickHouse write offload tests (storage/ch_write_pool.py + thread-local client).

Contracts:

* ``run_ch_write`` executes the blocking write off the event loop and returns
  its value / propagates its exception unchanged.
* ``ClickHouseWriter._connect`` gives each worker thread its OWN client
  (thread-local), reuses it within that thread, and still honours a
  test-injected shared ``_client``.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from lite_indexer.storage.ch_write_pool import run_ch_write
from lite_indexer.storage.clickhouse import ClickHouseWriter


def test_run_ch_write_returns_value() -> None:
    assert asyncio.run(run_ch_write(lambda x: x + 1, 41)) == 42


def test_run_ch_write_passes_kwargs() -> None:
    def _add(a: int, *, b: int) -> int:
        return a + b

    assert asyncio.run(run_ch_write(_add, 1, b=2)) == 3


def test_run_ch_write_propagates_exception() -> None:
    def _boom() -> None:
        raise ValueError("write failed")

    with pytest.raises(ValueError, match="write failed"):
        asyncio.run(run_ch_write(_boom))


class _FakeClient:
    """Records the creating thread so thread-locality can be asserted."""

    def __init__(self, **_: Any) -> None:
        self.thread = threading.get_ident()
        self.queries: list[str] = []

    def execute(self, query: str, params: Any = None) -> list[tuple[()]]:
        self.queries.append(query)
        return []


def _run_in_thread(fn: Any) -> Any:
    box: dict[str, Any] = {}

    def _target() -> None:
        box["result"] = fn()

    thread = threading.Thread(target=_target)
    thread.start()
    thread.join()
    return box["result"]


def test_connect_is_thread_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("clickhouse_driver.Client", _FakeClient)
    writer = ClickHouseWriter()

    main_client = writer._connect()  # noqa: SLF001
    worker_a = _run_in_thread(writer._connect)  # noqa: SLF001
    worker_b = _run_in_thread(writer._connect)  # noqa: SLF001

    # Distinct worker threads must not share one connection.
    assert isinstance(main_client, _FakeClient)
    assert isinstance(worker_a, _FakeClient)
    assert isinstance(worker_b, _FakeClient)
    assert len({id(main_client), id(worker_a), id(worker_b)}) == 3

    # ...but a thread reuses its own connection.
    same_thread: list[Any] = []

    def _twice() -> None:
        same_thread.append(writer._connect())  # noqa: SLF001
        same_thread.append(writer._connect())  # noqa: SLF001

    _run_in_thread(_twice)
    assert same_thread[0] is same_thread[1]


def test_injected_client_is_shared_across_threads() -> None:
    writer = ClickHouseWriter()
    sentinel = _FakeClient()
    writer._client = sentinel  # noqa: SLF001 — test seam

    assert writer._connect() is sentinel  # noqa: SLF001
    assert _run_in_thread(writer._connect) is sentinel  # noqa: SLF001


def test_offloaded_write_transfers_matches_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    writer = ClickHouseWriter()
    writer._client = _FakeClient()  # type: ignore[assignment]  # noqa: SLF001
    events = [
        {
            "blockNumber": 20_000_000,
            "txHash": "0x" + "11" * 32,
            "from": "0x" + "aa" * 20,
            "to": "0x" + "bb" * 20,
            "tokenAddress": "0x" + "cc" * 20,
            "value": "1000",
            "blockTimestamp": "2026-09-13T00:00:00Z",
            "logIndex": 0,
        }
    ]
    sync_count = writer.write_transfers("ethereum", events)
    async_count = asyncio.run(run_ch_write(writer.write_transfers, "ethereum", events))
    assert sync_count == async_count == 1
