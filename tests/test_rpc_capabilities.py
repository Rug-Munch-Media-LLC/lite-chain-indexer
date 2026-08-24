"""Capability registry + probe classification tests (was 32% covered).

Offline: RpcClient stubbed. Contracts:

* registry: get/set/copy semantics for CapabilityRecord
* probe_archive: hex balance at block 1 → True; missing-trie error falls
  back to block 256; both blocks failing → False
* _probe_getlogs_once: result list → OK; range-limit message →
  RANGE_ERROR; other errors → OTHER_ERROR
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc.capabilities import (
    CapabilityRecord,
    capability_records,
    get_capability,
    set_capability,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    cap._CAPABILITY_RECORDS.clear()
    yield
    cap._CAPABILITY_RECORDS.clear()


class StubClient:
    def __init__(self, results):
        self._results = list(results)
        self.calls: list[tuple[str, str]] = []

    async def call(self, endpoint, method, params=None, timeout=None):
        self.calls.append((endpoint, method))
        r = self._results.pop(0) if self._results else None
        if isinstance(r, Exception):
            raise r
        return r


# ── Registry ──────────────────────────────────────────────────────────


def test_registry_set_get_copy():
    rec = CapabilityRecord(endpoint="https://x", chain="ethereum", archive=True)
    set_capability(rec)
    assert get_capability("https://x") is rec
    copy = capability_records()
    assert "https://x" in copy
    assert copy is not cap._CAPABILITY_RECORDS  # dict-level defensive copy
    assert get_capability("https://missing") is None


# ── probe_archive ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_archive_true_on_genesis_balance():
    client = StubClient([{"result": "0x0"}])  # balance at block 1, even zero
    assert await cap.probe_archive(client, "https://a", "ethereum") is True
    assert ("https://a", "eth_getBalance") in client.calls


@pytest.mark.asyncio
async def test_archive_falls_back_to_block_256():
    client = StubClient(
        [
            {"error": {"message": "missing trie node"}},
            {"result": "0xde0b6b3a7640000"},
        ]
    )
    assert await cap.probe_archive(client, "https://b", "ethereum") is True
    methods = [m for _, m in client.calls]
    assert methods == ["eth_getBalance", "eth_getBalance"]


@pytest.mark.asyncio
async def test_archive_false_when_both_blocks_fail():
    client = StubClient(
        [
            {"error": {"message": "missing trie node"}},
            {"error": {"message": "header not found"}},
        ]
    )
    assert await cap.probe_archive(client, "https://c", "ethereum") is False


@pytest.mark.asyncio
async def test_archive_false_on_transport_error():
    """RpcClient signals transport failure as None — probe retries block
    256 then gives up (False), never raising into verification."""
    client = StubClient([None, None])
    assert await cap.probe_archive(client, "https://d", "ethereum") is False
    assert len(client.calls) == 2  # tried both blocks


# ── _probe_getlogs_once classification ────────────────────────────────


@pytest.mark.asyncio
async def test_getlogs_ok_classification(monkeypatch):
    sent: dict = {}

    class FakeResp:
        status_code = 200

        def json(self):
            return {"result": []}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *excinfo):
            return None

        async def post(self, url, json=None, timeout=None):  # noqa: A002
            sent.update(json)
            return FakeResp()

    monkeypatch.setattr(cap.httpx, "AsyncClient", lambda **k: FakeClient())
    status = await cap._probe_getlogs_once("https://e", 1_000_000 - 512 + 1, 1_000_000, "0xtopic")
    assert status is cap._GetLogsProbeStatus.OK
    assert sent["params"][0]["toBlock"] == hex(1_000_000)
    assert sent["params"][0]["fromBlock"] == hex(1_000_000 - 512 + 1)
    assert sent["params"][0]["topics"] == ["0xtopic"]


@pytest.mark.asyncio
async def test_getlogs_range_error_classification(monkeypatch):
    class FakeResp:
        status_code = 200

        def json(self):
            return {"error": {"message": "exceeds max results"}}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *excinfo):
            return None

        async def post(self, url, json=None, timeout=None):  # noqa: A002
            return FakeResp()

    monkeypatch.setattr(cap.httpx, "AsyncClient", lambda **k: FakeClient())
    status = await cap._probe_getlogs_once("https://f", 901, 1_000, "0xtopic")
    assert status is cap._GetLogsProbeStatus.RANGE_ERROR


@pytest.mark.asyncio
async def test_getlogs_other_error_classification(monkeypatch):
    class FakeResp:
        status_code = 200

        def json(self):
            return {"error": {"message": "method not found"}}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *excinfo):
            return None

        async def post(self, url, json=None, timeout=None):  # noqa: A002
            return FakeResp()

    monkeypatch.setattr(cap.httpx, "AsyncClient", lambda **k: FakeClient())
    status = await cap._probe_getlogs_once("https://g", 901, 1_000, "0xtopic")
    assert status is cap._GetLogsProbeStatus.OTHER_ERROR
