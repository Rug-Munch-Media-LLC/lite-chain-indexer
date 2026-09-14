"""Tests for cross-provider JSON-RPC batching (RpcClient.call_batch).

Contracts:

* N independent calls collapse into ONE JSON-RPC array POST, results in order
* per-element JSON-RPC errors are isolated (returned in their own slot)
* batch-level failure transparently degrades to individual ``call`` requests
* ``RPC_BATCH_MAX`` bounds the array size (chunked internally)
* ``RPC_BATCH_ENABLED=0`` disables batching entirely
* metrics ``rpc_batch_calls`` / ``rpc_batch_fallback`` are emitted

Hermetic — no network, no disk.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from lite_indexer import metrics
from lite_indexer.rpc import circuit as circuit_mod
from lite_indexer.rpc import client as rc


class _Resp:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.text = json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _Http:
    """Batch-aware transport double.

    ``reject_batches`` makes a list payload return a single error object
    (as some providers do when they do not support arrays), forcing the
    individual-call fallback.
    """

    def __init__(self, reject_batches: bool = False) -> None:
        self.reject_batches = reject_batches
        self.posts: list[Any] = []

    async def post(self, url: str, json: Any = None):  # noqa: A002
        self.posts.append(json)
        if isinstance(json, list):
            if self.reject_batches:
                return _Resp({"jsonrpc": "2.0", "id": None, "error": {"code": -32600, "message": "no batch"}})
            return _Resp(
                [
                    {
                        "jsonrpc": "2.0",
                        "id": item["id"],
                        "result": {"echo": item["params"][0] if item["params"] else None},
                    }
                    for item in json
                ]
            )
        return _Resp({"jsonrpc": "2.0", "id": json.get("id", 1), "result": {"single": json["method"]}})


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch: pytest.MonkeyPatch):
    async def _noop(*a: Any, **k: Any) -> None:
        return None

    async def _circuit_false(endpoint: str) -> bool:
        return False

    monkeypatch.setattr(rc, "_safe_record", lambda *a, **k: None)
    monkeypatch.setattr(rc, "_defer_reason_for", lambda *a, **k: None)
    monkeypatch.setattr(rc, "provider_throttle_before", _noop)
    monkeypatch.setattr(rc, "record_provider_response", lambda *a, **k: None)
    monkeypatch.setattr("lite_indexer.rpc.throttle.throttle_before", _noop)
    monkeypatch.setattr("lite_indexer.rpc.throttle.record_rpc_response", lambda *a, **k: None)
    monkeypatch.setattr(circuit_mod, "circuit_open", _circuit_false)
    metrics.reset()
    yield
    metrics.reset()


def _wire(monkeypatch: pytest.MonkeyPatch, http: _Http) -> None:
    async def _get(timeout: float) -> _Http:
        return http

    monkeypatch.setattr(rc, "_get_call_client", _get)


@pytest.mark.asyncio
async def test_batch_collapses_to_one_request(monkeypatch: pytest.MonkeyPatch):
    http = _Http()
    _wire(monkeypatch, http)
    calls = [("eth_getBlockByNumber", [hex(i), False]) for i in range(5)]
    results = await rc.RpcClient().call_batch("https://batch.example", calls)

    assert len(http.posts) == 1
    assert isinstance(http.posts[0], list) and len(http.posts[0]) == 5
    assert [r["result"]["echo"] for r in results if r is not None] == [hex(i) for i in range(5)]
    assert metrics.get("rpc_batch_calls") == 1.0
    assert metrics.get("rpc_batch_fallback") == 0.0


@pytest.mark.asyncio
async def test_per_element_error_isolation(monkeypatch: pytest.MonkeyPatch):
    class _PartialHttp(_Http):
        async def post(self, url: str, json: Any = None):  # noqa: A002
            self.posts.append(json)
            return _Resp(
                [
                    {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
                    {"jsonrpc": "2.0", "id": 2, "error": {"code": -32000, "message": "boom"}},
                    {"jsonrpc": "2.0", "id": 3, "result": "0x3"},
                ]
            )

    http = _PartialHttp()
    _wire(monkeypatch, http)
    results = await rc.RpcClient().call_batch(
        "https://batch.example",
        [("eth_blockNumber", None), ("eth_call", [{"to": "0x1"}, "latest"]), ("eth_chainId", None)],
    )
    assert results[0] == {"jsonrpc": "2.0", "id": 1, "result": "0x1"}
    assert results[1] is not None and "error" in results[1]
    assert results[2] == {"jsonrpc": "2.0", "id": 3, "result": "0x3"}


@pytest.mark.asyncio
async def test_batch_failure_falls_back_to_individual(monkeypatch: pytest.MonkeyPatch):
    http = _Http(reject_batches=True)
    _wire(monkeypatch, http)

    individual: list[str] = []

    async def _fake_call(
        self: rc.RpcClient, endpoint: str, method: str, params: list[Any] | None = None, timeout: float = 0.0
    ) -> dict[str, Any]:
        individual.append(method)
        return {"result": {"single": method}}

    monkeypatch.setattr(rc.RpcClient, "call", _fake_call)
    results = await rc.RpcClient().call_batch(
        "https://batch.example",
        [("eth_blockNumber", None), ("eth_chainId", None), ("eth_gasPrice", None)],
    )
    assert individual == ["eth_blockNumber", "eth_chainId", "eth_gasPrice"]
    assert all(r is not None and "result" in r for r in results)
    assert metrics.get("rpc_batch_fallback") == 1.0


@pytest.mark.asyncio
async def test_batch_max_chunks_requests(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rc, "RPC_BATCH_MAX", 2)
    http = _Http()
    _wire(monkeypatch, http)
    calls = [("eth_blockNumber", None) for _ in range(5)]
    results = await rc.RpcClient().call_batch("https://batch.example", calls)

    assert [len(p) for p in http.posts] == [2, 2, 1]
    assert len(results) == 5
    assert metrics.get("rpc_batch_calls") == 3.0


@pytest.mark.asyncio
async def test_batch_failure_without_fallback_leaves_none(monkeypatch: pytest.MonkeyPatch):
    """``fallback=False`` must NOT explode a failed batch into per-element calls."""
    http = _Http(reject_batches=True)
    _wire(monkeypatch, http)

    individual: list[str] = []

    async def _fake_call(
        self: rc.RpcClient, endpoint: str, method: str, params: list[Any] | None = None, timeout: float = 0.0
    ) -> dict[str, Any]:
        individual.append(method)
        return {"result": method}

    monkeypatch.setattr(rc.RpcClient, "call", _fake_call)
    results = await rc.RpcClient().call_batch(
        "https://batch.example",
        [("eth_getBlockByNumber", ["0x1", False]), ("eth_getBlockByNumber", ["0x2", False])],
        fallback=False,
    )

    assert results == [None, None]
    assert individual == [], "no individual fallback when fallback=False"
    assert metrics.get("rpc_batch_fallback") == 1.0


@pytest.mark.asyncio
async def test_batch_disabled_uses_individual(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(rc, "RPC_BATCH_ENABLED", False)
    http = _Http()
    _wire(monkeypatch, http)
    individual: list[str] = []

    async def _fake_call(
        self: rc.RpcClient, endpoint: str, method: str, params: list[Any] | None = None, timeout: float = 0.0
    ) -> dict[str, Any]:
        individual.append(method)
        return {"result": method}

    monkeypatch.setattr(rc.RpcClient, "call", _fake_call)
    results = await rc.RpcClient().call_batch(
        "https://batch.example", [("eth_blockNumber", None), ("eth_chainId", None)]
    )
    assert individual == ["eth_blockNumber", "eth_chainId"]
    assert len(results) == 2
    assert http.posts == []
