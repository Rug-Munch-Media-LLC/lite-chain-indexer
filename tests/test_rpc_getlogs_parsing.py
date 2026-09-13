"""Tests for RpcClient.get_logs result-parsing contracts.

Scope: the response-shape handling of eth_getLogs — the branch that decides
whether a sync cycle sees logs, an empty range, or a failure. Network,
throttling, and telemetry are mocked; only parsing semantics are exercised.

Contracts probed against actual behavior first (fleet doctrine):
- ``result: [...]``  -> list of logs
- ``result: null``   -> []   (endpoint healthy, range empty — NOT a failure)
- malformed shapes   -> None (failure)
NOTE: the client AWAITS client.post(...) — fakes must be async.
"""

from __future__ import annotations

from typing import Any

import pytest


class _FakeResp:
    def __init__(self, payload: Any, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.last_json: dict[str, Any] | None = None

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected status {self.status_code}")


class _FakeHttpClient:
    """Async post — mirrors httpx.AsyncClient surface the client awaits."""

    def __init__(self, resp: _FakeResp):
        self.resp = resp
        self.captured: dict[str, Any] = {}

    async def post(self, url: str, **kw):
        self.captured = kw
        return self.resp


@pytest.fixture()
def client():
    from lite_indexer.rpc.client import RpcClient

    return RpcClient()


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch):
    """Neutralize rate-limiting/telemetry so tests exercise parsing only."""
    import lite_indexer.rpc.client as rc

    async def _noop(*a, **k):
        return None

    monkeypatch.setattr(rc, "apply_provider_rate_limit", _noop)
    monkeypatch.setattr(rc, "provider_throttle_before", _noop)
    monkeypatch.setattr(rc, "record_provider_response", lambda *a, **k: None)
    monkeypatch.setattr("lite_indexer.rpc.throttle.throttle_before", _noop)


async def _run(client, payload: Any) -> Any:
    from unittest.mock import patch

    import lite_indexer.rpc.client as rc

    http = _FakeHttpClient(_FakeResp(payload))

    async def _fc():
        return http

    async def _fc_timeout(timeout: float):
        return http

    client._get_client = _fc  # noqa: SLF001 — test seam
    # Batch (and its transparent individual fallback) also route through the
    # module-level pooled accessor — keep it offline too.
    with patch.object(rc, "_get_call_client", _fc_timeout):
        result = await client.get_logs(
            endpoint="https://endpoint",
            from_block=100,
            to_block=200,
            topic="0x" + "ab" * 32,
        )
    return result, http


@pytest.mark.asyncio
async def test_logs_list_returned_verbatim(client):
    logs = [{"address": "0xtok", "data": "0x", "topics": ["0xab"]}]
    result, _ = await _run(client, {"result": logs})
    assert result == logs


@pytest.mark.asyncio
async def test_null_result_means_empty_range_not_failure(client):
    # JSON-RPC null result = endpoint healthy, zero logs in range. The
    # fetch loop depends on this being [] (skip gracefully), never None.
    result, _ = await _run(client, {"result": None})
    assert result == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad",
    [
        {"error": {"code": -32000, "message": "query returned more than 10000 results"}},
        {"result": {"not": "a list"}},
        {"result": "0xdeadbeef"},
        {},
        None,
    ],
)
async def test_malformed_shapes_fail_closed_to_none(client, bad):
    result, _ = await _run(client, bad)
    assert result is None


@pytest.mark.asyncio
async def test_filter_params_include_address_and_hex_blocks(client):
    http = _FakeHttpClient(_FakeResp({"result": []}))

    async def _fc():
        return http

    client._get_client = _fc  # noqa: SLF001 — test seam
    await client.get_logs(
        endpoint="https://e",
        from_block=1,
        to_block=2,
        topic="0x" + "cd" * 32,
        address="0xcontract",
    )
    params = http.captured["json"]["params"][0]
    assert params["address"] == "0xcontract"
    assert params["topics"] == ["0x" + "cd" * 32]
    assert params["fromBlock"] == hex(1)
    assert params["toBlock"] == hex(2)
