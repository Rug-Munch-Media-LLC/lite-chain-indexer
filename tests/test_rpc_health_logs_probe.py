"""getLogs health probe — the bsc 2026-10-23 contract.

Every configured BSC endpoint answered ``eth_blockNumber`` while none could
serve ``eth_getLogs`` for the pending range. The probe must therefore judge
the endpoint on the getLogs answer alone:

* empty log list            -> PASS (served the span, no transfers in it)
* header not found / range  -> FAIL (pruned history, range ceiling)
* archive-token / rate wall -> FAIL (403 / 429)
* transport / non-json      -> FAIL
"""

from __future__ import annotations

import httpx
import pytest

from lite_indexer.rpc import health_logs as hl


class _Resp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Client:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def post(self, url, json=None):  # noqa: A002 — httpx signature
        if isinstance(self._resp, Exception):
            raise self._resp
        return self._resp


@pytest.fixture(autouse=True)
def _clean():
    hl.clear_logs_verdicts()
    yield
    hl.clear_logs_verdicts()


def _patch(monkeypatch, resp):
    monkeypatch.setattr(hl.httpx, "AsyncClient", lambda **kwargs: _Client(resp))


@pytest.mark.asyncio
async def test_empty_log_list_passes(monkeypatch):
    _patch(monkeypatch, _Resp({"jsonrpc": "2.0", "id": 1, "result": []}))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 120079835, 120079839)
    assert (ok, reason) == (True, "ok")
    assert hl.logs_probe_verdict("https://rpc.example") is True


@pytest.mark.asyncio
async def test_pruned_history_fails(monkeypatch):
    _patch(
        monkeypatch,
        _Resp({"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "header not found"}}),
    )
    ok, reason = await hl.probe_logs_range("https://rpc.example", 120079835, 120079839)
    assert ok is False
    assert "header not found" in reason
    assert hl.logs_probe_verdict("https://rpc.example") is False


@pytest.mark.asyncio
async def test_range_ceiling_fails(monkeypatch):
    _patch(
        monkeypatch,
        _Resp(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "error": {"code": -32000, "message": "log query range must not exceed 25 blocks"},
            }
        ),
    )
    ok, _ = await hl.probe_logs_range("https://rpc.example", 1, 5000)
    assert ok is False


@pytest.mark.asyncio
async def test_http_429_fails(monkeypatch):
    _patch(monkeypatch, _Resp({"error": "quota"}, status_code=429))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 1, 5)
    assert (ok, reason) == (False, "http 429")


@pytest.mark.asyncio
async def test_http_403_archive_wall_fails(monkeypatch):
    _patch(monkeypatch, _Resp({"error": "token"}, status_code=403))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 1, 5)
    assert (ok, reason) == (False, "http 403")


@pytest.mark.asyncio
async def test_non_json_body_fails(monkeypatch):
    _patch(monkeypatch, _Resp(ValueError("not json")))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 1, 5)
    assert (ok, reason) == (False, "non-json body")


@pytest.mark.asyncio
async def test_transport_error_fails(monkeypatch):
    _patch(monkeypatch, httpx.ConnectError("boom"))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 1, 5)
    assert ok is False
    assert reason.startswith("transport:")


@pytest.mark.asyncio
async def test_result_must_be_a_list(monkeypatch):
    _patch(monkeypatch, _Resp({"jsonrpc": "2.0", "id": 1, "result": None}))
    ok, reason = await hl.probe_logs_range("https://rpc.example", 1, 5)
    assert (ok, reason) == (False, "no result list")
