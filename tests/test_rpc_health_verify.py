"""RPC health verification tests — verify_endpoint decision matrix.

Previously 15% covered. Contracts (HTTP stubbed at httpx boundary):

* valid result (list or null) → verified OK
* range/archive/limit errors → ACCEPTED (endpoint works with right ranges)
* other RPC errors / non-dict payloads → rejected
* timeouts and transport errors → rejected
* is_endpoint_healthy reflects last verification; unknown endpoints
  default healthy (fail-open into rotation, verification tightens later)
"""

from __future__ import annotations

import httpx
import pytest

from lite_indexer.rpc import health as hh


@pytest.fixture(autouse=True)
def _clean_state():
    hh._endpoint_verified_ok.clear()
    hh._endpoint_last_verified.clear()
    yield
    hh._clean_state.clear() if hasattr(hh, "_clean_state") else (
        hh._endpoint_verified_ok.clear(),
        hh._endpoint_last_verified.clear(),
    )


class _Resp:
    def __init__(self, payload):
        self._p = payload

    def json(self):
        return self._p


class _Client:
    def __init__(self, payload=None, exc=None):
        self._payload = payload
        self._exc = exc

    async def __aenter__(self):
        return self

    async def __aexit__(self, *excinfo):
        return None

    async def post(self, url, json=None):  # noqa: A002
        if self._exc:
            raise self._exc
        return _Resp(self._payload)


EP = "https://verify.example"


@pytest.mark.asyncio
async def test_valid_list_result_verifies(monkeypatch):
    monkeypatch.setattr(hh.httpx, "AsyncClient", lambda **k: _Client({"result": []}))
    assert await hh.verify_endpoint(EP, "ethereum", latest_block=19_000_000) is True
    assert hh.is_endpoint_healthy(EP) is True


@pytest.mark.asyncio
async def test_null_result_also_verifies(monkeypatch):
    monkeypatch.setattr(hh.httpx, "AsyncClient", lambda **k: _Client({"result": None}))
    assert await hh.verify_endpoint(EP, "base") is True


@pytest.mark.asyncio
async def test_range_limited_errors_accepted(monkeypatch):
    for msg in ("archive data not supported", "range too large", "limit exceeded"):
        monkeypatch.setattr(
            hh.httpx,
            "AsyncClient",
            lambda _msg=msg, **k: _Client({"error": {"message": _msg}}),
        )
        assert await hh.verify_endpoint(EP, "polygon") is True, msg


@pytest.mark.asyncio
async def test_other_rpc_error_rejected(monkeypatch):
    monkeypatch.setattr(
        hh.httpx,
        "AsyncClient",
        lambda **k: _Client({"error": {"message": "method not found"}}),
    )
    assert await hh.verify_endpoint(EP, "bsc") is False
    assert hh.is_endpoint_healthy(EP) is False


@pytest.mark.asyncio
async def test_non_dict_payload_rejected(monkeypatch):
    monkeypatch.setattr(hh.httpx, "AsyncClient", lambda **k: _Client([1, 2, 3]))
    assert await hh.verify_endpoint(EP, "arbitrum") is False


@pytest.mark.asyncio
async def test_timeout_rejected(monkeypatch):
    monkeypatch.setattr(
        hh.httpx,
        "AsyncClient",
        lambda **k: _Client(exc=httpx.TimeoutException("slow")),
    )
    assert await hh.verify_endpoint(EP, "optimism") is False


@pytest.mark.asyncio
async def test_transport_error_rejected(monkeypatch):
    monkeypatch.setattr(
        hh.httpx,
        "AsyncClient",
        lambda **k: _Client(exc=httpx.ConnectError("down")),
    )
    assert await hh.verify_endpoint(EP, "gnosis") is False


def test_unknown_endpoint_defaults_healthy():
    """Fail-open: unverified endpoints enter rotation; verification
    tightens the set over time."""
    assert hh.is_endpoint_healthy("https://never-seen.example") is True
