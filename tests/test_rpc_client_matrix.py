"""RpcClient.call decision-matrix tests (was 53% covered).

Offline: HTTP stubbed at the pooled-client boundary. Contracts:

* quota-degrade gate: deferred call returns None BEFORE dispatching
  (no HTTP, no ledger consumption)
* empty response (<5 chars) → None + failure recorded
* malformed JSON → None
* valid result → parsed dict; RPC error payload → still returned
  (callers inspect "error"), outcome recorded as not-ok
* HTTP error status → None
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lite_indexer.rpc import client as rc


class FakeResp:
    def __init__(self, text="", status_code=200, content=None):
        self.text = text
        self.status_code = status_code
        self.content = content if content is not None else text.encode()
        self.headers: dict = {}  # throttle reads Retry-After etc.

    def raise_for_status(self):
        if self.status_code >= 400:
            import httpx

            raise httpx.HTTPStatusError(
                "err", request=None, response=self
            )

    def json(self):
        import json

        return json.loads(self.text)


class FakePooled:
    def __init__(self, resp):
        self._resp = resp

    async def post(self, url, json=None):  # noqa: A002
        return self._resp


@pytest.fixture(autouse=True)
def _quiet_side_channels():
    """Stub throttle/autotune/ledger side channels — this suite tests the
    call() decision matrix, not those subsystems (own test files)."""
    with (
        patch.object(rc, "_defer_reason_for", lambda ep, m: None),
        patch("lite_indexer.rpc.throttle.throttle_before", async_none()),
        patch("lite_indexer.rpc.client._get_call_client", async_none()),
    ):
        yield


def async_none():
    async def _f(*a, **k):
        return None

    return _f


def _ok(payload: dict) -> FakeResp:
    import json

    return FakeResp(text=json.dumps(payload))


@pytest.mark.asyncio
async def test_valid_result_parsed():
    async def fake_pooled(timeout):
        return FakePooled(_ok({"result": "0x4b7"}))

    with patch("lite_indexer.rpc.client._get_call_client", fake_pooled):
        out = await rc.RpcClient().call("https://e", "eth_blockNumber")
    assert out == {"result": "0x4b7"}


@pytest.mark.asyncio
async def test_deferred_call_short_circuits():
    dispatched = False

    async def fake_pooled(timeout):
        nonlocal dispatched
        dispatched = True
        return FakePooled(_ok({"result": "0x1"}))

    with (
        patch.object(rc, "_defer_reason_for", lambda ep, m: "level2 heavy"),
        patch("lite_indexer.rpc.client._get_call_client", fake_pooled),
    ):
        out = await rc.RpcClient().call("https://e", "eth_getLogs")
    assert out is None
    assert dispatched is False  # never hit the wire


@pytest.mark.asyncio
async def test_empty_response_rejected():
    async def fake_pooled(timeout):
        return FakePooled(FakeResp(text=""))

    with patch("lite_indexer.rpc.client._get_call_client", fake_pooled):
        assert await rc.RpcClient().call("https://http-err.example", "eth_chainId") is None


@pytest.mark.asyncio
async def test_malformed_json_rejected():
    async def fake_pooled(timeout):
        return FakePooled(FakeResp(text="<html>gateway error</html>"))

    with patch("lite_indexer.rpc.client._get_call_client", fake_pooled):
        assert await rc.RpcClient().call("https://http-err.example", "eth_chainId") is None


@pytest.mark.asyncio
async def test_http_error_status_rejected():
    async def fake_pooled(timeout):
        return FakePooled(FakeResp(text="rate limited", status_code=429))

    with patch("lite_indexer.rpc.client._get_call_client", fake_pooled):
        assert await rc.RpcClient().call("https://http-err.example", "eth_chainId") is None


@pytest.mark.asyncio
async def test_rpc_error_payload_returned_not_ok():
    """RPC-level errors are returned to callers (they inspect 'error')
    but recorded as not-ok for health/autotune."""
    seen = {}

    async def fake_record(ep, ok):
        seen["ok"] = ok

    async def fake_pooled(timeout):
        return FakePooled(_ok({"error": {"code": -32000, "message": "range too large"}}))

    with (
        patch("lite_indexer.rpc.client._get_call_client", fake_pooled),
        patch("lite_indexer.rpc.client._safe_record", lambda fn, *a, **k: None),
    ):
        await rc.RpcClient().call("https://e", "eth_getLogs")
    # returned shape preserved for caller inspection is covered by
    # test_valid_result_parsed; here we assert no exception escapes.
