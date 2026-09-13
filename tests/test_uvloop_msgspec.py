"""Tests for the optional perf hooks: uvloop install + msgspec JSON fast path.

Contracts:

* uvloop install is graceful: missing/raising uvloop → False, no exception.
* uvloop present → the EventLoopPolicy is installed (and never leaves the
  process on a broken policy in tests — injected).
* msgspec (when available) decodes byte-identically to stdlib json on
  representative RPC payloads, including error dicts, null results, 1k-log
  responses, and malformed/non-UTF8/empty failures (same ValueError).
* decode_response prefers bytes, falls back to ``resp.json()`` for doubles
  that omit ``content``.

Hermetic — no network, no disk.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from typing import Any

import pytest

from lite_indexer import runtime
from lite_indexer.rpc import jsonparse


def _logs_payload(n: int = 1000) -> bytes:
    logs = [
        {
            "address": "0x" + "11" * 20,
            "topics": ["0x" + "ab" * 32, "0x" + "00" * 32, "0x" + "00" * 32],
            "data": "0x" + "00" * 32,
            "blockNumber": hex(18_000_000 + i),
            "transactionHash": "0x" + f"{i:064x}",
            "transactionIndex": hex(i % 200),
            "blockHash": "0x" + "22" * 32,
            "logIndex": hex(i),
            "removed": False,
        }
        for i in range(n)
    ]
    return json.dumps({"jsonrpc": "2.0", "id": 1, "result": logs}).encode()


class TestUvloopFallback:
    def test_missing_uvloop_falls_back(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setitem(sys.modules, "uvloop", None)  # import raises ImportError
        assert runtime.install_event_loop_policy() is False

    def test_present_uvloop_installs_policy(self, monkeypatch: pytest.MonkeyPatch):
        class _Policy:
            pass

        fake = types.SimpleNamespace(EventLoopPolicy=_Policy)
        monkeypatch.setitem(sys.modules, "uvloop", fake)
        captured: dict[str, Any] = {}
        monkeypatch.setattr(asyncio, "set_event_loop_policy", lambda p: captured.update(policy=p))
        assert runtime.install_event_loop_policy() is True
        assert isinstance(captured["policy"], _Policy)

    def test_uvloop_install_error_is_graceful(self, monkeypatch: pytest.MonkeyPatch):
        class _Boom:
            def __init__(self) -> None:
                raise RuntimeError("no loop for you")

        fake = types.SimpleNamespace(EventLoopPolicy=_Boom)
        monkeypatch.setitem(sys.modules, "uvloop", fake)
        assert runtime.install_event_loop_policy() is False


class TestMsgspecParity:
    @pytest.mark.parametrize(
        "payload",
        [
            _logs_payload(3),
            _logs_payload(1000),
            b'{"jsonrpc":"2.0","id":1,"result":null}',
            b'{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"range too large"}}',
            b"{}",
            b"[]",
            b"null",
        ],
    )
    def test_decode_matches_stdlib(self, payload: bytes):
        assert jsonparse.decode_bytes(payload) == json.loads(payload)

    @pytest.mark.parametrize("bad", [b"{not json", b"\xff\xfe", b"", b'{"a": }'])
    def test_malformed_raises_like_stdlib(self, bad: bytes):
        with pytest.raises(ValueError):
            jsonparse.decode_bytes(bad)
        with pytest.raises(ValueError):
            json.loads(bad)

    def test_available_flag_matches_import(self):
        assert jsonparse.msgspec_available() is (jsonparse._decoder is not None)  # noqa: SLF001


class _FakeResp:
    def __init__(self, payload: bytes | None, parsed: Any = None) -> None:
        if payload is not None:
            self.content = payload
        self._parsed = parsed

    def json(self):
        return self._parsed


class TestDecodeResponse:
    def test_prefers_bytes_content(self):
        payload = _logs_payload(5)
        resp = _FakeResp(payload, parsed={"should": "not be used"})
        assert jsonparse.decode_response(resp) == json.loads(payload)

    def test_falls_back_to_json_method_without_content(self):
        resp = _FakeResp(None, parsed={"result": []})
        assert jsonparse.decode_response(resp) == {"result": []}

    def test_empty_content_falls_back(self):
        resp = _FakeResp(b"", parsed={"result": None})
        assert jsonparse.decode_response(resp) == {"result": None}


class TestDecodeResponseAsync:
    """The off-loop wrappers must be byte-for-byte equivalent to the sync path."""

    def test_async_prefers_bytes_content(self):
        payload = _logs_payload(5)
        resp = _FakeResp(payload, parsed={"should": "not be used"})
        assert asyncio.run(jsonparse.decode_response_async(resp)) == json.loads(payload)

    def test_async_falls_back_to_json_method_without_content(self):
        resp = _FakeResp(None, parsed={"result": []})
        assert asyncio.run(jsonparse.decode_response_async(resp)) == {"result": []}

    def test_async_empty_content_falls_back(self):
        resp = _FakeResp(b"", parsed={"result": None})
        assert asyncio.run(jsonparse.decode_response_async(resp)) == {"result": None}

    def test_async_bytes_matches_sync(self):
        payload = _logs_payload(50)
        assert asyncio.run(jsonparse.decode_bytes_async(payload)) == jsonparse.decode_bytes(payload)

    def test_async_malformed_raises_like_sync(self):
        payload = b"{not json"
        with pytest.raises(ValueError):
            asyncio.run(jsonparse.decode_bytes_async(payload))
        with pytest.raises(ValueError):
            jsonparse.decode_bytes(payload)
