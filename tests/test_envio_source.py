"""Tests for EnvioSource — HyperRPC wallet transfers + trace_block."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.query.envio_source import (
    TRANSFER_TOPIC,
    EnvioLimitError,
    EnvioSource,
)

ADDR = "0xAbCdEf000000000000000000000000000000CaFe"
PADDED = "0x" + "0" * 24 + "abcdef000000000000000000000000000000cafe"


class FakePost:
    """Scripted transport: list of responses, or a callable(payload) -> response."""

    def __init__(self, responses: Any) -> None:
        self._responses = responses
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
        self.calls.append(payload)
        if callable(self._responses):
            return self._responses(payload)  # type: ignore[no-any-return]
        return self._responses.pop(0)  # type: ignore[no-any-return]


def _rpc_ok(result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "result": result}


def _rpc_err(message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": message}}


def _log(
    block: int, log_index: int, frm: str = "0x" + "1" * 40, to: str = "0x" + "2" * 40
) -> dict[str, Any]:
    return {
        "address": "0xToken",
        "blockNumber": hex(block),
        "transactionHash": "0xHASH",
        "logIndex": hex(log_index),
        "topics": [TRANSFER_TOPIC, "0x" + "0" * 24 + frm[2:], "0x" + "0" * 24 + to[2:]],
        "data": "0x3d",  # 61
    }


def _source(responses: Any) -> tuple[EnvioSource, FakePost]:
    fake = FakePost(responses)
    return EnvioSource(rpc_token="test-token", min_interval=0, post=fake), fake


def test_unavailable_without_token() -> None:
    src = EnvioSource(rpc_token="", min_interval=0, post=FakePost([]))
    assert src.available() is False
    assert src.address_transfers("base", ADDR, 1, 2) == []


def test_padded_topic() -> None:
    assert EnvioSource._padded_topic(ADDR) == PADDED


def test_address_transfers_merge_sort_map() -> None:
    # Query 1 (from-position): one log at block 200; Query 2 (to-position): one at block 300.
    fake_rows = [
        _rpc_ok([_log(200, 1)]),
        _rpc_ok([_log(300, 2)]),
    ]
    src, fake = _source(fake_rows)
    rows = src.address_transfers("base", ADDR, 100, 400, limit=10)
    assert [r["block_number"] for r in rows] == [300, 200]  # newest first
    assert rows[0]["chain"] == "base"
    assert rows[0]["value"] == "61"
    assert rows[0]["from_address"].startswith("0x1111")
    assert rows[0]["token_address"] == "0xtoken"
    # Both queries fired with the padded topic in the right positions.
    t1 = fake.calls[0]["params"][0]["topics"]
    t2 = fake.calls[1]["params"][0]["topics"]
    assert t1 == [TRANSFER_TOPIC, PADDED]
    assert t2 == [TRANSFER_TOPIC, None, PADDED]


def test_get_logs_splits_on_limit() -> None:
    def splitter(payload: dict[str, Any]) -> dict[str, Any]:
        p = payload["params"][0]
        lo = int(p["fromBlock"], 16)
        hi = int(p["toBlock"], 16)
        if hi - lo > 100:
            return _rpc_err("Limit exceeded: More than 50000 logs returned; this")
        return _rpc_ok([_log(lo, 1)])

    src, fake = _source(splitter)
    logs = src.get_logs("base", 0, 400)
    # 0-400 splits into 4 leaves of <= 100 span; each leaf returns one log.
    assert len(logs) == 4
    assert len(fake.calls) == 7  # 3 failures + 4 leaves


def test_unsupported_chain_gates() -> None:
    src, fake = _source([_rpc_ok([])])
    assert src.address_transfers("solana", ADDR, 1, 2) == []
    assert fake.calls == []
    with pytest.raises(ValueError):
        src.call("solana", "eth_blockNumber", [])


def test_trace_block_rows_mapping() -> None:
    raw = [
        {
            "blockNumber": "0x3000000",
            "transactionHash": "0xTX",
            "traceAddress": [0, 1],
            "action": {
                "from": "0xAA",
                "to": "0xBB",
                "value": "123",
                "callType": "call",
                "input": "0xabcdef12dead",
            },
            "result": {"gasUsed": "0x5208"},
        }
    ]
    src, _ = _source([_rpc_ok(raw)])
    rows = src.block_traces_rows("base", 0x3000000)
    assert len(rows) == 1
    r = rows[0]
    assert r["trace_address"] == "0.1"
    assert r["value_wei"] == "123"
    assert r["gas_used"] == 21000
    assert r["input_method_id"] == "0xabcdef12"
    assert r["error"] == ""


def test_limit_error_is_typed() -> None:
    src, _ = _source([_rpc_err("Limit exceeded: too many logs")])
    with pytest.raises(EnvioLimitError):
        src.call("base", "eth_getLogs", [{}])
