"""Tests for SQDSource — SQD Portal stream client (Solana/Tron/etc)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

OWNER = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"


class FakePortal:
    """Scripted transport: sequence of (status, text) responses."""

    def __init__(self, script: list[tuple[int, str]]) -> None:
        self.script = list(script)
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self, method: str, url: str, payload: dict[str, Any] | None, timeout: float
    ) -> tuple[int, str]:
        self.calls.append({"method": method, "url": url, "payload": payload})
        if not self.script:
            raise AssertionError("no more scripted responses")
        return self.script.pop(0)


def _jl(*blocks: dict[str, Any]) -> str:
    return "\n".join(json.dumps(b) for b in blocks)


def _block(
    number: int, token_balances: list[dict[str, Any]] | None = None, ts: int = 1788377894
) -> dict[str, Any]:
    return {"header": {"number": number, "timestamp": ts}, "tokenBalances": token_balances or []}


def _source(script: list[tuple[int, str]], **kwargs: Any) -> tuple[SQDSource, FakePortal]:
    fake = FakePortal(script)
    return SQDSource(min_interval=0, retry_backoffs=(0.0,), transport=fake, **kwargs), fake


def test_available_with_transport() -> None:
    src, _ = _source([])
    assert src.available() is True


def test_head_parses_number() -> None:
    src, fake = _source([(200, '{"dataset":"solana-mainnet","number":443778647}')])
    assert src.head("solana") == 443778647
    assert "/datasets/solana-mainnet/head" in fake.calls[0]["url"]


def test_stream_paginates_until_to_block() -> None:
    src, fake = _source(
        [
            (200, _jl(_block(100), _block(101))),
            (200, _jl(_block(102), _block(103))),
        ]
    )
    blocks = src.stream("solana", 100, 103, fields={"block": {"number": True}})
    assert [b["header"]["number"] for b in blocks] == [100, 101, 102, 103]
    assert len(fake.calls) == 2
    assert fake.calls[1]["payload"]["fromBlock"] == 102
    assert fake.calls[0]["payload"]["type"] == "solana"


def test_stream_stops_on_204() -> None:
    src, fake = _source([(204, "")])
    assert src.stream("hyperliquid", 750000000, None, fields={"block": {"number": True}}) == []
    assert fake.calls[0]["payload"]["type"] == "hyperliquidFills"


def test_backoff_retries_overload_then_succeeds() -> None:
    src, fake = _source([(529, "overloaded"), (200, _jl(_block(5)))])
    blocks = src.stream("ethereum", 5, 5, fields={"block": {"number": True}})
    assert len(blocks) == 1
    assert len(fake.calls) == 2


def test_retry_exhausted_raises() -> None:
    src, _ = _source([(529, "overloaded"), (529, "overloaded")])
    with pytest.raises(SQDPortalError):
        src.stream("ethereum", 5, 5, fields={"block": {"number": True}})


def test_unknown_dataset_raises() -> None:
    src, _ = _source([])
    with pytest.raises(ValueError):
        src.stream("no-such-chain", 1, 2, fields={})


def test_owner_activity_maps_instructions() -> None:
    match = {"transactionIndex": 361, "programId": "ProgA", "accounts": ["other", OWNER], "data": "0xdead"}
    skip = {"transactionIndex": 9, "programId": "ProgB", "accounts": ["nobody"], "data": "0x"}
    src, fake = _source(
        [(200, _jl({"header": {"number": 100, "timestamp": 1788377894}, "instructions": [match, skip]}))]
    )
    rows = src.solana_owner_activity(OWNER, 100, 100, limit=10)
    assert len(rows) == 1
    r = rows[0]
    assert r["chain"] == "solana"
    assert r["program_id"] == "ProgA"
    assert r["transaction_index"] == 361
    assert r["account_count"] == 2
    assert fake.calls[0]["payload"]["instructions"][0] == {"mentionsAccount": [OWNER]}


def test_token_balances_for_accounts() -> None:
    rec = {
        "transactionIndex": 3,
        "account": "AcctA",
        "preMint": "MintX",
        "postMint": "MintX",
        "preAmount": "1000",
        "postAmount": "600",
        "preOwner": "P",
        "postOwner": "P",
    }
    dup = dict(rec)
    src, fake = _source(
        [(200, _jl({"header": {"number": 100, "timestamp": 1788377894}, "tokenBalances": [rec, dup]}))]
    )
    rows = src.solana_token_balances_for_accounts(["AcctA"], 100, 100)
    assert len(rows) == 1  # deduped
    assert rows[0]["delta"] == "-400"
    assert rows[0]["mint"] == "MintX"
    assert rows[0]["pre_amount"] == "1000"
    assert fake.calls[0]["payload"]["tokenBalances"] == [{"account": ["AcctA"]}]
    assert src.solana_token_balances_for_accounts([], 100, 100) == []


TRON_WALLET_TOPIC = "000000000000000000000000c686c48436aec3a1dfce4ac5a4526c39366985ba"


def test_tron_address_normalization_and_base58() -> None:
    assert SQDSource._tron_hex40("0xC686C48436AEC3A1DFCE4AC5A4526C39366985BA") == "c686c48436aec3a1dfce4ac5a4526c39366985ba"
    assert SQDSource._tron_hex40("41c686c48436aec3a1dfce4ac5a4526c39366985ba") == "c686c48436aec3a1dfce4ac5a4526c39366985ba"
    assert SQDSource._tron_topic("c686c48436aec3a1dfce4ac5a4526c39366985ba") == TRON_WALLET_TOPIC
    assert SQDSource._tron_base58("a614f803b6fd780986a42c78ec9c7f77e6ded13c") == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    with pytest.raises(ValueError):
        SQDSource._tron_hex40("not-an-address")


def test_tron_trc20_transfers_mapping() -> None:
    log = {
        "transactionIndex": 9,
        "address": "a614f803b6fd780986a42c78ec9c7f77e6ded13c",
        "data": "000000000000000000000000000000000000000000000000000000028fa6ae00",
        "topics": [
            "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            TRON_WALLET_TOPIC,
            "0000000000000000000000009353bbb1f69098da4a72457711e356585946574f",
        ],
    }
    block = {"header": {"number": 86278000, "timestamp": 1789506147000}, "logs": [log]}
    src, fake = _source([(200, _jl(block)), (200, _jl(block))])
    rows = src.tron_trc20_transfers("c686c48436aec3a1dfce4ac5a4526c39366985ba", 86278000, 86278000)
    assert len(rows) == 1
    r = rows[0]
    assert r["chain"] == "tron"
    assert r["token_address"] == "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    assert r["from_address"] == "0xc686c48436aec3a1dfce4ac5a4526c39366985ba"
    assert r["value"] == str(int("28fa6ae00", 16))
    assert r["block_timestamp"] is not None and r["block_timestamp"].year == 2026
    assert fake.calls[0]["payload"]["logs"][0]["topic1"] == [TRON_WALLET_TOPIC]
    assert fake.calls[1]["payload"]["logs"][0]["topic2"] == [TRON_WALLET_TOPIC]


def test_tron_internal_transfers_mapping() -> None:
    rec = {
        "transactionIndex": 423,
        "internalTransactionIndex": 0,
        "callerAddress": "41a31d689a84244bc01be56e07aeafb7686f56bb89",
        "transferToAddress": "41b50b027637ab9a7f2ba5b70a24f778f8438bb279",
        "callValueInfo": [{"callValue": 1000000, "tokenId": None}],
        "rejected": None,
    }
    block = {"header": {"number": 86278002, "timestamp": 1789506153000}, "internalTransactions": [rec]}
    src, fake = _source([(200, _jl(block)), (200, _jl(block))])
    rows = src.tron_internal_transfers("0xa31d689a84244bc01be56e07aeafb7686f56bb89", 86278002, 86278002)
    assert len(rows) == 1
    r = rows[0]
    assert r["trace_address"] == "423.0"
    assert r["value_wei"] == "1000000"
    assert r["from_address"] == "0xa31d689a84244bc01be56e07aeafb7686f56bb89"
    assert fake.calls[0]["payload"]["internalTransactions"][0]["transferTo"] == ["41a31d689a84244bc01be56e07aeafb7686f56bb89"]


def test_hyperliquid_owner_fills_mapping() -> None:
    fill = {"user": "0xAbC", "coin": "BTC", "px": "67234.5", "sz": "0.1", "side": "B", "dir": "Open Long", "fee": "6.72"}
    block = {"header": {"number": 900000050, "timestamp": 1710000000000}, "fills": [fill, {**fill, "user": "0xother"}]}
    src, fake = _source([(200, _jl(block))])
    rows = src.hyperliquid_owner_fills("0xabc", 900000050, 900000050)
    assert len(rows) == 1
    assert rows[0]["coin"] == "BTC"
    assert rows[0]["direction"] == "Open Long"
    assert fake.calls[0]["payload"]["fills"][0] == {"user": ["0xabc"]}
