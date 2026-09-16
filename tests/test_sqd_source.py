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
