"""Tests for IndexedXyzSource — the Indexed.xyz R2 lake tier."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.query.indexed_source import ERC20_TRANSFER_TOPIC, IndexedXyzSource

WALLET40 = "ba1333333333a1ba1108e8412f11850a5c319ba9"
TO40 = "4f6f91599858bf0d19fabcf2c5d591fe13f7c059"
COLS = [
    "address",
    "log_index",
    "transaction_hash",
    "topics",
    "data",
    "block_number",
    "block_timestamp",
]


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], cols: list[str]) -> None:
        self._rows = rows
        self.description = [(c,) for c in cols]

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]], cols: list[str]) -> None:
        self.rows = rows
        self.cols = cols
        self.calls: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> FakeCursor:
        self.calls.append((sql, list(params or [])))
        return FakeCursor(self.rows, self.cols)


def _topics(frm: str = WALLET40, to: str = TO40) -> str:
    return f"{ERC20_TRANSFER_TOPIC},0x{'0' * 24}{frm},0x{'0' * 24}{to}"


def _source(rows: list[tuple[Any, ...]]) -> tuple[IndexedXyzSource, FakeConnection]:
    fake = FakeConnection(rows, COLS)
    src = IndexedXyzSource(
        endpoint="https://r2.example", access_key="k", secret_key="s", connect=lambda: fake
    )
    return src, fake


def test_disabled_without_credentials_or_connect() -> None:
    src = IndexedXyzSource(endpoint="", access_key="", secret_key="")
    assert src.available() is False
    assert src.query("evm_transfers", "base", dates=["2026-09-15"]) == []


def test_query_requires_dates() -> None:
    src, fake = _source([])
    assert src.query("evm_transfers", "base") == []
    assert fake.calls == []


def test_row_mapping_topics_and_value() -> None:
    row = (
        "0xTOKEN",
        845,
        "0xHASH",
        _topics(),
        "0x029c3f",
        51000000,
        1789430401,
    )
    src, _ = _source([row])
    rows = src.query("evm_transfers", "base", dates=["2026-09-15"])
    assert len(rows) == 1
    r = rows[0]
    assert r["chain"] == "base"
    assert r["from_address"] == "0x" + WALLET40
    assert r["to_address"] == "0x" + TO40
    assert r["token_address"] == "0xtoken"
    assert r["value"] == str(int("0x029c3f", 16))
    assert r["log_index"] == 845
    assert r["block_timestamp"] is not None and r["block_timestamp"].year == 2026


def test_address_filter_params_and_sql() -> None:
    src, fake = _source([])
    src.query(
        "evm_transfers",
        "base",
        from_block=100,
        to_block=200,
        address="0x" + WALLET40,
        dates=["2026-09-15"],
        limit=5,
    )
    sql, params = fake.calls[-1]
    assert "split_part(topics" in sql
    assert "LIKE '0xddf252ad" in sql
    assert params == [100, 200, WALLET40, WALLET40, 5]


def test_chain_and_table_gating() -> None:
    src, fake = _source([])
    assert src.query("dex_events", "base", dates=["2026-09-15"]) == []
    assert src.query("evm_transfers", "solana", dates=["2026-09-15"]) == []
    assert fake.calls == []


def test_invalid_date_rejected() -> None:
    src, _ = _source([])
    with pytest.raises(ValueError):
        src.query("evm_transfers", "base", dates=["2026-09-15; DROP TABLE"])
