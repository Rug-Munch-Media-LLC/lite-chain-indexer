"""Tests for BigLakeSource — the AWS public lake read tier."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.query.biglake_source import BigLakeSource, _num_str


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


TRANSFER_COLS = [
    "token_address",
    "from_address",
    "to_address",
    "value",
    "transaction_hash",
    "log_index",
    "block_number",
    "block_timestamp",
]

TRACE_COLS = [
    "block_number",
    "transaction_hash",
    "trace_address",
    "from_address",
    "to_address",
    "value",
    "call_type",
    "gas_used",
    "error",
    "input",
]


def _source(rows: list[tuple[Any, ...]], cols: list[str]) -> tuple[BigLakeSource, FakeConnection]:
    fake = FakeConnection(rows, cols)
    return BigLakeSource(connect=lambda: fake), fake


def test_disabled_source_returns_empty() -> None:
    src, fake = _source([], TRANSFER_COLS)
    src = BigLakeSource(enabled=False, connect=lambda: fake)
    assert src.available() is False
    assert src.query("evm_transfers") == []
    assert fake.calls == []


def test_evm_transfers_row_mapping() -> None:
    row = (
        "0xTOKEN",
        "0xAAA",
        "0xBBB",
        61.0,
        "0xHASH",
        382,
        25963314,
        "2026-09-12T19:05:47",
    )
    src, _ = _source([row], TRANSFER_COLS)
    out = src.query("evm_transfers", dates=["2026-09-12"])
    assert len(out) == 1
    r = out[0]
    assert r["chain"] == "ethereum"
    assert r["tx_hash"] == "0xHASH"
    assert r["from_address"] == "0xaaa"
    assert r["to_address"] == "0xbbb"
    assert r["token_address"] == "0xtoken"
    assert r["value"] == "61"
    assert r["log_index"] == 382
    assert r["block_number"] == 25963314
    assert "ingested_at" in r


def test_traces_row_mapping() -> None:
    row = (
        25963314,
        "0xHASH",
        [0, 1],
        "0xFROM",
        "0xTO",
        12345.0,
        "call",
        21000,
        None,
        "0x12345678deadbeef",
    )
    src, _ = _source([row], TRACE_COLS)
    out = src.query("internal_transfers", dates=["2026-09-12"])
    r = out[0]
    assert r["trace_address"] == "0.1"
    assert r["value_wei"] == "12345"
    assert r["call_type"] == "call"
    assert r["gas_used"] == 21000
    assert r["error"] == ""
    assert r["input_method_id"] == "0x12345678"


def test_address_filter_is_case_insensitive_or() -> None:
    src, fake = _source([], TRANSFER_COLS)
    src.query("evm_transfers", address="0xAbC", dates=["2026-09-12"])
    sql, params = fake.calls[-1]
    assert "from_address" in sql and "OR" in sql
    assert "0xabc" in params


def test_block_range_params() -> None:
    src, fake = _source([], TRANSFER_COLS)
    src.query("evm_transfers", from_block=100, to_block=200, dates=["2026-09-12"])
    sql, params = fake.calls[-1]
    assert "block_number >= ?" in sql and "block_number <= ?" in sql
    assert params[:2] == [100, 200]


def test_unsupported_table_and_chain_return_empty() -> None:
    src, fake = _source([], TRANSFER_COLS)
    assert src.query("dex_events") == []
    assert src.query("evm_transfers", chain="base") == []
    assert fake.calls == []


def test_invalid_date_rejected() -> None:
    src, _ = _source([], TRANSFER_COLS)
    with pytest.raises(ValueError):
        src.query("evm_transfers", dates=["2026-09-12; DROP TABLE x"])


def test_num_str_edge_cases() -> None:
    assert _num_str(None) == "0"
    assert _num_str(61.0) == "61"
    assert _num_str(61.5) == "61.5"
    assert _num_str("abc") == "abc"
