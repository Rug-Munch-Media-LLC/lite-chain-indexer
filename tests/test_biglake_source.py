"""Tests for BigLakeSource — the AWS public lake read tier."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.lakes.registry import LakeProvider
from lite_indexer.lakes.router import LakeProviderRouter
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


def _rows(src: BigLakeSource, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    """``query`` now returns ``(rows, truncated)``; most assertions want rows."""
    rows, _truncated = src.query(*args, **kwargs)
    return rows


def test_disabled_source_returns_empty() -> None:
    src, fake = _source([], TRANSFER_COLS)
    src = BigLakeSource(enabled=False, connect=lambda: fake)
    assert src.available() is False
    assert _rows(src, "evm_transfers") == []
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
    out = _rows(src, "evm_transfers", dates=["2026-09-12"])
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
    out = _rows(src, "internal_transfers", dates=["2026-09-12"])
    r = out[0]
    assert r["trace_address"] == "0.1"
    assert r["value_wei"] == "12345"
    assert r["call_type"] == "call"
    assert r["gas_used"] == 21000
    assert r["error"] == ""
    assert r["input_method_id"] == "0x12345678"


def test_address_filter_is_case_insensitive_or() -> None:
    src, fake = _source([], TRANSFER_COLS)
    _rows(src, "evm_transfers", address="0xAbC", dates=["2026-09-12"])
    sql, params = fake.calls[-1]
    assert "from_address" in sql and "OR" in sql
    # AWS stores addresses 32-byte left-padded; the filter compares the
    # trailing 20 bytes and passes the un-prefixed 40-hex form.
    assert "right(from_address, 40)" in sql
    assert "abc" in params


def test_block_range_params() -> None:
    src, fake = _source([], TRANSFER_COLS)
    _rows(src, "evm_transfers", from_block=100, to_block=200, dates=["2026-09-12"])
    sql, params = fake.calls[-1]
    assert "block_number >= ?" in sql and "block_number <= ?" in sql
    assert params[:2] == [100, 200]


def test_unsupported_table_and_chain_return_empty() -> None:
    src, fake = _source([], TRANSFER_COLS)
    assert _rows(src, "dex_events") == []
    assert _rows(src, "evm_transfers", chain="base") == []
    assert fake.calls == []


def test_invalid_date_rejected() -> None:
    src, _ = _source([], TRANSFER_COLS)
    with pytest.raises(ValueError):
        _rows(src, "evm_transfers", dates=["2026-09-12; DROP TABLE x"])


def test_num_str_edge_cases() -> None:
    assert _num_str(None) == "0"
    assert _num_str(61.0) == "61"
    assert _num_str(61.5) == "61.5"
    assert _num_str("abc") == "abc"


# ── Lake provider rotation ───────────────────────────────────────────


class RecordingRouter(LakeProviderRouter):
    """Router that records selections and reports for assertions."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.selected: list[str] = []
        self.successes: list[str] = []
        self.failures: list[str] = []

    def select(self, chain: str, table: str, *, exclude: frozenset[str] | set[str] = frozenset()) -> Any:
        prov = super().select(chain, table, exclude=exclude)
        if prov is not None:
            self.selected.append(prov.name)
        return prov

    def report_success(self, provider: str, *, bytes_read: int = 0) -> None:
        self.successes.append(provider)
        super().report_success(provider, bytes_read=bytes_read)

    def report_failure(self, provider: str, *, error: str) -> None:
        self.failures.append(provider)
        super().report_failure(provider, error=error)


def _two_providers() -> dict[str, LakeProvider]:
    serves = frozenset({("eth", "token_transfers")})
    return {
        "p1": LakeProvider(name="p1", kind="s3/anonymous", serves=serves),
        "p2": LakeProvider(name="p2", kind="s3/anonymous", serves=serves),
    }


class FlakyConnection:
    """Fails the first execute, then serves one transfer row."""

    def __init__(self) -> None:
        self.n = 0

    def execute(self, sql: str, params: list[Any] | None = None) -> Any:
        self.n += 1
        if self.n == 1:
            raise RuntimeError("boom")
        return FakeCursor(
            [("0xTOKEN", "0xAAA", "0xBBB", 1.0, "0xHASH", 1, 100, "t")],
            TRANSFER_COLS,
        )


def test_rotation_falls_through_on_failure(tmp_path: Any) -> None:
    router = RecordingRouter(registry=_two_providers(), usage_path=tmp_path / "u.json")
    conn = FlakyConnection()
    src = BigLakeSource(connect=lambda: conn, router=router)
    out = _rows(src, "evm_transfers", dates=["2026-09-12"])
    assert router.selected == ["p1", "p2"]
    assert router.failures == ["p1"]
    assert router.successes == ["p2"]
    assert len(out) == 1
    assert out[0]["tx_hash"] == "0xHASH"


def test_quota_refused_provider_skipped(tmp_path: Any) -> None:
    p1 = LakeProvider(name="p1", kind="s3/anonymous", serves=frozenset({("eth", "token_transfers")}), daily_requests=0)
    router = RecordingRouter(registry={"p1": p1}, usage_path=tmp_path / "u.json")
    conn = FlakyConnection()
    src = BigLakeSource(connect=lambda: conn, router=router)
    assert _rows(src, "evm_transfers", dates=["2026-09-12"]) == []
    assert router.selected == []
    assert conn.n == 0  # never touched the connection


def test_cooldown_provider_skipped(tmp_path: Any) -> None:
    p1 = LakeProvider(name="p1", kind="s3/anonymous", serves=frozenset({("eth", "token_transfers")}))
    router = RecordingRouter(registry={"p1": p1}, usage_path=tmp_path / "u.json")
    router.report_failure("p1", error="warmup")  # puts p1 into cooldown
    conn = FlakyConnection()
    src = BigLakeSource(connect=lambda: conn, router=router)
    assert _rows(src, "evm_transfers", dates=["2026-09-12"]) == []
    assert router.selected == []
    assert conn.n == 0


def test_success_reporting_updates_usage(tmp_path: Any) -> None:
    p1 = LakeProvider(name="p1", kind="s3/anonymous", serves=frozenset({("eth", "token_transfers")}))
    router = RecordingRouter(registry={"p1": p1}, usage_path=tmp_path / "u.json")
    row = ("0xTOKEN", "0xAAA", "0xBBB", 1.0, "0xHASH", 1, 100, "t")
    src = BigLakeSource(connect=lambda: FakeConnection([row], TRANSFER_COLS), router=router)
    out = _rows(src, "evm_transfers", dates=["2026-09-12"])
    assert len(out) == 1
    assert router.successes == ["p1"]
    assert router.failures == []
    assert router._state["p1"].usage["requests"] == 1


def test_empty_read_falls_through_then_exhausts(tmp_path: Any) -> None:
    router = RecordingRouter(registry=_two_providers(), usage_path=tmp_path / "u.json")
    src = BigLakeSource(connect=lambda: FakeConnection([], TRANSFER_COLS), router=router)
    assert _rows(src, "evm_transfers", dates=["2026-09-12"]) == []
    assert router.selected == ["p1", "p2"]
    assert router.successes == ["p1", "p2"]  # empty reads are successes, not failures
