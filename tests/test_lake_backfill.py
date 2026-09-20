"""Tests for the sonarx lake → ClickHouse backfill seam (DI only, no monkeypatch)."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.ingest.lake_backfill import backfill_chain_from_lake, expand_dates
from lite_indexer.ingest.lake_sonarx import map_trace_row, map_transfer_row

# ── Mapping tests (fixture dicts, no network) ─────────────────────────


def test_map_transfer_row() -> None:
    raw = {
        "BLOCK_NUMBER": 51233226,
        "TIMESTAMP": 1789255799,
        "TRANSACTION_HASH": "0xabc",
        "LOG_INDEX": 171,
        "FROM_ADDRESS": "0x0000000000000000000000000000000000000000",
        "TO_ADDRESS": "0x3605B18FEF53A0EB865EB3663E427FB56C4E55F2",
        "TOKEN_ADDRESS": "0xC20EC7C1D13941C55C75933E841CADA9BAAB25BC",
        "VALUE": 31740.0,
    }
    row = map_transfer_row(raw)
    assert row["blockNumber"] == 51233226
    assert row["txHash"] == "0xabc"
    assert row["logIndex"] == 171
    assert row["to"] == "0x3605b18fef53a0eb865eb3663e427fb56c4e55f2"
    assert row["value"] == "31740"
    assert row["blockTimestamp"].startswith("2026-09-12T")
    # All keys the ClickHouse writer requires are present.
    assert set(row) == {"blockNumber", "txHash", "from", "to", "tokenAddress", "value", "blockTimestamp", "logIndex"}


def test_map_transfer_row_nulls() -> None:
    row = map_transfer_row({"BLOCK_NUMBER": None, "VALUE": None, "TIMESTAMP": None})
    assert row["blockNumber"] == 0
    assert row["value"] == "0"
    assert row["blockTimestamp"] == ""


def test_map_trace_row() -> None:
    raw = {
        "BLOCK_NUMBER": 51219453,
        "TIMESTAMP": 1789228253,
        "TRANSACTION_HASH": "0x91f9",
        "PATH": "0,7,0,0,2,0",
        "FROM_ADDRESS": "0x833589FCD6EDB6E08F4C7C32D4F71B54BDA02913",
        "TO_ADDRESS": "0x2CE6311DDAE708829BC0784C967B7D77D19FD779",
        "VALUE": 0.0,
        "TYPE": "delegatecall",
        "GAS_USED": 7463,
        "ERROR": None,
        "INPUT": "0xa9059cbb0000",
    }
    row = map_trace_row(raw)
    assert row["trace_address"] == [0, 7, 0, 0, 2, 0]
    assert row["call_type"] == "delegatecall"
    assert row["value_wei"] == 0
    assert row["gas_used"] == 7463
    assert row["error"] == ""
    assert row["input_method_id"] == "0xa9059cbb"
    assert row["block_timestamp"].startswith("2026-09-12T")


def test_map_trace_row_empty_path() -> None:
    row = map_trace_row({"PATH": None, "VALUE": None})
    assert row["trace_address"] == []
    assert row["value_wei"] == 0


# ── Date expansion ────────────────────────────────────────────────────


def test_expand_dates_inclusive() -> None:
    assert expand_dates("2026-09-08", "2026-09-10") == ["2026-09-08", "2026-09-09", "2026-09-10"]


def test_expand_dates_reversed_empty() -> None:
    assert expand_dates("2026-09-10", "2026-09-08") == []


# ── Fake source / writer (DI, no monkeypatch) ─────────────────────────


class FakeSource:
    """SonarxLakeSource stand-in: yields N rows per (chain, date, table)."""

    def __init__(self, fail_dates: set[str] | None = None) -> None:
        self.fail_dates = fail_dates or set()
        self.reads: list[tuple[str, str, str]] = []

    def available(self) -> bool:
        return True

    def _rows(self, chain: str, d: str, table: str) -> list[dict[str, Any]]:
        self.reads.append((chain, d, table))
        if d in self.fail_dates:
            raise FileNotFoundError(f"no partition: {d}")
        if table == "transfers":
            return [map_transfer_row({"BLOCK_NUMBER": i, "TIMESTAMP": 1789255799, "VALUE": 1.0}) for i in range(3)]
        return [map_trace_row({"BLOCK_NUMBER": i, "TIMESTAMP": 1789228253, "VALUE": 0.0}) for i in range(2)]

    def read_transfers(self, chain: str, date: str) -> Any:
        return iter(self._rows(chain, date, "transfers"))

    def read_traces(self, chain: str, date: str) -> Any:
        return iter(self._rows(chain, date, "traces"))

    def count(self, chain: str, table: str, date: str) -> int:
        self.reads.append((chain, date, table))
        if date in self.fail_dates:
            raise FileNotFoundError(f"no partition: {date}")
        return 3 if table == "transfers" else 2


class FakeClient:
    def __init__(self, writer: FakeWriter) -> None:
        self._writer = writer

    def execute(self, sql: str, rows: list[Any] | None = None) -> Any:
        self._writer.internal.append(("insert", len(rows or [])))
        return None


class FakeWriter:
    def __init__(self) -> None:
        self.transfers: list[tuple[str, list[dict[str, Any]]]] = []
        self.internal: list[tuple[str, int]] = []

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        self.transfers.append((chain, events))
        return len(events)

    def client(self) -> Any:
        return FakeClient(self)


def test_dry_run_counts_without_writing() -> None:
    src, wh = FakeSource(), FakeWriter()
    counts = backfill_chain_from_lake("base", ["2026-09-08"], dry_run=True, source=src, writer=wh)
    assert counts == {"base/2026-09-08/transfers": 3, "base/2026-09-08/traces": 2}
    assert not wh.transfers and not wh.internal


def test_real_run_writes_batches() -> None:
    src, wh = FakeSource(), FakeWriter()
    counts = backfill_chain_from_lake("base", ["2026-09-08"], dry_run=False, batch_rows=2, source=src, writer=wh)
    assert counts == {"base/2026-09-08/transfers": 3, "base/2026-09-08/traces": 2}
    # 3 rows at batch_rows=2 → 2 write calls for transfers.
    assert sum(len(ev) for _, ev in wh.transfers) == 3
    assert len(wh.transfers) == 2
    assert sum(n for _, n in wh.internal) == 2


def test_missing_partition_tolerated() -> None:
    src, wh = FakeSource(fail_dates={"2026-09-09"}), FakeWriter()
    counts = backfill_chain_from_lake("base", ["2026-09-08", "2026-09-09"], dry_run=False, source=src, writer=wh)
    assert counts["base/2026-09-09/transfers"] == 0
    assert counts["base/2026-09-08/transfers"] == 3  # good date unaffected


def test_unsupported_chain_raises() -> None:
    with pytest.raises(ValueError, match="unsupported sonarx chain"):
        backfill_chain_from_lake("ethereum", ["2026-09-08"], source=FakeSource(), writer=FakeWriter())


def test_unavailable_source_returns_empty() -> None:
    class Dead:
        def available(self) -> bool:
            return False

    assert backfill_chain_from_lake("base", ["2026-09-08"], source=Dead(), writer=FakeWriter()) == {}


# ── Network-optional live schema probe (skips cleanly on failure) ─────


def test_sonarx_base_transfers_describe() -> None:
    duckdb = pytest.importorskip("duckdb")
    try:
        con = duckdb.connect()
        con.execute("SET s3_region='us-east-1'")
        path = "s3://aws-public-blockchain/v1.1/sonarx/base/transfers/date=2026-09-12/*.parquet"
        cols = [
            r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        ]  # nosemgrep: formatted-sql-query — constant path
    except Exception:  # noqa: BLE001 — network/extension failures skip cleanly
        pytest.skip("sonarx lake unreachable")
    assert "TRANSACTION_HASH" in cols
    assert "TOKEN_ADDRESS" in cols
    assert duckdb is not None
