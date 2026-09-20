"""Tests for the lake gap-detector (DI fakes, no network, no monkeypatch)."""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.ingest.lake_gaps import GapDeps, fill_gaps, find_gaps, lookback_dates, main

# ── Fakes (DI seams — same pattern as tests/test_lake_backfill.py) ────


class FakeLake:
    """SonarxLakeSource stand-in: every count/read yields rows for present dates."""

    def __init__(self, present: bool = True) -> None:
        self.present = present
        self.counts: list[tuple[str, str, str]] = []
        self.reads: list[tuple[str, str, str]] = []

    def available(self) -> bool:
        return True

    def count(self, chain: str, table: str, day: str) -> int:
        self.counts.append((chain, table, day))
        return 1 if self.present else 0

    def _rows(self, chain: str, day: str, table: str) -> list[dict[str, Any]]:
        self.reads.append((chain, day, table))
        return [{"blockNumber": 1}] * (3 if table == "transfers" else 2)

    def read_transfers(self, chain: str, day: str) -> Any:
        return iter(self._rows(chain, day, "transfers"))

    def read_traces(self, chain: str, day: str) -> Any:
        return iter(self._rows(chain, day, "traces"))


class FakeCH:
    """ClickHouse client stand-in: ``execute`` returns one [[rows]] row."""

    def __init__(self, rows: int = 0) -> None:
        self.rows = rows
        self.queries: list[dict[str, Any]] = []

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> Any:
        self.queries.append(params or {})
        return [[self.rows]]


class FakeWriter:
    """ClickHouseWriter stand-in: records transfer batches, counts internal rows."""

    def __init__(self) -> None:
        self.transfers: list[tuple[str, list[dict[str, Any]]]] = []
        self.internal: list[int] = []

    def write_transfers(self, chain: str, events: list[dict[str, Any]]) -> int:
        self.transfers.append((chain, events))
        return len(events)

    def client(self) -> Any:
        return self

    def execute(self, sql: str, rows: list[Any] | None = None) -> Any:
        self.internal.append(len(rows or []))
        return None


# ── Date window ───────────────────────────────────────────────────────


def test_lookback_window_expansion() -> None:
    assert lookback_dates(3, end="2026-09-12") == ["2026-09-10", "2026-09-11", "2026-09-12"]
    assert lookback_dates(0) == []


# ── Gap rule ──────────────────────────────────────────────────────────


def test_lake_has_and_ch_empty_is_a_gap() -> None:
    gaps = find_gaps("base", 5, source=FakeLake(present=True), ch_client=FakeCH(rows=0))
    assert gaps == lookback_dates(5)  # every lake-present day, CH empty → all gaps


def test_partial_ch_count_is_not_a_gap() -> None:
    # Short-but-nonzero: dedup/rewrites make partial counts normal → never refill.
    gaps = find_gaps("base", 5, source=FakeLake(present=True), ch_client=FakeCH(rows=5))
    assert gaps == []


def test_lake_missing_is_not_a_gap() -> None:
    # The lake cannot serve the date, so CH emptiness there is not our gap.
    gaps = find_gaps("base", 5, source=FakeLake(present=False), ch_client=FakeCH(rows=0))
    assert gaps == []


def test_ch_query_is_parameterized_and_scoped() -> None:
    ch = FakeCH(rows=0)
    find_gaps("arbitrum", 1, source=FakeLake(present=True), ch_client=ch)
    assert ch.queries[0]["chain"] == "arbitrum"
    assert ch.queries[0]["lo"] <= ch.queries[0]["hi"]


def test_unsupported_chain_raises() -> None:
    with pytest.raises(ValueError, match="unsupported sonarx chain"):
        find_gaps("ethereum", 1, source=FakeLake(), ch_client=FakeCH())


def test_unavailable_source_returns_empty() -> None:
    class Dead:
        def available(self) -> bool:
            return False

    assert find_gaps("base", 5, source=Dead(), ch_client=FakeCH()) == []


# ── Fill delegation ───────────────────────────────────────────────────


def test_fill_delegates_expected_partitions() -> None:
    src, wh = FakeLake(), FakeWriter()
    counts = fill_gaps("base", ["2026-09-08", "2026-09-09"], source=src, writer=wh)
    assert counts == {
        "base/2026-09-08/transfers": 3,
        "base/2026-09-08/traces": 2,
        "base/2026-09-09/transfers": 3,
        "base/2026-09-09/traces": 2,
    }
    assert src.reads == [
        ("base", "2026-09-08", "transfers"),
        ("base", "2026-09-08", "traces"),
        ("base", "2026-09-09", "transfers"),
        ("base", "2026-09-09", "traces"),
    ]


def test_fill_empty_dates_is_noop() -> None:
    assert fill_gaps("base", [], source=FakeLake(), writer=FakeWriter()) == {}


def test_main_logs_scan_and_fills_gaps() -> None:
    src, ch, wh = FakeLake(present=True), FakeCH(rows=0), FakeWriter()
    result = main("base", 2, fill=True, deps=GapDeps(source=src, ch_client=ch, writer=wh))
    assert result["gaps_found"] == 2
    assert result["dates_checked"] == 2
    assert set(result["fill"]) == {f"base/{d}/{t}" for d in result["gaps"] for t in ("transfers", "traces")}


def test_main_scan_only_does_not_write() -> None:
    wh = FakeWriter()
    deps = GapDeps(source=FakeLake(), ch_client=FakeCH(rows=0), writer=wh)
    result = main("base", 2, fill=False, deps=deps)
    assert result["gaps_found"] == 2
    assert "fill" not in result
    assert wh.transfers == []
