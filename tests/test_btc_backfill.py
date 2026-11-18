"""Tests for the BTC backfill entry point — the seam into the verified writer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lite_indexer.archive import UnitResult, UnitSpec, WriteError
from lite_indexer.query import btc_backfill
from lite_indexer.query.btc_source import BtcSource


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]], cols: list[str]) -> None:
        self._rows = rows
        self.description = [(c,) for c in cols]

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows[0] if self._rows else None


class FakeConnection:
    def __init__(self, count: int) -> None:
        self._count = count
        self.calls: list[str] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> FakeCursor:
        self.calls.append(sql)
        return FakeCursor([(self._count,)], ["count_star"])


def _source(count: int = 42) -> tuple[BtcSource, FakeConnection]:
    fake = FakeConnection(count)
    return BtcSource(connect=lambda: fake), fake


def test_count_rows_reads_one_partition_and_returns_an_int() -> None:
    src, fake = _source(963_618)
    assert btc_backfill.count_rows(src, "btc_transactions", "2026-11-15") == 963_618
    assert "date=2026-11-15" in fake.calls[0]


def test_plan_sets_day_granularity_and_the_counted_row_total() -> None:
    src, _ = _source(144)
    plan = btc_backfill.plan_day(src, "btc_blocks", "2026-11-15")
    spec = plan.spec()
    assert spec == UnitSpec(chain="btc", table="btc_blocks", date="2026-11-15", expected_rows=144)
    assert spec.hour == "", "a BTC day is one unit; an hour would invent a partition"


def test_write_day_hands_the_writer_a_stream_not_a_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The rows must reach write_unit lazily — a list is a whole day in memory."""
    src, _ = _source(3)
    seen: dict[str, Any] = {}

    def fake_iter(table: str, date: str, *, batch_size: int = 50_000):
        seen["called"] = (table, date)
        yield {"tx_hash": "a"}
        yield {"tx_hash": "b"}
        yield {"tx_hash": "c"}

    monkeypatch.setattr(src, "iter_rows", fake_iter)
    captured: dict[str, Any] = {}

    def fake_write_unit(spec: UnitSpec, rows: Any, **kw: Any) -> UnitResult:
        captured["spec"] = spec
        captured["is_iterator"] = not isinstance(rows, list | tuple)
        captured["rows"] = list(rows)
        return UnitResult(spec=spec, key="k", rows=3, bytes_written=1, verified=True)

    monkeypatch.setattr(btc_backfill, "write_unit", fake_write_unit)
    result = btc_backfill.write_day(src, "btc_transactions", "2026-11-15")

    assert result.verified is True
    assert seen["called"] == ("btc_transactions", "2026-11-15")
    assert captured["is_iterator"] is True
    assert len(captured["rows"]) == 3
    assert captured["spec"].chain == "btc"
    assert captured["spec"].expected_rows == 3


def test_write_day_refuses_an_empty_partition_instead_of_publishing_zero_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Publishing an empty unit would look like a complete day with no activity."""
    src, _ = _source(0)
    with pytest.raises(WriteError, match="empty_unit"):
        btc_backfill.write_day(src, "btc_transactions", "2010-01-01")


def test_write_day_rejects_an_unknown_table() -> None:
    src, _ = _source(1)
    with pytest.raises(ValueError, match="unsupported btc unit table"):
        btc_backfill.write_day(src, "evm_transfers", "2026-11-15")


def test_write_day_propagates_a_writer_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """A swallowed WriteError is a corrupt archive — it must escape."""
    src, _ = _source(2)
    monkeypatch.setattr(src, "iter_rows", lambda *a, **k: iter([{"tx_hash": "a"}, {"tx_hash": "b"}]))

    def boom(spec: UnitSpec, rows: Any, **kw: Any) -> UnitResult:
        raise WriteError("row_count_mismatch", key="k", detail="source=2 written=1")

    monkeypatch.setattr(btc_backfill, "write_unit", boom)
    with pytest.raises(WriteError, match="row_count_mismatch"):
        btc_backfill.write_day(src, "btc_transactions", "2026-11-15")


def test_cli_dry_run_publishes_nothing(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    src, _ = _source(7)
    monkeypatch.setattr(btc_backfill, "BtcSource", lambda: src)
    monkeypatch.setattr(btc_backfill, "write_unit", lambda *a, **k: pytest.fail("dry run must not publish"))

    rc = btc_backfill.main(["--dates", "2026-11-15", "--dry-run"])

    assert rc == 0
    assert "7 rows (dry run)" in capsys.readouterr().out


def test_cli_rejects_a_malformed_date_without_publishing(monkeypatch: pytest.MonkeyPatch) -> None:
    src, _ = _source(1)
    monkeypatch.setattr(btc_backfill, "BtcSource", lambda: src)
    with pytest.raises(ValueError, match="invalid date"):
        btc_backfill.main(["--dates", "2026-11-1"])
