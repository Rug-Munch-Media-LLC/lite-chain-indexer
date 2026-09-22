"""Tests for the BigLake → ClickHouse solana fill engine.

No network: the ``bq`` CLI is replaced by an injected fake runner and the
ClickHouse client by a fake recorder — pure DI, no monkeypatching.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.ingest.lake_bigquery_solana import (
    SolanaBigQueryFillRequest,
    backfill_solana_from_bigquery,
)
from lite_indexer.lakes.router import LakeProviderRouter
from lite_indexer.query.bigquery_source import BigQuerySource

DAY = "2026-09-21"


class FakeRunner:
    """Returns canned raw solana rows; records calls; can fail per date."""

    def __init__(self, rows_by_date: dict[str, list[dict[str, Any]]], fail_dates: set[str] | None = None) -> None:
        self.rows_by_date = rows_by_date
        self.fail_dates = fail_dates or set()
        self.calls: list[str] = []

    def __call__(self, sql: str, params: list[tuple[str, str, Any]]) -> list[dict[str, Any]]:
        p = {name: value for name, _t, value in params}
        day = (p.get("dates") or [DAY])[0]
        self.calls.append(day)
        if day in self.fail_dates:
            raise RuntimeError(f"boom {day}")
        offset = int(p.get("offset") or 0)
        rows = self.rows_by_date.get(day, [])
        return rows[offset : offset + int(p.get("limit") or 0)]


class FakeCHClient:
    """ClickHouse stand-in: counts return 0 (or preset), INSERTs recorded."""

    def __init__(self, existing: int = 0) -> None:
        self.existing = existing
        self.inserts: list[Any] = []

    def execute(self, sql: str, params: Any = None) -> list[Any]:
        if sql.startswith("SELECT count()"):
            return [(self.existing,)]
        if sql.startswith("INSERT"):
            self.inserts.append(params)
            return []
        return []


class FakeCHWriter:
    """ClickHouseWriter stand-in exposing ``client()`` / ``_connect()``."""

    def __init__(self, existing: int = 0) -> None:
        self.client_obj = FakeCHClient(existing)

    def client(self) -> Any:
        return self.client_obj

    def _connect(self) -> Any:
        return self.client_obj


def _raw_row(i: int) -> dict[str, Any]:
    return {
        "slot": 32000000 + i,
        "block_timestamp": 1758451200000 + i * 400,
        "tx_hash": f"sig{i}",
        "mint": "Mint" + str(i % 2),
        "owner": f"owner{i % 3}",
        "value": str(i + 1),
        "decimals": "6",
        "source": "src",
        "destination": "dst",
        "transfer_type": "transfer",
    }


def _source(tmp_path: Any, runner: FakeRunner) -> BigQuerySource:
    (tmp_path / "sa.json").write_text("{}")
    return BigQuerySource(
        credentials_path=str(tmp_path / "sa.json"),
        runner=runner,
        router=LakeProviderRouter(usage_path=tmp_path / "lake_usage.json"),
    )


def _req(**kwargs: Any) -> SolanaBigQueryFillRequest:
    defaults: dict[str, Any] = {"dates": [DAY], "batch_rows": 10, "max_rows": 200_000, "max_scan_gib": 500.0}
    defaults.update(kwargs)
    return SolanaBigQueryFillRequest(**defaults)


# ── Dry run ──────────────────────────────────────────────────────────


def test_dry_run_counts_without_writing(tmp_path: Any) -> None:
    runner = FakeRunner({DAY: [_raw_row(i) for i in range(25)]})
    ch = FakeCHWriter()
    counts = backfill_solana_from_bigquery(_req(dry_run=True), source=_source(tmp_path, runner), ch_writer=ch)
    assert counts == {f"solana/{DAY}/token_transfers": 25}
    assert ch.client_obj.inserts == []


def test_dry_run_pages_multi_page(tmp_path: Any) -> None:
    runner = FakeRunner({DAY: [_raw_row(i) for i in range(1250)]})
    counts = backfill_solana_from_bigquery(
        _req(dry_run=True), source=_source(tmp_path, runner), ch_writer=FakeCHWriter()
    )
    assert counts == {f"solana/{DAY}/token_transfers": 1250}


# ── Batching ─────────────────────────────────────────────────────────


def test_batching_multi_batch_writes(tmp_path: Any) -> None:
    runner = FakeRunner({DAY: [_raw_row(i) for i in range(25)]})
    ch = FakeCHWriter()
    counts = backfill_solana_from_bigquery(_req(batch_rows=10), source=_source(tmp_path, runner), ch_writer=ch)
    assert counts == {f"solana/{DAY}/token_transfers": 25}
    sizes = [len(batch) for batch in ch.client_obj.inserts]
    assert sizes == [10, 10, 5]


# ── max_rows hard stop ───────────────────────────────────────────────


def test_max_rows_hard_stops(tmp_path: Any) -> None:
    rows = [_raw_row(i) for i in range(25)]
    runner = FakeRunner({DAY: rows, "2026-09-22": rows, "2026-09-23": rows})
    counts = backfill_solana_from_bigquery(
        _req(dates=[DAY, "2026-09-22", "2026-09-23"], max_rows=30),
        source=_source(tmp_path, runner),
        ch_writer=FakeCHWriter(),
    )
    assert f"solana/{DAY}/token_transfers" in counts
    assert "solana/2026-09-22/token_transfers" in counts
    assert "solana/2026-09-23/token_transfers" not in counts
    assert sum(counts.values()) == 50


# ── Bad date never aborts ────────────────────────────────────────────


def test_bad_date_skipped_run_continues(tmp_path: Any) -> None:
    rows = [_raw_row(i) for i in range(5)]
    runner = FakeRunner({DAY: rows, "2026-09-22": rows}, fail_dates={DAY})
    counts = backfill_solana_from_bigquery(
        _req(dates=[DAY, "2026-09-22"]),
        source=_source(tmp_path, runner),
        ch_writer=FakeCHWriter(),
    )
    assert counts[f"solana/{DAY}/token_transfers"] == 0
    assert counts["solana/2026-09-22/token_transfers"] == 5


# ── Idempotency guard ────────────────────────────────────────────────


def test_existing_day_skipped_unless_forced(tmp_path: Any) -> None:
    runner = FakeRunner({DAY: [_raw_row(i) for i in range(5)]})
    ch = FakeCHWriter(existing=77)
    counts = backfill_solana_from_bigquery(_req(), source=_source(tmp_path, runner), ch_writer=ch)
    assert counts[f"solana/{DAY}/token_transfers"] == 0
    assert ch.client_obj.inserts == []
    counts = backfill_solana_from_bigquery(_req(force=True), source=_source(tmp_path, runner), ch_writer=ch)
    assert counts[f"solana/{DAY}/token_transfers"] == 5


# ── Request validation ───────────────────────────────────────────────


def test_unsupported_table_rejected(tmp_path: Any) -> None:
    with pytest.raises(ValueError, match="unsupported solana fill table"):
        backfill_solana_from_bigquery(
            _req(tables=("instructions",)), source=_source(tmp_path, FakeRunner({})), ch_writer=FakeCHWriter()
        )


def test_mint_filter_reaches_sql(tmp_path: Any) -> None:
    runner = FakeRunner({DAY: [_raw_row(0)]})
    src = _source(tmp_path, runner)
    backfill_solana_from_bigquery(_req(dry_run=True, mints=("Mint0",)), source=src, ch_writer=FakeCHWriter())
    assert src is not None


# ── Quota handling ───────────────────────────────────────────────────


class QuotaRunner:
    """Runner that simulates the BigQuery free-bytes quota cap."""

    def __call__(self, sql: str, params: Any) -> list[dict[str, Any]]:
        from lite_indexer.query.bigquery_common import BigQueryQuotaError

        raise BigQueryQuotaError("Quota exceeded: free query bytes scanned")


def test_quota_error_aborts_whole_run(tmp_path: Any) -> None:
    import pytest

    from lite_indexer.query.bigquery_common import BigQueryQuotaError

    with pytest.raises(BigQueryQuotaError):
        backfill_solana_from_bigquery(
            _req(dates=[DAY, "2026-09-22"]),
            source=_source(tmp_path, QuotaRunner()),
            ch_writer=FakeCHWriter(),
        )


def test_is_quota_error_matches_bq_stderr() -> None:
    from lite_indexer.query.bigquery_common import is_quota_error

    assert is_quota_error("Quota exceeded: Your project exceeded quota for free query bytes scanned.")
    assert is_quota_error("quotaExceeded: something")
    assert not is_quota_error("not found: table x")
    assert not is_quota_error("")
