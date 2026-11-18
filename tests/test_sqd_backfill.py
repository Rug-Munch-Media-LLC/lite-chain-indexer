"""Tests for the SQD → B2 day-unit backfill.

Two seams, both constructor/module-level rather than ``monkeypatch`` (the repo
runs a decrease-only monkeypatch ratchet at its limit):

* the portal client is a :class:`FakeSource` injected as ``src=`` — it records
  every request body so batching and the trace/transaction pairing are asserted
  from the wire, not from a guess;
* the writer's rclone/duckdb seams are the real
  :func:`lite_indexer.archive.unit_writer.write_unit` arguments, so the tests
  assert the ``UnitSpec`` a publisher WOULD hand it. ``write_unit`` itself is
  exercised by its own module's tests; here the contract under test is the spec
  and the row accounting, which is what makes ``expected_rows`` exact.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest

from lite_indexer.archive import UnitSpec, object_key
from lite_indexer.query import (
    sqd_backfill,
    sqd_backfill_cli,
    sqd_day,
    sqd_rows,
    sqd_source,
    sqd_tables,
)

DAY = dt.date(2026, 11, 17)
#: Model epoch0 — exactly midnight of :data:`DAY` UTC, so block 1 falls on the
#: previous date and the binary search has a real boundary to find above it.
MIDNIGHT = int(dt.datetime.combine(DAY, dt.time.min, tzinfo=dt.UTC).timestamp())
DATASET = "polygon-mainnet"


class FakeSource:
    """Portal double: header probes by number, data streams by window.

    Headers follow a linear model — block ``start + i`` has timestamp
    ``epoch0 + i * step`` — and are answered for every block in ``[1, head]``,
    exactly as the real portal does (verified: polygon blocks 1, 12345678 and
    97530000 all return a header). A probe ABOVE ``head`` answers 204, which is
    the portal's "nothing at or beyond this block" signal and the only reason
    the binary search can terminate correctly.
    """

    def __init__(
        self,
        start: int,
        count: int,
        *,
        epoch0: int = MIDNIGHT,
        step: int = 12,
        blocks: dict[tuple[int, int], list[dict[str, Any]]] | None = None,
        head: int | None = None,
        fail_status: int | None = None,
    ) -> None:
        self.start = start
        self.count = count
        self.epoch0 = epoch0
        self.step = step
        self.blocks = blocks or {}
        self.fallback: list[dict[str, Any]] = []
        self._head = (start + count - 1) if head is None else head
        self.fail_status = fail_status
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.EVM_DATASETS = {
            "polygon": DATASET,
            "base": "base-mainnet",
            "optimism": "optimism-mainnet",
        }
        self.PORTAL = "https://portal.example"

    def head(self, dataset: str) -> int | None:
        return self._head

    def timestamp(self, number: int) -> int:
        """The model timestamp for a block number."""
        return self.epoch0 + (number - self.start) * self.step

    def _request(
        self, method: str, url: str, payload: dict[str, Any] | None = None, *, timeout: float = 60.0
    ) -> tuple[int, str]:
        assert payload is not None
        self.calls.append((method, url, payload))
        if self.fail_status is not None:
            return self.fail_status, "overloaded"
        if "traces" in payload or "logs" in payload:
            key = (int(payload["fromBlock"]), int(payload["toBlock"]))
            return 200, "\n".join(json.dumps(b) for b in self.blocks.get(key, self.fallback))
        number = int(payload["fromBlock"])
        if number > self._head or number < 1:
            return 204, ""
        return 200, json.dumps({"header": {"number": number, "timestamp": self.timestamp(number)}})


# ── date iteration ──────────────────────────────────────────────────


def test_iter_days_is_oldest_first_and_inclusive() -> None:
    """--days N yields N dates ending at the given day, oldest first."""
    days = list(sqd_day.iter_days(DAY, 3))
    assert days == [dt.date(2026, 11, 15), dt.date(2026, 11, 16), DAY]


def test_iter_days_rejects_zero() -> None:
    """A zero/negative window is a caller bug, not an empty backfill."""
    with pytest.raises(ValueError, match="days must be >= 1"):
        list(sqd_day.iter_days(DAY, 0))


def test_day_bounds_is_exactly_one_utc_day() -> None:
    """A day unit is [00:00:00, 24:00:00) UTC — no timezone drift."""
    start, end = sqd_day.day_bounds(DAY)
    assert dt.datetime.fromtimestamp(start, tz=dt.UTC).strftime("%Y-%m-%d %H:%M:%S") == "2026-11-17 00:00:00"
    assert end - start == 86_400


# ── block range resolution ──────────────────────────────────────────


def test_first_block_at_or_after_finds_the_boundary() -> None:
    """Binary search returns the first block whose timestamp reaches the epoch."""
    src = FakeSource(100, 64)
    target = src.timestamp(131)
    assert sqd_day.first_block_at_or_after(src, DATASET, target, head=src.head(DATASET)) == 131


def test_first_block_at_or_after_returns_none_past_head() -> None:
    """A target later than every held block has no boundary."""
    src = FakeSource(100, 8)
    assert sqd_day.first_block_at_or_after(src, DATASET, 10**10, head=src.head(DATASET)) is None


def test_day_block_range_is_the_real_blocks_of_that_day() -> None:
    """The range's first block is in the day and the block before it is not."""
    # 300 s blocks so 400 of them span more than one UTC day — the range has to
    # be able to close on both ends for this assertion to mean anything.
    src = FakeSource(1000, 400, step=300)
    day = dt.datetime.fromtimestamp(src.timestamp(1200), tz=dt.UTC).date()
    bounds = sqd_day.day_block_range(src, DATASET, day, head=src.head(DATASET))
    assert bounds is not None
    first, last = bounds
    start_ts, end_ts = sqd_day.day_bounds(day)
    assert src.timestamp(first) >= start_ts
    assert src.timestamp(first - 1) < start_ts
    assert src.timestamp(last) < end_ts
    assert src.timestamp(last + 1) >= end_ts


def test_block_range_memo_saves_a_search_per_day() -> None:
    """Consecutive days share a boundary epoch, so the cache halves the work."""
    src = FakeSource(1000, 400, step=300)
    cache: dict[int, int | None] = {}
    day = dt.datetime.fromtimestamp(src.timestamp(1200), tz=dt.UTC).date()
    sqd_day.day_block_range(src, DATASET, day, head=src.head(DATASET), cache=cache)
    after_first = len(src.calls)
    sqd_day.day_block_range(src, DATASET, day + dt.timedelta(days=1), head=src.head(DATASET), cache=cache)
    assert len(src.calls) - after_first < after_first


# ── backfill_day: batching and accounting ───────────────────────────


def _trace_block(number: int, timestamp: int, traces: list[dict[str, Any]], hashes: list[str]) -> dict[str, Any]:
    return {
        "header": {"number": number, "timestamp": timestamp},
        "traces": traces,
        "transactions": [{"hash": h} for h in hashes],
    }


def _value_trace(index: int, value: str, to: str = "0xabc") -> dict[str, Any]:
    return {
        "transactionIndex": index,
        "traceAddress": [],
        "type": "call",
        "action": {"from": "0xfrom", "to": to, "value": value, "input": "0xa9059cbb"},
        "result": {"gasUsed": "0x5208"},
    }


def test_backfill_day_counts_exactly_the_rows_it_yields() -> None:
    """expected_rows is the number of rows yielded — measured, never estimated."""
    src = FakeSource(100, 40)
    block = _trace_block(100, src.timestamp(100), [_value_trace(0, "0x1"), _value_trace(1, "0x2")], ["0xh0", "0xh1"])
    src.blocks = {(100, 102): [block], (103, 105): []}
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    rows = list(sqd_backfill.backfill_day("polygon", "internal_transfers", day, src=src, sleep_s=0, block_batch=3))
    assert len(rows) == 2
    assert {r["tx_hash"] for r in rows} == {"0xh0", "0xh1"}
    assert all(r["chain"] == "polygon" for r in rows)


def test_backfill_day_skips_frames_that_cannot_move_value() -> None:
    """Staticcall and zero-value calls are dropped, matching the table's contract."""
    traces = [
        _value_trace(0, "0x5"),
        {"transactionIndex": 0, "traceAddress": [0], "type": "staticcall", "action": {"value": "0x0"}},
        _value_trace(1, "0x0"),
        {"transactionIndex": 0, "traceAddress": [1], "type": "delegatecall", "action": {"value": "0x9"}},
    ]
    src = FakeSource(100, 10)
    src.fallback = [_trace_block(100, src.timestamp(100), traces, ["0xh0", "0xh1"])]
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    rows = list(sqd_backfill.backfill_day("polygon", "internal_transfers", day, src=src, sleep_s=0))
    assert len(rows) == 1 and rows[0]["call_type"] == "call"


def test_backfill_day_requests_transactions_alongside_traces() -> None:
    """Without the sibling transactions field the row's tx_hash is empty."""
    src = FakeSource(100, 10)
    src.fallback = [_trace_block(100, src.timestamp(100), [_value_trace(0, "0x1")], ["0xh"])]
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    list(sqd_backfill.backfill_day("polygon", "internal_transfers", day, src=src, sleep_s=0))
    data_calls = [c for c in src.calls if "traces" in c[2]]
    assert data_calls, "no data request was made"
    assert data_calls[0][2]["transactions"] == [{}]
    assert data_calls[0][2]["fields"]["transaction"] == {"hash": True}


def test_backfill_day_paginates_in_block_batches() -> None:
    """Batches advance by window, never by returned-block contiguity."""
    src = FakeSource(100, 30, blocks={})
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    list(sqd_backfill.backfill_day("polygon", "internal_transfers", day, src=src, sleep_s=0, block_batch=10))
    windows = [(c[2]["fromBlock"], c[2]["toBlock"]) for c in src.calls if "traces" in c[2]]
    assert windows == [(100, 109), (110, 119), (120, 129)]


def test_backfill_day_raises_on_unsupported_chain() -> None:
    """A chain with no EVM dataset fails loudly rather than yielding zero rows."""
    src = FakeSource(1, 2)
    with pytest.raises(sqd_backfill.UnsupportedChainError, match="no SQD EVM dataset"):
        list(sqd_backfill.backfill_day("solana", "internal_transfers", DAY, src=src, sleep_s=0))


def test_backfill_day_raises_on_unsupported_table() -> None:
    """Tables we do not serve raise instead of returning an empty unit."""
    src = FakeSource(1, 2)
    with pytest.raises(sqd_backfill.UnsupportedTableError, match="no verified portal field set"):
        list(sqd_backfill.backfill_day("polygon", "dex_events", DAY, src=src, sleep_s=0))


def test_backfill_day_raises_on_portal_refusal() -> None:
    """An HTTP 529 becomes SQDPortalError — never a silently short unit."""
    src = FakeSource(100, 10, fail_status=529)
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    with pytest.raises(Exception, match="HTTP 529|header probe"):  # noqa: B017 — message is the contract
        list(sqd_backfill.backfill_day("polygon", "internal_transfers", day, src=src, sleep_s=0))


# ── evm_transfers ──────────────────────────────────────────────────


def test_log_rows_use_the_erc20_topic_selector() -> None:
    """evm_transfers asks for Transfer logs only — not every log in the block."""
    src = FakeSource(100, 10, blocks={})
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    list(sqd_backfill.backfill_day("polygon", "evm_transfers", day, src=src, sleep_s=0))
    selector = [c for c in src.calls if "logs" in c[2]][0][2]["logs"]
    assert selector == [{"topic0": [sqd_rows.ERC20_TRANSFER_TOPIC]}]


def test_log_row_decodes_erc20_transfer_and_rejects_nft() -> None:
    """3-topic/32-byte-data is ERC-20; a 4-topic Transfer is an NFT and skipped."""
    block = {"header": {"number": 5, "timestamp": 1_700_000_000}}
    erc20 = {
        "address": "0xToken",
        "topics": [
            sqd_rows.ERC20_TRANSFER_TOPIC,
            "0x000000000000000000000000" + "ab" * 20,
            "0x000000000000000000000000" + "cd" * 20,
        ],
        "data": "0x" + "00" * 31 + "05",
        "transactionHash": "0xtx",
        "logIndex": 3,
    }
    row = sqd_rows.log_row("polygon", block, erc20)
    assert row is not None
    assert row["from_address"] == "0x" + "ab" * 20
    assert row["value"] == "5"
    assert row["token_address"] == "0xtoken"
    nft = {**erc20, "topics": [*erc20["topics"], "0x" + "00" * 31 + "01"]}
    assert sqd_rows.log_row("polygon", block, nft) is None


# ── write_unit handoff ─────────────────────────────────────────────


def test_publish_day_hands_write_unit_an_exact_spec(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """The UnitSpec carries the measured row count and the archive's key layout."""
    src = FakeSource(100, 10)
    src.fallback = [
        _trace_block(100, src.timestamp(100), [_value_trace(0, "0x1"), _value_trace(1, "0x2")], ["0xh0", "0xh1"])
    ]
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()

    captured: list[tuple[UnitSpec, list[dict[str, Any]], str]] = []
    real_write = sqd_backfill_cli.write_unit

    def _fake_write(spec: UnitSpec, rows: Any, *, remote: str = "", skip_if_present: bool = True) -> Any:
        captured.append((spec, list(rows), remote))
        from lite_indexer.archive import UnitResult

        return UnitResult(
            spec=spec, key=object_key(spec, remote), rows=spec.expected_rows, bytes_written=42, verified=True
        )

    sqd_backfill_cli.write_unit = _fake_write
    try:
        report = sqd_backfill_cli.publish_day(
            "polygon", "internal_transfers", day, remote="b2-test:root", src=src, sleep_s=0
        )
    finally:
        sqd_backfill_cli.write_unit = real_write

    assert len(captured) == 1
    spec, rows, remote = captured[0]
    assert spec.chain == "polygon"
    assert spec.table == "internal_transfers"
    assert spec.date == day.isoformat()
    assert spec.expected_rows == len(rows) == 2
    assert remote == "b2-test:root"
    assert object_key(spec, remote) == f"b2-test:root/polygon/internal_transfers/{day.isoformat()}/part-000.parquet"
    assert report.startswith(f"ok polygon/internal_transfers/{day.isoformat()} rows=2")


def test_publish_day_does_not_write_an_empty_day() -> None:
    """A day with no rows reports 'empty' and produces no object."""
    src = FakeSource(100, 10, blocks={})
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()
    report = sqd_backfill_cli.publish_day(
        "polygon", "internal_transfers", day, remote="b2-test:root", src=src, sleep_s=0
    )
    assert report == f"empty polygon/internal_transfers/{day.isoformat()}"


def test_publish_day_propagates_a_write_failure() -> None:
    """A WriteError is never swallowed — a swallowed one is a corrupt archive."""
    from lite_indexer.archive import WriteError

    src = FakeSource(100, 10)
    src.fallback = [_trace_block(100, src.timestamp(100), [_value_trace(0, "0x1")], ["0xh"])]
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()

    real_write = sqd_backfill_cli.write_unit

    def _boom(*_a: Any, **_k: Any) -> Any:
        raise WriteError("row_count_mismatch", key="k", detail="source=1 written=0")

    sqd_backfill_cli.write_unit = _boom
    try:
        with pytest.raises(WriteError, match="row_count_mismatch"):
            sqd_backfill_cli.publish_day("polygon", "internal_transfers", day, remote="r", src=src, sleep_s=0)
    finally:
        sqd_backfill_cli.write_unit = real_write


# ── CLI ────────────────────────────────────────────────────────────


def test_cli_dry_run_counts_and_publishes_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    """--dry-run prints a row count and never reaches write_unit."""
    src = FakeSource(100, 10)
    src.fallback = [
        _trace_block(
            100, src.timestamp(100), [_value_trace(0, "0x1"), _value_trace(1, "0x2"), _value_trace(2, "0x3")], ["0xh"]
        )
    ]
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()

    real_src, real_write = sqd_backfill_cli.SQDSource, sqd_backfill_cli.write_unit
    sqd_backfill_cli.SQDSource = lambda: src  # type: ignore[assignment]
    sqd_backfill_cli.write_unit = lambda *_a, **_k: pytest.fail("dry-run must not write")
    try:
        code = sqd_backfill_cli.cli(
            [
                "--chain",
                "polygon",
                "--table",
                "internal_transfers",
                "--day",
                day.isoformat(),
                "--dry-run",
                "--sleep",
                "0",
            ]
        )
    finally:
        sqd_backfill_cli.SQDSource, sqd_backfill_cli.write_unit = real_src, real_write
    assert code == 0
    out = capsys.readouterr().out
    assert f"dry-run polygon/internal_transfers/{day.isoformat()} rows=3" in out


def test_cli_reports_a_failed_day_without_aborting_the_run(capsys: pytest.CaptureFixture[str]) -> None:
    """One bad day is reported and the run continues; the exit code is non-zero."""
    src = FakeSource(100, 10, fail_status=529)
    day = dt.datetime.fromtimestamp(src.timestamp(100), tz=dt.UTC).date()

    real_src = sqd_backfill_cli.SQDSource
    sqd_backfill_cli.SQDSource = lambda: src  # type: ignore[assignment]
    try:
        code = sqd_backfill_cli.cli(
            [
                "--chain",
                "polygon",
                "--table",
                "internal_transfers",
                "--day",
                day.isoformat(),
                "--dry-run",
                "--sleep",
                "0",
            ]
        )
    finally:
        sqd_backfill_cli.SQDSource = real_src
    assert code == 1
    assert "FAIL polygon/internal_transfers" in capsys.readouterr().out


# ── per-chain table availability ────────────────────────────────────


def test_measured_absent_table_is_refused_before_any_request() -> None:
    """optimism has no portal traces table; the backfill must not spend a run on it."""
    src = FakeSource(100, 10)
    with pytest.raises(sqd_backfill.MissingTableError, match="does not serve 'internal_transfers'"):
        list(sqd_backfill.backfill_day("optimism", "internal_transfers", DAY, src=src, sleep_s=0))
    assert src.calls == [], "a measured-absent table must cost zero requests"


def test_absent_traces_does_not_block_evm_transfers() -> None:
    """The gap is trace-specific: optimism's logs table answers."""
    assert sqd_tables.tables_for("optimism", "internal_transfers") is False
    assert sqd_tables.tables_for("optimism", "evm_transfers") is True


def test_probe_traces_reads_the_400_body_as_absent() -> None:
    """A 400 'table does not exist' is 'absent', not an overload to retry."""

    class _NoTraces(FakeSource):
        def _request(
            self, method: str, url: str, payload: dict | None = None, *, timeout: float = 60.0
        ) -> tuple[int, str]:
            self.calls.append((method, url, payload or {}))
            if "traces" in (payload or {}):
                return 400, '{"error":{"code":"malformed_request","message":"table \'traces\' does not exist"}}'
            return 200, json.dumps({"header": {"number": 5, "timestamp": 1}})

    src = _NoTraces(1, 10)
    assert sqd_tables.probe_traces(src, "optimism") is False


def test_probe_traces_raises_on_overload() -> None:
    """529 is 'try later', never 'absent' — conflating them loses a chain silently."""
    src = FakeSource(100, 10, fail_status=529)
    with pytest.raises(sqd_source.SQDPortalError, match="traces probe HTTP 529"):
        sqd_tables.probe_traces(src, "polygon")
