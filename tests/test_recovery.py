"""Tests for recovery + integrity tooling (lite_indexer/recovery.py).

Covers:

1. ``query_chain_stats`` — min/max/distinct/count per chain
2. ``compute_integrity`` — contiguity percentage + status classification
3. ``find_missing_ranges`` — sampled gap detection over the block axis
4. ``recover_chain_state`` — resets the state file + high-water mark to
   the ClickHouse max (the 08/03 + 08/04 state-regression fix), plus the
   ``backfill=True`` rewind to the earliest missing range
5. ``run_integrity_check`` + ``summarize_report`` — daily audit output

Hermetic — fake ClickHouse clients only, no network.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from lite_indexer.recovery import (
    compute_integrity,
    find_missing_ranges,
    query_chain_stats,
    recover_chain_state,
    run_integrity_check,
    summarize_report,
)
from lite_indexer.state import StateHighWater, StateManager


class FakeCH:
    """Configurable ClickHouse double.

    ``execute`` dispatches on query shape:
      - ``min(`` + ``max(`` + ``uniqExact`` → aggregate stats row
      - ``DISTINCT`` → sampled block rows
    """

    def __init__(
        self,
        stats: list[tuple[Any, ...]] | None = None,
        distinct: list[tuple[int, ...]] | None = None,
    ) -> None:
        self.stats = stats or []
        self.distinct = distinct or []
        self.queries: list[str] = []

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        self.queries.append(query)
        if "DISTINCT" in query:
            return self.distinct
        return self.stats


@pytest.fixture()
def state_dir(tmp_path: Path):
    """Patch STATE_DIR to a temp dir for the duration of the test."""
    patched = tmp_path / "state"
    patched.mkdir(parents=True, exist_ok=True)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("lite_indexer.state.STATE_DIR", patched)
        yield patched


# ═══════════════════════════════════════════════════════════════════════
# query_chain_stats / compute_integrity
# ═══════════════════════════════════════════════════════════════════════


class TestChainStats:
    def test_evm_stats(self):
        ch = FakeCH(stats=[(100, 25_674_000, 25_673_901, 359_000_000)])
        stats = query_chain_stats(ch, "ethereum", "evm")
        assert stats["ok"] is True
        assert stats["min_block"] == 100
        assert stats["max_block"] == 25_674_000
        assert stats["distinct_blocks"] == 25_673_901
        assert stats["total_rows"] == 359_000_000

    def test_none_client_unavailable(self):
        stats = query_chain_stats(None, "ethereum", "evm")
        assert stats["ok"] is False
        assert stats["error"] == "clickhouse_unavailable"

    def test_query_failure_unavailable(self):
        class BoomCH:
            def execute(self, *a, **k):
                raise RuntimeError("auth failed")

        stats = query_chain_stats(BoomCH(), "ethereum", "evm")
        assert stats["ok"] is False
        assert "auth failed" in stats["error"]

    def test_empty_table(self):
        ch = FakeCH(stats=[(None, None, None, 0)])
        stats = query_chain_stats(ch, "ethereum", "evm")
        assert stats["ok"] is True
        assert stats["max_block"] is None


class TestComputeIntegrity:
    def test_contiguous_ok(self):
        stats = compute_integrity(
            {"ok": True, "min_block": 100, "max_block": 1_000_000, "distinct_blocks": 999_901, "total_rows": 5_000_000}
        )
        assert stats["status"] == "ok"
        assert stats["contiguous_pct"] >= 99.9

    def test_gaps_detected(self):
        stats = compute_integrity(
            {"ok": True, "min_block": 0, "max_block": 1_000_000, "distinct_blocks": 500_000, "total_rows": 5_000_000}
        )
        assert stats["status"] == "gaps"
        assert stats["contiguous_pct"] == pytest.approx(50.0)

    def test_empty_status(self):
        stats = compute_integrity({"ok": True, "min_block": None, "max_block": None, "total_rows": 0})
        assert stats["status"] == "empty"

    def test_unavailable_status(self):
        stats = compute_integrity({"ok": False, "error": "boom"})
        assert stats["status"] == "unavailable"


# ═══════════════════════════════════════════════════════════════════════
# find_missing_ranges
# ═══════════════════════════════════════════════════════════════════════


class TestFindMissingRanges:
    def test_contiguous_no_gaps(self):
        ch = FakeCH(stats=[(0, 100_000, 100_001, 1_000_000)])
        present = list(range(0, 100_001, 1000))
        ch.distinct = [(b,) for b in present]
        assert find_missing_ranges(ch, "ethereum") == []

    def test_detects_large_gap(self):
        ch = FakeCH(stats=[(100, 25_674_000, 25_000_000, 400_000_000)])
        # Sampled blocks present: all except a 500k-block hole around 10M.
        stride = 1000
        present = []
        for b in range(100, 25_674_001, stride):
            if 10_000_000 <= b <= 10_500_000:
                continue
            present.append((b,))
        ch.distinct = present
        ranges = find_missing_ranges(ch, "ethereum")
        assert len(ranges) == 1
        lo, hi = ranges[0]
        # The detected range must cover the 10M–10.5M hole (sampled at
        # stride 1000 starting from min_block 100: first missing sample
        # is 10_000_100, last is 10_499_100 since 10_500_100 is outside
        # the hole and present).
        assert lo <= 10_000_100
        assert hi >= 10_499_100

    def test_unavailable_returns_empty(self):
        assert find_missing_ranges(None, "ethereum") == []


# ═══════════════════════════════════════════════════════════════════════
# recover_chain_state
# ═══════════════════════════════════════════════════════════════════════


class TestRecoverChainState:
    def test_resets_state_to_ch_max(self, tmp_path: Path, state_dir: Path):
        # State file regressed to 93_193 (the 08/04 incident); CH has 25.6M.
        manager = StateManager("ethereum")
        manager.save(93_193, 0)
        ch = FakeCH(stats=[(0, 25_674_000, 25_673_900, 359_000_000)])
        ch.distinct = [(b,) for b in range(0, 25_674_001, 1000)]

        result = recover_chain_state(ch, "ethereum", "evm", state_dir=state_dir)

        assert result["ok"] is True
        assert result["previous_state"] == 93_193
        assert result["ch_max"] == 25_674_000
        assert result["recovered_to"] == 25_674_000
        # State file + high-water mark now point at the CH max.
        assert StateManager("ethereum").load()["last_block"] == 25_674_000
        assert StateHighWater("ethereum").load() == 25_674_000

    def test_backfill_rewinds_to_missing_range(self, tmp_path: Path, state_dir: Path):
        manager = StateManager("ethereum")
        manager.save(25_000_000, 0)  # state ahead of a 10M hole? no: below CH max
        ch = FakeCH(stats=[(0, 25_674_000, 25_000_000, 400_000_000)])
        stride = 1000
        present = []
        for b in range(0, 25_674_001, stride):
            if 10_000_000 <= b <= 10_500_000:
                continue
            present.append((b,))
        ch.distinct = present

        result = recover_chain_state(ch, "ethereum", "evm", state_dir=state_dir, backfill=True)

        assert result["ok"] is True
        assert result["missing_ranges"], "expected a missing range"
        assert result["backfill_from"] is not None
        # State rewound to just below the earliest missing range.
        assert StateManager("ethereum").load()["last_block"] == result["backfill_from"] - 1

    def test_no_ch_returns_error(self, state_dir: Path):
        result = recover_chain_state(None, "ethereum", "evm", state_dir=state_dir)
        assert result["ok"] is False
        assert result["error"] == "clickhouse_unavailable"

    def test_no_data_returns_error(self, state_dir: Path):
        ch = FakeCH(stats=[(None, None, None, 0)])
        result = recover_chain_state(ch, "ethereum", "evm", state_dir=state_dir)
        assert result["ok"] is False
        assert result["error"] == "no_data_in_clickhouse"

    def test_solana_uses_slot_table(self, state_dir: Path):
        ch = FakeCH(stats=[(10_000, 200_000_000, 199_990_000, 5_000_000)])
        result = recover_chain_state(ch, "solana", "solana", state_dir=state_dir)
        assert result["ok"] is True
        assert result["recovered_to"] == 200_000_000
        # Solana queries must reference the SPL slot table.
        assert any("solana_spl_balance_changes" in q and "slot" in q for q in ch.queries)


# ═══════════════════════════════════════════════════════════════════════
# run_integrity_check / summarize_report
# ═══════════════════════════════════════════════════════════════════════


class TestIntegrityCheck:
    def test_mixed_report_summary(self):
        ch = FakeCH(stats=[(0, 1_000_000, 1_000_001, 5_000_000)])
        report = run_integrity_check(
            ch,
            ["ethereum", "base", "solana"],
            chain_types={"ethereum": "evm", "base": "evm", "solana": "solana"},
        )
        # The fake returns the same stats for every chain.
        summary = summarize_report(report)
        assert summary["chains"] == 3
        assert summary["ok_count"] == 3
        assert summary["gap_count"] == 0
        assert summary["total_rows"] == 15_000_000

    def test_summary_flags_gaps(self):
        ch = FakeCH(stats=[(0, 1_000_000, 500_000, 5_000_000)])
        report = run_integrity_check(ch, ["ethereum"])
        summary = summarize_report(report)
        assert summary["gap_count"] == 1
        assert summary["chains_with_gaps"] == ["ethereum"]

    def test_summary_unavailable(self):
        report = run_integrity_check(None, ["ethereum"])
        summary = summarize_report(report)
        assert summary["unavailable_count"] == 1
