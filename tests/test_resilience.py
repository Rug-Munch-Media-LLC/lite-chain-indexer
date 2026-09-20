"""Tests for the 2026-08-04 resilience hardening.

Covers:

1. Monotonic high-water mark (:class:`lite_indexer.state.StateHighWater`)
2. Backward-state guard (``LiteIndexer._guard_state_regression``) —
   state file regression recovers from the ClickHouse max block
3. Memory watchdog (``MemoryGate`` / ``get_process_rss_mb``)
4. ClickHouse health pause (``ClickHouseHealth``)
5. Per-cycle GC (``LiteIndexer._maybe_run_gc``)
6. Endpoint hard ban for 403/429 (``lite_indexer.rpc.pool``)
7. Range completeness check + backfill trigger
   (``storage.reconciliation.check_range_completeness`` +
   ``LiteIndexer._maybe_reconcile_range``)

Hermetic — no ClickHouse, no RPC, no network.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.indexer import LiteIndexer, get_process_rss_mb
from lite_indexer.state import StateHighWater


class FakeCH:
    """Minimal ClickHouse client double (execute only)."""

    def __init__(self, count_rows: list[tuple[int]] | None = None, max_rows: list[tuple[int]] | None = None) -> None:
        self.count_rows = count_rows or []
        self.max_rows = max_rows or []
        self.queries: list[str] = []

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        self.queries.append(query)
        if "count()" in query:
            return self.count_rows
        if "max(" in query:
            return self.max_rows
        return []


class FakeWriter:
    """ClickHouseWriter double exposing ``client()``."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def client(self) -> Any:
        return self._client


@pytest.fixture()
def state_dir(tmp_path: Path):
    """Patch STATE_DIR to a temp dir for the duration of the test."""
    patched = tmp_path / "state"
    patched.mkdir(parents=True, exist_ok=True)
    with patch("lite_indexer.state.STATE_DIR", patched):
        yield patched


@pytest.fixture()
def indexer(tmp_path: Path):
    """A LiteIndexer instance that never touches the network.

    STATE_DIR stays patched for the whole test (yield inside the patch
    context) so high-water marks / state writes never touch the real
    /var/data/chain-indexer/state directory.
    """
    with patch("lite_indexer.state.STATE_DIR", tmp_path / "state"):
        inst = LiteIndexer(chain="ethereum")
        # Keep the lazy CH writer pointing at a fake so no real connection.
        inst._ch_writer = FakeWriter(FakeCH())
        yield inst


# ── 1. High-water mark ────────────────────────────────────────────────


class TestStateHighWater:
    def test_update_and_load(self, state_dir: Path):
        hw = StateHighWater("ethereum")
        assert hw.load() is None
        hw.update(100)
        hw.update(200)
        assert hw.load() == 200

    def test_never_rewinds(self, state_dir: Path):
        hw = StateHighWater("ethereum")
        hw.update(500)
        hw.update(300)  # backward — must be a no-op
        assert hw.load() == 500

    def test_persists_across_instances(self, state_dir: Path):
        StateHighWater("base").update(42)
        assert StateHighWater("base").load() == 42


# ── 2. Backward-state guard ────────────────────────────────────────────


class TestStateRegressionGuard:
    @pytest.mark.asyncio
    async def test_recovers_to_ch_max_when_highwater_regressed(self, indexer: LiteIndexer, tmp_path: Path):
        # High-water mark says 25.6M; state file was clobbered to 93K.
        indexer._highwater.update(25_674_059)
        indexer._ch_writer = FakeWriter(FakeCH(max_rows=[(25_674_000,)]))
        summary: dict[str, Any] = {"errors": []}
        recovered = await indexer._guard_state_regression({"last_block": 93_193}, 93_194, summary)
        # resume = max(highwater, ch_max) + 1
        assert recovered == 25_674_060
        assert summary.get("state_regression") is True
        assert summary.get("state_regression_recovered_to") == 25_674_060

    @pytest.mark.asyncio
    async def test_no_recovery_within_tolerance(self, indexer: LiteIndexer, tmp_path: Path):
        # A small reorg rewind (within STATE_REGRESSION_TOLERANCE_BLOCKS)
        # must NOT be treated as corruption.
        indexer._highwater.update(1_000_000)
        indexer._ch_writer = FakeWriter(FakeCH(max_rows=[(999_990,)]))
        summary: dict[str, Any] = {"errors": []}
        recovered = await indexer._guard_state_regression({"last_block": 999_990}, 999_991, summary)
        assert recovered is None
        assert "state_regression" not in summary

    @pytest.mark.asyncio
    async def test_recovers_from_ch_when_no_highwater(self, indexer: LiteIndexer, tmp_path: Path):
        # First run with protection: no high-water mark, but CH is far
        # ahead of the (clobbered) state file.
        indexer._ch_writer = FakeWriter(FakeCH(max_rows=[(25_674_000,)]))
        summary: dict[str, Any] = {"errors": []}
        recovered = await indexer._guard_state_regression({"last_block": 100}, 101, summary)
        assert recovered == 25_674_001
        assert summary.get("state_regression") is True

    @pytest.mark.asyncio
    async def test_falls_back_to_highwater_when_ch_unavailable(self, indexer: LiteIndexer, tmp_path: Path):
        indexer._highwater.update(10_000_000)
        indexer._ch_writer = FakeWriter(FakeCH())  # CH client returns nothing / unavailable
        summary: dict[str, Any] = {"errors": []}
        recovered = await indexer._guard_state_regression({"last_block": 5_000}, 5_001, summary)
        assert recovered == 10_000_001

    @pytest.mark.asyncio
    async def test_state_trusted_when_all_unknown(self, indexer: LiteIndexer, tmp_path: Path):
        summary: dict[str, Any] = {"errors": []}
        recovered = await indexer._guard_state_regression({"last_block": 42}, 43, summary)
        assert recovered is None

    @pytest.mark.asyncio
    async def test_regression_increments_metric(self, indexer: LiteIndexer, tmp_path: Path):
        from lite_indexer import metrics

        counter = metrics._ALERT_COUNTERS["lite_indexer_state_regression_detected"]  # noqa: SLF001
        before = float(counter.labels(chain="ethereum")._value.get())  # noqa: SLF001
        indexer._highwater.update(25_674_059)
        indexer._ch_writer = FakeWriter(FakeCH(max_rows=[(25_674_000,)]))
        summary: dict[str, Any] = {"errors": []}
        await indexer._guard_state_regression({"last_block": 93_193}, 93_194, summary)
        after = float(counter.labels(chain="ethereum")._value.get())  # noqa: SLF001
        assert after == before + 1.0


# ── 3. Memory watchdog ─────────────────────────────────────────────────


class TestMemoryGate:
    def test_rss_reader_returns_positive(self):
        # On Linux /proc/self/statm always exists.
        assert get_process_rss_mb() >= 0

    def test_gate_closes_over_threshold(self):
        from lite_indexer.indexer import MemoryGate

        with (
            patch("lite_indexer.indexer.get_process_rss_mb", return_value=9999),
            patch("lite_indexer.indexer.get_cgroup_memory_mb", return_value=0),
        ):
            gate = MemoryGate(high_mb=1024)
            gate._last_check = 0.0  # force a fresh read
            assert gate.ok() is False

    def test_gate_open_under_threshold(self):
        from lite_indexer.indexer import MemoryGate

        with (
            patch("lite_indexer.indexer.get_process_rss_mb", return_value=100),
            patch("lite_indexer.indexer.get_cgroup_memory_mb", return_value=0),
        ):
            gate = MemoryGate(high_mb=1024)
            gate._last_check = 0.0
            assert gate.ok() is True

    def test_gate_disabled_when_threshold_zero(self):
        from lite_indexer.indexer import MemoryGate

        gate = MemoryGate(high_mb=0)
        assert gate.ok() is True


class TestCgroupMemoryGate:
    """2026-09-13 OOM post-mortem: gate must guard cgroup memory.current."""

    def _fake_sysfs(self, tmp_path: Path, cgroup_line: str, current_bytes: int) -> Path:
        root = tmp_path / "sysfs"
        rel = cgroup_line.split("::", 1)[1].strip().lstrip("/")
        cgdir = root / rel
        cgdir.mkdir(parents=True)
        (cgdir / "memory.current").write_text(str(current_bytes))
        return root

    def test_parses_v2_cgroup_and_reads_current(self, tmp_path: Path):
        from lite_indexer.indexer._resource_guards import get_cgroup_memory_mb

        root = self._fake_sysfs(tmp_path, "0::/system.slice/lite-indexer.service", 3 * 1024 * 1024 * 1024)
        fake_proc = tmp_path / "cgroup"
        fake_proc.write_text("0::/system.slice/lite-indexer.service\n")
        with (
            patch("lite_indexer.indexer._resource_guards._CGROUP_ROOT", str(root)),
            patch("lite_indexer.indexer._resource_guards._PROC_CGROUP", str(fake_proc)),
        ):
            assert get_cgroup_memory_mb() == 3072

    def test_returns_zero_when_cgroup_file_missing(self, tmp_path: Path):
        from lite_indexer.indexer._resource_guards import get_cgroup_memory_mb

        fake_proc = tmp_path / "cgroup"
        fake_proc.write_text("0::/system.slice/lite-indexer.service\n")
        with (
            patch("lite_indexer.indexer._resource_guards._CGROUP_ROOT", str(tmp_path)),
            patch("lite_indexer.indexer._resource_guards._PROC_CGROUP", str(fake_proc)),
        ):
            assert get_cgroup_memory_mb() == 0

    def test_gate_closes_on_cgroup_even_when_rss_under(self):
        from lite_indexer.indexer import MemoryGate

        with (
            patch("lite_indexer.indexer.get_process_rss_mb", return_value=100),
            patch("lite_indexer.indexer.get_cgroup_memory_mb", return_value=8000),
        ):
            gate = MemoryGate(high_mb=4096)
            gate._last_check = 0.0
            assert gate.ok() is False

    def test_gate_open_when_both_under(self):
        from lite_indexer.indexer import MemoryGate

        with (
            patch("lite_indexer.indexer.get_process_rss_mb", return_value=100),
            patch("lite_indexer.indexer.get_cgroup_memory_mb", return_value=200),
        ):
            gate = MemoryGate(high_mb=4096)
            gate._last_check = 0.0
            assert gate.ok() is True

    def test_falls_back_to_rss_when_cgroup_unavailable(self):
        from lite_indexer.indexer import MemoryGate

        with (
            patch("lite_indexer.indexer.get_process_rss_mb", return_value=5000),
            patch("lite_indexer.indexer.get_cgroup_memory_mb", return_value=0),
        ):
            gate = MemoryGate(high_mb=4096)
            gate._last_check = 0.0
            assert gate.ok() is False


# ── 4. ClickHouse health pause ─────────────────────────────────────────


class TestClickHouseHealth:
    def test_ok_when_client_available(self):
        from lite_indexer.indexer import ClickHouseHealth

        health = ClickHouseHealth(ch_writer=FakeWriter(FakeCH()))
        health._last_check = 0.0
        assert health.ok() is True

    def test_down_when_client_none(self):
        from lite_indexer.indexer import ClickHouseHealth

        health = ClickHouseHealth(ch_writer=FakeWriter(None))
        health._last_check = 0.0
        assert health.ok() is False

    def test_cached_result_respected(self):
        from lite_indexer.indexer import ClickHouseHealth

        health = ClickHouseHealth(ch_writer=FakeWriter(None))
        health._last_check = time.monotonic()  # fresh cache
        health._cached_ok = True
        assert health.ok() is True  # uses cache, does not re-probe


# ── 5. Periodic GC ─────────────────────────────────────────────────────


class TestPeriodicGC:
    def test_collects_every_n_cycles(self, indexer: LiteIndexer):
        calls: list[int] = []
        with (
            patch("lite_indexer.indexer._maintenance.GC_EVERY_N_CYCLES", 2),
            patch("lite_indexer.indexer._maintenance.gc.collect", side_effect=lambda: calls.append(1) or 0),
        ):
            indexer._maybe_run_gc()  # cycle 1 — no collect
            indexer._maybe_run_gc()  # cycle 2 — collect
            indexer._maybe_run_gc()  # cycle 3 — no collect
            indexer._maybe_run_gc()  # cycle 4 — collect
        assert len(calls) == 2


# ── 6. Endpoint hard ban (403/429) ─────────────────────────────────────


class TestEndpointBan:
    def test_ban_blocks_and_expires(self):
        from lite_indexer.rpc.pool import ban_endpoint, is_endpoint_banned, unban_endpoint

        url = "https://example.invalid/rpc"
        unban_endpoint(url)  # clean slate
        assert is_endpoint_banned(url) is False
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 60):
            ban_endpoint(url, reason="http_429")
        assert is_endpoint_banned(url) is True
        unban_endpoint(url)
        assert is_endpoint_banned(url) is False

    def test_ban_escalates_exponentially(self):
        from lite_indexer.rpc import ban_state, pool

        url = "https://example.invalid/rpc2"
        pool.unban_endpoint(url)
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 60):
            pool.ban_endpoint(url, reason="http_403")
            first_until = ban_state._endpoint_bans[url]
            pool.ban_endpoint(url, reason="http_403")
            second_until = ban_state._endpoint_bans[url]
        # Second ban window is strictly longer (exponential).
        assert second_until > first_until
        pool.unban_endpoint(url)

    def test_banned_endpoint_skipped_by_best_endpoint(self):
        from lite_indexer.rpc import pool

        healthy = "https://healthy.invalid/rpc"
        banned = "https://banned.invalid/rpc"
        pool.unban_endpoint(banned)
        pool._endpoint_failures.clear()
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            pool.ban_endpoint(banned, reason="http_429")
        # With the banned endpoint present, the picker must choose healthy.
        for _ in range(50):
            assert pool.get_best_endpoint([healthy, banned]) == healthy
        pool.unban_endpoint(banned)

    def test_success_unbans(self):
        from lite_indexer.rpc.pool import ban_endpoint, is_endpoint_banned, record_endpoint_success

        url = "https://example.invalid/rpc3"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(url, reason="http_429")
        assert is_endpoint_banned(url) is True
        record_endpoint_success(url)
        assert is_endpoint_banned(url) is False


# ── 7. Range completeness + backfill ───────────────────────────────────


class TestRangeCompleteness:
    def test_range_table_for(self):
        from lite_indexer.storage.reconciliation import range_table_for

        assert range_table_for("ethereum", "evm") == ("wallet_memory.evm_transfers", "block_number")
        assert range_table_for("solana", "solana") == (
            "wallet_memory.solana_spl_balance_changes",
            "slot",
        )

    def test_complete_range_is_ok(self):
        from lite_indexer.storage.reconciliation import check_range_completeness

        fake = FakeCH(count_rows=[(500,)])
        outcome = check_range_completeness("ethereum", fake, 100, 200, expected=500)
        assert outcome["ok"] is True
        assert outcome["actual"] == 500

    def test_short_range_reports_discrepancy(self):
        from lite_indexer.storage.reconciliation import check_range_completeness

        fake = FakeCH(count_rows=[(400,)])
        outcome = check_range_completeness("ethereum", fake, 100, 200, expected=500)
        assert outcome["ok"] is False
        assert outcome["actual"] == 400
        assert 19.9 < outcome["discrepancy_pct"] < 20.1

    def test_unavailable_ch_is_not_ok(self):
        from lite_indexer.storage.reconciliation import check_range_completeness

        outcome = check_range_completeness("ethereum", None, 100, 200, expected=500)
        assert outcome["ok"] is False
        assert "unavailable" in outcome["error"]

    def test_reconcile_mismatch_rewinds_state(self, indexer: LiteIndexer, tmp_path: Path):
        # Make the reconcile fire on the first check.
        with patch("lite_indexer.indexer._maintenance.DATA_RECONCILIATION_EVERY_N_CYCLES", 1):
            indexer._ch_writer = FakeWriter(FakeCH(count_rows=[(100,)]))
            indexer._cycles_since_reconcile = 0
            summary: dict[str, Any] = {}
            indexer._maybe_reconcile_range(1000, 1100, expected=1000, summary=summary)
        assert summary.get("reconcile_backfill_triggered") is True
        assert summary.get("reconcile_discrepancy_pct") == pytest.approx(90.0, abs=0.1)
        # State rewound to from_block-1 so the next cycle re-fetches.
        assert indexer.state.load().get("last_block") == 999

    def test_reconcile_within_tolerance_no_rewind(self, indexer: LiteIndexer, tmp_path: Path):
        with patch("lite_indexer.indexer._maintenance.DATA_RECONCILIATION_EVERY_N_CYCLES", 1):
            indexer._ch_writer = FakeWriter(FakeCH(count_rows=[(980,)]))  # 2% short
            indexer._cycles_since_reconcile = 0
            summary: dict[str, Any] = {}
            indexer._maybe_reconcile_range(1000, 1100, expected=1000, summary=summary)
        assert "reconcile_backfill_triggered" not in summary
        assert summary.get("reconcile_discrepancy_pct") == pytest.approx(2.0, abs=0.1)
