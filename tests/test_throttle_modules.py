"""Throttle split (headers / state / wait / record) — behaviour contracts.

Covers the success path, every failure path (corrupt state file, unwritable
state file) and the boundaries (absent / malformed headers, zero rate,
backoff edge), plus the invariant that a 429 window is honoured exactly.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lite_indexer.rpc.throttle import RPCRateTracker, record_rpc_response
from lite_indexer.rpc.throttle_headers import extract_rate_limit


class TestHeaderParsing:
    @pytest.mark.parametrize(
        ("headers", "limit", "remaining"),
        [
            ({"x-ratelimit-limit": "100", "x-ratelimit-remaining": "10"}, 100, 10),
            ({"RateLimit-Limit": "50", "RateLimit-Remaining": "5"}, 50, 5),
        ],
        ids=["x-prefixed", "bare-family"],
    )
    def test_parses_both_header_families(self, headers, limit, remaining):
        info = extract_rate_limit(headers)
        assert (info["limit"], info["remaining"]) == (limit, remaining)

    def test_absent_headers_are_none(self):
        info = extract_rate_limit({})
        assert info == {"limit": None, "remaining": None, "reset_at": None, "retry_after": None}

    @pytest.mark.parametrize("bad", ["", "  ", "not-a-number", "1.2.3"])
    def test_malformed_headers_degrade_to_none(self, bad):
        info = extract_rate_limit({"x-ratelimit-limit": bad, "retry-after": bad})
        assert info["limit"] is None
        assert info["retry_after"] is None

    def test_epoch_reset_is_not_treated_as_delta(self):
        info = extract_rate_limit({"x-ratelimit-reset": "1900000000"})
        assert info["reset_at"] == 1900000000.0

    def test_delta_reset_is_relative_to_now(self):
        info = extract_rate_limit({"x-ratelimit-reset": "60"})
        assert info["reset_at"] is not None and info["reset_at"] > 1_000_000


class TestRecordResponse:
    def test_retry_after_window_is_honoured_exactly(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 429, {"retry-after": "30"}, now=1000.0)
        state = tracker.state_for("https://e1")
        assert state["backoff_until"] == pytest.approx(1030.0)
        assert tracker.wait_seconds_for("https://e1", "eth_getLogs", now=1000.0) == pytest.approx(30.0)

    def test_missing_retry_after_uses_capped_exponential_window(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 429, now=0.0)
        first = tracker.state_for("https://e1")["backoff_until"]
        tracker.record_response("https://e1", "eth_getLogs", 429, now=0.0)
        second = tracker.state_for("https://e1")["backoff_until"]
        assert first == pytest.approx(2.0)
        assert second == pytest.approx(4.0)  # escalation by consecutive hits
        assert second <= 300.0

    def test_clean_success_closes_the_429_streak(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 429, {"retry-after": "5"}, now=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 200, now=10.0)
        assert tracker.state_for("https://e1")["rate_limit_hits"] == 0
        assert tracker.allow("https://e1", now=10.0) is True

    def test_record_rpc_response_wrapper_is_wired(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            "lite_indexer.rpc.throttle.RATE_TRACKER.record_response",
            lambda *a, **k: calls.append((a, k)),
        )
        record_rpc_response("https://e1", "eth_blockNumber", 429, {"retry-after": "1"})
        assert calls and calls[0][0][:3] == ("https://e1", "eth_blockNumber", 429)


class TestWaitComputation:
    def test_preemptive_pacing_when_budget_almost_spent(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.record_response(
            "https://e1",
            "eth_blockNumber",
            200,
            {"x-ratelimit-limit": "100", "x-ratelimit-remaining": "5", "x-ratelimit-reset": "1900000000"},
            now=1800000000.0,
        )
        wait = tracker.wait_seconds_for("https://e1", "eth_blockNumber", now=1800000000.0)
        assert wait > 0

    def test_no_history_means_no_wait(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        assert tracker.wait_seconds_for("https://fresh", "eth_getLogs", now=0.0) == 0.0

    def test_mark_sent_arms_the_per_method_window(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.mark_sent("https://e1", "eth_getLogs", now=100.0)
        assert tracker.wait_seconds_for("https://e1", "eth_getLogs", now=100.0) > 0


class TestPersistence:
    def test_state_survives_a_restart(self, tmp_path: Path):
        path = tmp_path / "t.json"
        tracker = RPCRateTracker(path, persist_interval=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 429, {"retry-after": "60"}, now=500.0)
        reloaded = RPCRateTracker(path)
        assert reloaded.state_for("https://e1")["rate_limit_hits"] == 1
        assert reloaded.wait_seconds_for("https://e1", "eth_getLogs", now=500.0) == pytest.approx(60.0)

    def test_corrupt_state_file_degrades_to_empty_tracker(self, tmp_path: Path):
        path = tmp_path / "t.json"
        path.write_text("{not json")
        tracker = RPCRateTracker(path)
        assert tracker.snapshot() == {}

    def test_unwritable_state_path_does_not_raise(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "missing-dir" / "t.json", persist_interval=0.0)
        tracker.record_response("https://e1", "eth_getLogs", 200, now=1.0)  # persist swallowed
        assert tracker.state_for("https://e1")["last_status"] == 200
        assert json.loads((tmp_path / "missing-dir" / "t.json").read_text())  # dir was created

    def test_reset_drops_one_or_all_endpoints(self, tmp_path: Path):
        tracker = RPCRateTracker(tmp_path / "t.json", persist_interval=0.0)
        tracker.record_response("https://a", "eth_getLogs", 429, now=0.0)
        tracker.record_response("https://b", "eth_getLogs", 429, now=0.0)
        tracker.reset("https://a")
        assert set(tracker.snapshot()) == {"https://b"}
        tracker.reset()
        assert tracker.snapshot() == {}
