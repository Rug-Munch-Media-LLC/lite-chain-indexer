"""Tests for intelligent RPC throttling (lite_indexer/rpc/throttle.py).

Covers:

1. Rate-limit header extraction (X-RateLimit-* / Retry-After, mixed case)
2. RPCRateTracker — 429 Retry-After honored exactly, exponential fallback
3. Pre-emptive throttling when >80% of the advertised budget is consumed
4. Per-method request pacing (eth_getLogs expensive, eth_blockNumber cheap)
5. Disk persistence — state survives a tracker reload (restart)
6. Module-level convenience wrappers

Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from lite_indexer.rpc.throttle import (
    RPCRateTracker,
    extract_rate_limit,
    record_rpc_response,
    throttle_before,
)

# ═══════════════════════════════════════════════════════════════════════
# Header extraction
# ═══════════════════════════════════════════════════════════════════════


class TestExtractRateLimit:
    def test_mixed_case_headers(self):
        info = extract_rate_limit(
            {
                "X-RateLimit-Limit": "300",
                "X-RateLimit-Remaining": "42",
                "X-RateLimit-Reset": "1722800000",
            }
        )
        assert info["limit"] == 300
        assert info["remaining"] == 42
        assert info["reset_at"] == 1722800000.0
        assert info["retry_after"] is None

    def test_httpx_lowercase_headers(self):
        info = extract_rate_limit(
            {
                "x-ratelimit-limit": "100",
                "x-ratelimit-remaining": "100",
                "x-ratelimit-reset": "9999",  # delta form (near future)
            }
        )
        assert info["limit"] == 100
        assert info["remaining"] == 100
        # 9999 < 1_000_000_000 → treated as delta seconds.
        assert info["reset_at"] is not None
        assert info["reset_at"] > time.time()

    def test_retry_after_delta(self):
        info = extract_rate_limit({"Retry-After": "60"})
        assert info["retry_after"] == 60.0

    def test_retry_after_http_date(self):
        import datetime
        import email.utils

        future = email.utils.format_datetime(datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=45))
        info = extract_rate_limit({"retry-after": future})
        assert info["retry_after"] is not None
        assert 40 <= info["retry_after"] <= 50

    def test_absent_headers_all_none(self):
        info = extract_rate_limit({"Content-Type": "application/json"})
        assert info == {"limit": None, "remaining": None, "reset_at": None, "retry_after": None}

    def test_garbage_values_none(self):
        info = extract_rate_limit(
            {"X-RateLimit-Limit": "abc", "X-RateLimit-Remaining": "", "Retry-After": "not-a-number"}
        )
        assert info["limit"] is None
        assert info["remaining"] is None
        assert info["retry_after"] is None


# ═══════════════════════════════════════════════════════════════════════
# RPCRateTracker — 429 handling
# ═══════════════════════════════════════════════════════════════════════


@pytest.fixture()
def tracker(tmp_path: Path) -> RPCRateTracker:
    return RPCRateTracker(path=tmp_path / "throttle.json")


class TestRetryAfter:
    def test_429_retry_after_honored_exactly(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.record_response("https://a.example", "eth_getLogs", 429, {"Retry-After": "30"}, now=now)
        state = tracker.state_for("https://a.example")
        assert state["in_backoff"] is True
        assert state["rate_limit_hits"] == 1
        # Wait = remaining of the 30s window (not the capped exponential).
        assert 29.5 <= tracker.wait_seconds_for("https://a.example", "eth_getLogs", now=now + 0.1) <= 30.0
        assert tracker.allow("https://a.example", now=now + 5) is False
        assert tracker.allow("https://a.example", now=now + 31) is True

    def test_429_without_header_exponential(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.record_response("https://b.example", "eth_getLogs", 429, {}, now=now)
        tracker.record_response("https://b.example", "eth_getLogs", 429, {}, now=now + 1)
        wait = tracker.wait_seconds_for("https://b.example", "eth_getLogs", now=now + 1)
        # 2^hits capped at 300s — two hits → 4s window.
        assert 2.5 <= wait <= 5.0

    def test_success_clears_429_streak(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.record_response("https://c.example", "eth_getLogs", 429, {"Retry-After": "10"}, now=now)
        tracker.record_response("https://c.example", "eth_getLogs", 200, {}, now=now + 11)
        assert tracker.state_for("https://c.example")["rate_limit_hits"] == 0
        assert tracker.allow("https://c.example", now=now + 11) is True


# ═══════════════════════════════════════════════════════════════════════
# Pre-emptive throttling
# ═══════════════════════════════════════════════════════════════════════


class TestPreemptiveThrottle:
    def test_slow_down_above_threshold(self, tracker: RPCRateTracker):
        now = time.time()
        # Budget 100, only 10 left, resets in 60s → 90% consumed.
        tracker.record_response(
            "https://pre.example",
            "eth_getLogs",
            200,
            {
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Remaining": "10",
                "X-RateLimit-Reset": str(int(now) + 60),
            },
            now=now,
        )
        wait = tracker.wait_seconds_for("https://pre.example", "eth_getLogs", now=now)
        # Pace remaining 10 calls across 60s → ~6s per call.
        assert 5.0 <= wait <= 7.0

    def test_no_slow_down_below_threshold(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.record_response(
            "https://low.example",
            "eth_getLogs",
            200,
            {
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Remaining": "90",
                "X-RateLimit-Reset": str(int(now) + 60),
            },
            now=now,
        )
        assert tracker.wait_seconds_for("https://low.example", "eth_getLogs", now=now) == 0.0

    def test_no_headers_means_no_preemptive(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.record_response("https://no-hdr.example", "eth_getLogs", 200, {}, now=now)
        assert tracker.wait_seconds_for("https://no-hdr.example", "eth_getLogs", now=now) == 0.0


# ═══════════════════════════════════════════════════════════════════════
# Per-method pacing
# ═══════════════════════════════════════════════════════════════════════


class TestPerMethodPacing:
    def test_eth_getlogs_paced_at_budget(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.mark_sent("https://m.example", "eth_getLogs", now=now)
        # eth_getLogs budget 3 req/s → 0.33s spacing.
        wait = tracker.wait_seconds_for("https://m.example", "eth_getLogs", now=now)
        assert 0.2 <= wait <= 0.4

    def test_cheap_method_faster(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.mark_sent("https://m.example", "eth_blockNumber", now=now)
        wait_block = tracker.wait_seconds_for("https://m.example", "eth_blockNumber", now=now)
        tracker.mark_sent("https://m.example", "eth_getLogs", now=now)
        wait_logs = tracker.wait_seconds_for("https://m.example", "eth_getLogs", now=now)
        # eth_blockNumber (20/s → 0.05s) must pace tighter than eth_getLogs.
        assert wait_block < wait_logs

    def test_unknown_method_uses_default(self, tracker: RPCRateTracker):
        now = time.time()
        tracker.mark_sent("https://m.example", "debug_traceCustomTxn", now=now)
        assert tracker.wait_seconds_for("https://m.example", "debug_traceCustomTxn", now=now) > 0


# ═══════════════════════════════════════════════════════════════════════
# Persistence (restart safety)
# ═══════════════════════════════════════════════════════════════════════


class TestPersistence:
    def test_state_survives_reload(self, tmp_path: Path):
        path = tmp_path / "throttle.json"
        t1 = RPCRateTracker(path=path)
        now = time.time()
        t1.record_response(
            "https://persist.example",
            "eth_getLogs",
            429,
            {"Retry-After": "120"},
            now=now,
        )
        t1.persist()

        t2 = RPCRateTracker(path=path)
        state = t2.state_for("https://persist.example")
        assert state["in_backoff"] is True
        assert state["rate_limit_hits"] == 1
        assert t2.allow("https://persist.example", now=now + 5) is False
        assert t2.allow("https://persist.example", now=now + 121) is True

    def test_corrupt_file_degrades_gracefully(self, tmp_path: Path):
        path = tmp_path / "throttle.json"
        path.write_text("{not json")
        tracker = RPCRateTracker(path=path)
        assert tracker.snapshot() == {}
        tracker.mark_sent("https://x.example", "eth_getLogs")
        assert tracker.state_for("https://x.example")  # still usable

    def test_persist_if_stale_rate_limits_writes(self, tmp_path: Path):
        path = tmp_path / "throttle.json"
        tracker = RPCRateTracker(path=path, persist_interval=60)
        now = time.time()
        tracker.record_response("https://s.example", "eth_getLogs", 200, {}, now=now)
        assert path.exists()
        mtime = path.stat().st_mtime
        tracker.record_response("https://s.example", "eth_getLogs", 200, {}, now=now + 1)
        assert path.stat().st_mtime == mtime  # no write within interval


# ═══════════════════════════════════════════════════════════════════════
# Module wrappers
# ═══════════════════════════════════════════════════════════════════════


class TestModuleWrappers:
    @pytest.mark.asyncio
    async def test_throttle_before_returns_without_wait_when_open(self):
        from lite_indexer.rpc.throttle import RATE_TRACKER

        RATE_TRACKER.reset()
        await throttle_before("https://open.example", "eth_blockNumber")

    def test_record_response_updates_tracker(self):
        from lite_indexer.rpc.throttle import RATE_TRACKER

        RATE_TRACKER.reset()
        record_rpc_response("https://wrap.example", "eth_getLogs", 429, {"Retry-After": "5"})
        assert RATE_TRACKER.state_for("https://wrap.example")["in_backoff"] is True

    def test_rate_tracker_is_singleton(self):
        import lite_indexer.rpc as pkg
        import lite_indexer.rpc.throttle as throttle_module

        # The rpc package re-export must resolve to the live throttle
        # singleton (which the conftest isolation fixture replaces per test).
        assert pkg.RATE_TRACKER is throttle_module.RATE_TRACKER
