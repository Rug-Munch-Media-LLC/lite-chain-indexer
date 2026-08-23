"""Tests for intelligent RPC routing (lite_indexer/rpc/routing.py).

Covers:

1. Endpoint tier classification (``endpoint_tier``)
2. Per-method preferred tier (``preferred_tier_for_method``) and
   ``select_endpoint_for_method`` re-ranking
3. RpcUsageTracker — bounded per-endpoint cost ledger
4. RpcCircuitBreaker — open / half-open / closed state machine
5. Module-level convenience wrappers

The circuit-breaker tests control ``time.time`` via monkeypatch so the
window/cooldown transitions are deterministic.
"""

from __future__ import annotations

from typing import Any

import time

import pytest

from lite_indexer.rpc.routing import (
    CIRCUIT_BREAKER,
    USAGE_TRACKER,
    RpcCircuitBreaker,
    RpcUsageTracker,
    endpoint_tier,
    is_circuit_open,
    log_rpc_usage,
    preferred_tier_for_method,
    record_rpc_usage,
    select_endpoint_for_method,
)

# ═══════════════════════════════════════════════════════════════════════════
# Endpoint tier classification
# ═══════════════════════════════════════════════════════════════════════════


class TestEndpointTier:
    def test_dedicated_markers(self):
        for url in (
            "https://eth-mainnet.g.alchemy.com/v2/key",
            "https://eth.getblock.io/mainnet/?api_key=k",
            "https://ethereum-mainnet.core.chainstack.com/key",
            "https://nd-123.quicknode.com",
            "https://mainnet.infura.io/v3/key",
            "https://rpc.helius.xyz/?api-key=k",
            "https://bsc.public.blastapi.io",
        ):
            assert endpoint_tier(url) == "dedicated", url

    def test_public_markers(self):
        for url in (
            "https://eth.drpc.org",
            "https://ethereum.publicnode.com",
            "https://base.gateway.tenderly.co",
            "https://bsc-mainnet.gateway.tatum.io",
        ):
            assert endpoint_tier(url) == "public", url

    def test_unknown_domain(self):
        assert endpoint_tier("https://rpc.example.com") == "unknown"
        assert endpoint_tier("https://rpc.flashbots.net") == "unknown"

    def test_tier_marker_matches_subdomain_not_just_host(self):
        # The marker "quicknode" appears mid-hostname.
        assert endpoint_tier("https://nd-abc.quicknode-extra.io") == "dedicated"


class TestPreferredTierForMethod:
    def test_public_methods(self):
        assert preferred_tier_for_method("eth_getLogs") == "public"
        assert preferred_tier_for_method("eth_blockNumber") == "public"
        assert preferred_tier_for_method("eth_getBlockByNumber") == "public"

    def test_dedicated_methods(self):
        assert preferred_tier_for_method("eth_getTransactionReceipt") == "dedicated"
        assert preferred_tier_for_method("eth_call") == "dedicated"

    def test_unknown_method_defaults_public(self):
        assert preferred_tier_for_method("eth_unknown") == "public"


class TestSelectEndpointForMethod:
    """select_endpoint_for_method re-ranks the pool per method tier."""

    DEDICATED = [
        "https://eth-mainnet.g.alchemy.com/v2/key",
        "https://mainnet.infura.io/v3/key",
    ]
    PUBLIC = [
        "https://eth.drpc.org",
        "https://rpc.flashbots.net",
    ]

    def _capturing_select(self, calls: list[list[str]]) -> Any:
        def _select(endpoints: list[str]) -> str:
            calls.append(list(endpoints))
            return endpoints[0]

        return _select

    def test_dedicated_method_filters_to_dedicated(self):
        calls: list[list[str]] = []
        select = self._capturing_select(calls)
        selected = select_endpoint_for_method(
            "eth_call",
            self.DEDICATED + self.PUBLIC,
            chain="ethereum",
            base_select=select,
        )
        assert selected == self.DEDICATED[0]
        # base_select received ONLY the dedicated endpoints.
        assert calls == [self.DEDICATED]

    def test_public_method_excludes_dedicated(self):
        calls: list[list[str]] = []
        select = self._capturing_select(calls)
        selected = select_endpoint_for_method(
            "eth_getLogs",
            self.DEDICATED + self.PUBLIC,
            chain="ethereum",
            base_select=select,
        )
        assert selected == self.PUBLIC[0]
        assert calls == [self.PUBLIC]

    def test_fallback_when_no_matching_tier(self):
        """No dedicated endpoints for a dedicated method → full pool."""
        calls: list[list[str]] = []
        select = self._capturing_select(calls)
        selected = select_endpoint_for_method(
            "eth_call",
            self.PUBLIC,
            chain="ethereum",
            base_select=select,
        )
        assert selected == self.PUBLIC[0]
        # Falls back to the whole pool (filtered list was empty).
        assert calls == [self.PUBLIC]

    def test_default_base_select_is_pool_get_best_endpoint(self):
        # Without base_select it defers to the pool; returns something sane.
        selected = select_endpoint_for_method("eth_getLogs", self.PUBLIC, chain="ethereum")
        assert selected in self.PUBLIC


# ═══════════════════════════════════════════════════════════════════════════
# RpcUsageTracker
# ═══════════════════════════════════════════════════════════════════════════


class TestRpcUsageTracker:
    def test_record_success_and_failure(self):
        tracker = RpcUsageTracker()
        tracker.record("https://a.drpc.org", "eth_blockNumber", ok=True, bytes_received=128)
        tracker.record("https://a.drpc.org", "eth_getLogs", ok=False, error="timeout")
        usage = tracker.usage_for("https://a.drpc.org")
        assert usage is not None
        assert usage.calls == 2
        assert usage.errors == 1
        assert usage.bytes_received == 128
        assert usage.last_error == "timeout"
        assert usage.by_method == {"eth_blockNumber": 1, "eth_getLogs": 1}

    def test_rate_limit_and_error_truncation(self):
        tracker = RpcUsageTracker()
        tracker.record(
            "https://a.drpc.org",
            "eth_getLogs",
            ok=False,
            error="x" * 500,
            rate_limited=True,
        )
        usage = tracker.usage_for("https://a.drpc.org")
        assert usage is not None
        assert usage.rate_limits == 1
        assert len(usage.last_error) == 200  # truncated

    def test_negative_bytes_clamped_to_zero(self):
        tracker = RpcUsageTracker()
        tracker.record("https://a.drpc.org", "eth_blockNumber", ok=True, bytes_received=-5)
        usage = tracker.usage_for("https://a.drpc.org")
        assert usage is not None
        assert usage.bytes_received == 0

    def test_usage_for_missing_returns_none(self):
        assert RpcUsageTracker().usage_for("https://nope.example") is None

    def test_snapshot_shape(self):
        tracker = RpcUsageTracker()
        tracker.record("https://a.drpc.org", "eth_blockNumber", ok=True, bytes_received=10)
        snapshot = tracker.snapshot()
        assert set(snapshot.keys()) == {"https://a.drpc.org"}
        entry = snapshot["https://a.drpc.org"]
        assert set(entry.keys()) == {"calls", "errors", "rate_limits", "bytes", "last_error", "methods"}
        assert entry["calls"] == 1
        assert entry["methods"] == {"eth_blockNumber": 1}

    def test_worst_endpoints_sorted_worst_first(self):
        tracker = RpcUsageTracker()
        tracker.record("https://good.example", "eth_blockNumber", ok=True)
        tracker.record("https://good.example", "eth_blockNumber", ok=True)
        tracker.record("https://bad.example", "eth_blockNumber", ok=False)
        tracker.record("https://bad.example", "eth_blockNumber", ok=False)
        tracker.record("https://mid.example", "eth_blockNumber", ok=False)
        tracker.record("https://mid.example", "eth_blockNumber", ok=True)

        worst = tracker.worst_endpoints(limit=10)
        # error ratios: bad=1.0, mid=0.5, good=0.0
        assert worst[0][0] == "https://bad.example"
        assert worst[0][1] == pytest.approx(1.0)
        assert worst[1][0] == "https://mid.example"
        assert worst[1][1] == pytest.approx(0.5)
        assert worst[2][0] == "https://good.example"
        assert worst[2][1] == pytest.approx(0.0)

    def test_worst_endpoints_limit(self):
        tracker = RpcUsageTracker()
        for i in range(5):
            tracker.record(f"https://ep{i}.example", "eth_blockNumber", ok=False)
        assert len(tracker.worst_endpoints(limit=2)) == 2

    def test_ledger_bounded_evicts_oldest(self):
        tracker = RpcUsageTracker(max_endpoints=2)
        tracker.record("https://first.example", "eth_blockNumber", ok=True)
        tracker.record("https://second.example", "eth_blockNumber", ok=True)
        tracker.record("https://third.example", "eth_blockNumber", ok=True)

        assert tracker.usage_for("https://first.example") is None  # evicted
        assert tracker.usage_for("https://second.example") is not None
        assert tracker.usage_for("https://third.example") is not None

    def test_reset_clears_ledger(self):
        tracker = RpcUsageTracker()
        tracker.record("https://a.example", "eth_blockNumber", ok=True)
        tracker.reset()
        assert tracker.snapshot() == {}

    def test_module_tracker_and_wrappers(self):
        # Convenience wrappers operate on the shared module tracker.
        before = len(USAGE_TRACKER.snapshot())
        try:
            record_rpc_usage("https://module-test.example", "eth_blockNumber", ok=True)
            assert USAGE_TRACKER.usage_for("https://module-test.example") is not None
            snapshot = log_rpc_usage()
            assert "https://module-test.example" in snapshot
        finally:
            USAGE_TRACKER.reset()
        assert before == 0 or before == len(USAGE_TRACKER.snapshot())


# ═══════════════════════════════════════════════════════════════════════════
# RpcCircuitBreaker
# ═══════════════════════════════════════════════════════════════════════════


class _Clock:
    """Mutable fake clock for monkeypatching time.time."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class TestRpcCircuitBreaker:
    @pytest.fixture(autouse=True)
    def _fake_clock(self, monkeypatch: pytest.MonkeyPatch):
        import lite_indexer.rpc.routing as routing_mod

        clock = _Clock()
        monkeypatch.setattr(routing_mod.time, "time", clock)
        self._clock = clock
        return clock

    def test_closed_allows(self):
        breaker = RpcCircuitBreaker(failure_threshold=3)
        assert breaker.state("https://a.example") == "closed"
        assert breaker.allow("https://a.example") is True

    def test_opens_after_threshold_failures(self):
        breaker = RpcCircuitBreaker(failure_threshold=3)
        ep = "https://a.example"
        for _ in range(3):
            breaker.record_failure(ep)
        assert breaker.state(ep) == "open"
        assert breaker.is_open(ep) is True
        assert breaker.allow(ep) is False  # skipped entirely

    def test_below_threshold_stays_closed(self):
        breaker = RpcCircuitBreaker(failure_threshold=5)
        ep = "https://a.example"
        for _ in range(4):
            breaker.record_failure(ep)
        assert breaker.state(ep) == "closed"
        assert breaker.allow(ep) is True

    def test_half_open_after_cooldown(self):
        breaker = RpcCircuitBreaker(failure_threshold=3, cooldown_seconds=30)
        ep = "https://a.example"
        for _ in range(3):
            breaker.record_failure(ep)
        assert breaker.state(ep) == "open"
        self._clock.now += 30  # cooldown elapsed
        assert breaker.state(ep) == "half_open"
        # Still denied in half-open (probe is explicit via allow()).
        assert breaker.is_open(ep) is False

    def test_probe_success_closes_circuit(self):
        breaker = RpcCircuitBreaker(failure_threshold=3, cooldown_seconds=30)
        ep = "https://a.example"
        for _ in range(3):
            breaker.record_failure(ep)
        self._clock.now += 30
        assert breaker.state(ep) == "half_open"
        # Exactly one probe allowed in half-open.
        assert breaker.allow(ep) is True
        breaker.record_success(ep)
        assert breaker.state(ep) == "closed"
        assert breaker.allow(ep) is True

    def test_probe_failure_reopens(self):
        breaker = RpcCircuitBreaker(failure_threshold=3, cooldown_seconds=30)
        ep = "https://a.example"
        for _ in range(3):
            breaker.record_failure(ep)
        self._clock.now += 30
        assert breaker.allow(ep) is True  # the probe
        breaker.record_failure(ep)  # probe fails → re-open
        assert breaker.state(ep) == "open"
        assert breaker.allow(ep) is False

    def test_success_resets_failure_count(self):
        breaker = RpcCircuitBreaker(failure_threshold=3)
        ep = "https://a.example"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        breaker.record_success(ep)
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        # 2 failures after the reset < threshold 3 → still closed.
        assert breaker.state(ep) == "closed"

    def test_old_failures_expire_outside_window(self):
        breaker = RpcCircuitBreaker(failure_threshold=3, window_seconds=60)
        ep = "https://a.example"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        self._clock.now += 61  # failures now outside the window
        breaker.record_failure(ep)
        # Only 1 failure within the window < threshold → closed.
        assert breaker.state(ep) == "closed"
        assert breaker.allow(ep) is True

    def test_shared_module_breaker(self, monkeypatch: pytest.MonkeyPatch):
        import lite_indexer.rpc.routing as routing_mod

        # Use a fresh instance to avoid polluting the shared state.
        monkeypatch.setattr(routing_mod, "CIRCUIT_BREAKER", RpcCircuitBreaker(failure_threshold=2))
        ep = "https://shared.example"
        routing_mod.CIRCUIT_BREAKER.record_failure(ep)
        routing_mod.CIRCUIT_BREAKER.record_failure(ep)
        assert is_circuit_open(ep) is True


def test_module_breaker_not_dirty():
    """The shared breaker should not leak state across test modules."""
    assert CIRCUIT_BREAKER.state("https://unused.example") == "closed"


# ── Success-rate damping in the P2C score (2026-08-23) ────────────────


class TestErrorRateDamping:
    """damped_score() multiplies by (1 + ERROR_RATE_DAMPING * error_rate)."""

    def test_zero_errors_unchanged(self):
        from lite_indexer.rpc.routing import EndpointLatency

        e = EndpointLatency(endpoint="https://a")
        e.ewma_ms = 100.0
        assert e.damped_score() == pytest.approx(100.0)

    def test_error_rate_damps_score(self, monkeypatch: pytest.MonkeyPatch):
        from lite_indexer.rpc import routing as rm

        monkeypatch.setattr(rm, "ERROR_RATE_DAMPING", 2.0)
        e = rm.EndpointLatency(endpoint="https://b")
        e.ewma_ms = 100.0
        now = time.time()
        # 7 ok / 3 errors in window → error_rate 0.3 → factor 1.6.
        e._outcomes = [(now - i, i < 7) for i in range(10)]
        assert e.error_rate(now) == pytest.approx(0.3)
        assert e.damped_score() == pytest.approx(160.0)

    def test_flaky_endpoint_loses_p2c(self, monkeypatch: pytest.MonkeyPatch):
        """Equal latency; the flaky endpoint must lose the P2C draw."""
        from lite_indexer.rpc import routing as rm

        good, flaky = "https://good", "https://flaky"
        for ep in (good, flaky):
            st = rm._stats(ep)
            st.ewma_ms = 50.0
        now = time.time()
        rm._stats(flaky)._outcomes = [(now - i, False) for i in range(10)]  # 100% errors

        # Deterministic draw of both candidates — flaky always loses.
        monkeypatch.setattr(rm.random, "sample", lambda pop, k: [good, flaky])
        for _ in range(20):
            assert rm.select_p2c([good, flaky]) == good

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch):
        import importlib

        monkeypatch.setenv("RPC_ERROR_RATE_DAMPING", "5.0")
        from lite_indexer.rpc import routing as rm

        importlib.reload(rm)
        assert rm.ERROR_RATE_DAMPING == 5.0
        monkeypatch.setattr(rm, "ERROR_RATE_DAMPING", 2.0)  # restore for others
