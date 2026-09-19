"""Tests for RPC pool exhaustion recovery and self-healing.

Covers the 2026-08-05 resilience hardening:

1. Pool-wide outage → desperation probe (least-banned endpoint probed at
   most once per interval, chain does NOT stall / crash)
2. Endpoint recovery → circuit closes, chain resumes
3. Single endpoint failure → other endpoints take over
3b. 429 rate limits → backoff only, never trip the circuit / hard-ban
4. Adaptive batch/chunk-size reduction on repeated failures
5. Atomic state-file writes (unique tmp + flock + os.replace)

Hermetic — no network, no ClickHouse. All timing is controlled with a
fake clock or patched intervals.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.rpc.pool import (
    ban_endpoint,
    desperation_probe_due,
    force_probe_endpoint,
    get_best_endpoint,
    is_endpoint_banned,
    is_endpoint_hard_banned,
    is_endpoint_soft_banned,
    least_banned_endpoint,
    mark_desperation_probe,
    pool_fully_down,
    pool_rate_limited,
    probe_eligible_endpoints,
    record_endpoint_failure,
    record_endpoint_success,
    record_rate_limit,
    unban_endpoint,
)
from lite_indexer.rpc.routing import RpcCircuitBreaker


class _Clock:
    """Mutable fake clock for monkeypatching time.time."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock(monkeypatch: pytest.MonkeyPatch):
    """Fake clock shared across pool + routing modules."""
    fake = _Clock()
    import lite_indexer.rpc.pool as pool_mod
    import lite_indexer.rpc.routing as routing_mod

    monkeypatch.setattr(pool_mod.time, "time", fake)
    monkeypatch.setattr(routing_mod.time, "time", fake)
    return fake


@pytest.fixture(autouse=True)
def _clean_pool(_mp: pytest.MonkeyPatch, monkeypatch: pytest.MonkeyPatch):
    """Reset pool state between tests so bans/failures never leak."""
    import lite_indexer.rpc.ban_state as ban_state_mod
    import lite_indexer.rpc.pool as pool_mod
    import lite_indexer.rpc.routing as routing_mod

    monkeypatch.setattr(pool_mod, "_endpoint_failures", {})
    _mp.setattr(ban_state_mod, "_endpoint_bans", {})
    _mp.setattr(ban_state_mod, "_endpoint_ban_level", {})
    _mp.setattr(ban_state_mod, "_endpoint_hard_bans", set())
    _mp.setattr(ban_state_mod, "_endpoint_429_streaks", {})
    monkeypatch.setattr(routing_mod, "CIRCUIT_BREAKER", RpcCircuitBreaker())
    yield


# ═══════════════════════════════════════════════════════════════════════
# 1. Pool-wide outage → desperation probe, chain does not stall
# ═══════════════════════════════════════════════════════════════════════


class TestPoolWideOutage:
    def test_soft_banned_pool_is_rate_limited_not_down(self, clock: _Clock):
        """429-streak soft-bans are transient: an all-soft-banned pool is
        rate-limited, NOT down — the base/celo catch-up deadlock was caused
        by counting it as down and stopping the fetch path entirely."""
        a, b = "https://a.invalid", "https://b.invalid"
        ban_endpoint(a, reason="http_429")
        ban_endpoint(b, reason="http_429")
        assert is_endpoint_banned(a) is True
        assert is_endpoint_soft_banned(a) is True
        assert is_endpoint_hard_banned(a) is False
        assert pool_fully_down([a, b]) is False
        assert pool_rate_limited([a, b]) is True

    def test_hard_banned_pool_is_fully_down(self, clock: _Clock):
        """A provider blacklist (HTTP 403) never self-recovers by probing:
        an all-hard-banned pool IS down."""
        a, b = "https://a.invalid", "https://b.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(a, reason="http_403")
            ban_endpoint(b, reason="http_403")
        assert is_endpoint_hard_banned(a) is True
        assert pool_fully_down([a, b]) is True
        assert pool_rate_limited([a, b]) is False

    def test_mixed_hard_and_soft_is_rate_limited(self, clock: _Clock):
        """One hard-banned, one soft-banned: the soft endpoint is still
        retryable, so the pool is rate-limited, not down."""
        hard, soft = "https://hard.invalid", "https://soft.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(hard, reason="http_403")
            ban_endpoint(soft, reason="http_429")
        assert pool_fully_down([hard, soft]) is False
        assert pool_rate_limited([hard, soft]) is True

    def test_pool_fully_down_false_when_one_healthy(self, clock: _Clock):
        a, b = "https://a.invalid", "https://b.invalid"
        ban_endpoint(a, reason="http_429")
        assert pool_fully_down([a, b]) is False
        assert pool_rate_limited([a, b]) is False

    def test_pool_rate_limited_false_when_empty(self):
        assert pool_rate_limited([]) is False

    def test_pool_fully_down_empty_pool_is_false(self):
        assert pool_fully_down([]) is False

    def test_pool_fully_down_true_with_circuit_open(self, clock: _Clock):
        a, b = "https://a.invalid", "https://b.invalid"
        breaker = RpcCircuitBreaker(failure_threshold=2)
        breaker.record_failure(a)
        breaker.record_failure(a)
        breaker.record_failure(b)
        breaker.record_failure(b)
        with patch("lite_indexer.rpc.routing.is_circuit_open", lambda ep: ep in (a, b)):
            assert pool_fully_down([a, b]) is True

    def test_rate_limited_pool_fetch_attempts_and_advances(self, _mp: pytest.MonkeyPatch, clock: _Clock, monkeypatch: pytest.MonkeyPatch):
        """End-to-end: an all-soft-banned pool must NOT raise the
        "waiting for desperation probe" deadlock — it attempts the least-
        penalized endpoint and returns the (empty) log set."""

        import asyncio

        import httpx

        from lite_indexer.strategies.evm import transfer_handler as th

        a, b = "https://a.invalid", "https://b.invalid"
        for _ in range(3):
            record_rate_limit(a)
            record_rate_limit(b)
        assert pool_rate_limited([a, b]) is True

        async def _no_backoff(url: str) -> None:
            return None

        class _Slot:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *exc: object) -> None:
                return None

        _mp.setattr(th, "apply_endpoint_backoff", _no_backoff)
        _mp.setattr(th, "provider_slot", lambda _key: _Slot())

        def _handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": []})

        async def _run() -> list[dict[str, Any]]:
            async with httpx.AsyncClient(transport=httpx.MockTransport(_handler)) as client:
                return await th._fetch_range("celo", 1, 5, client=client, topic="0xtopic", endpoints=[a, b])

        assert asyncio.run(_run()) == []

    def test_rate_limited_rotation_spreads_across_calls(self, _mp: pytest.MonkeyPatch, clock: _Clock, monkeypatch: pytest.MonkeyPatch):
        """The crawl must rotate the starting endpoint so no soft-banned
        endpoint is pinned and re-banned every cycle."""
        from lite_indexer.strategies.evm import range_planning as rp

        _mp.setattr(rp, "_RATE_LIMITED_CURSOR", {})
        eps = ["https://a.invalid", "https://b.invalid", "https://c.invalid"]
        first = rp._rate_limited_rotation("celo", eps)
        second = rp._rate_limited_rotation("celo", eps)
        third = rp._rate_limited_rotation("celo", eps)
        assert first == eps
        assert second == eps[1:] + eps[:1]
        assert third == eps[2:] + eps[:2]
        assert {first[0], second[0], third[0]} == set(eps)  # progress, not pinning

    def test_prefer_wide_range_orders_high_caps_first(self, _mp: pytest.MonkeyPatch, clock: _Clock, monkeypatch: pytest.MonkeyPatch):
        """Catch-up must dial the 100-range endpoints before the 10-range
        ones so chunks are not throttled to the narrowest provider cap."""
        from lite_indexer.strategies.evm import range_planning as rp

        narrow, wide = "https://narrow.invalid", "https://wide.invalid"
        _mp.setattr(rp, "get_max_block_range", lambda ep: 10 if ep == narrow else 100)
        assert rp._prefer_wide_range([narrow, wide], 100) == [wide, narrow]
        assert rp._prefer_wide_range([narrow, wide], 10) == [narrow, wide]

    def test_get_best_endpoint_returns_least_banned_not_stall(self, clock: _Clock):
        """All banned → must return an endpoint (no crash), and it must be
        the least-banned one rather than blindly endpoints[0]."""
        banned_hard = "https://hard.invalid"
        banned_soft = "https://soft.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 60):
            ban_endpoint(banned_hard, reason="http_429")
            ban_endpoint(banned_soft, reason="http_429")
        # soft has a shorter ban window (banned later) → least-banned.
        selected = get_best_endpoint([banned_hard, banned_soft])
        assert selected in (banned_hard, banned_soft)  # no crash
        assert selected == least_banned_endpoint([banned_hard, banned_soft])

    def test_get_best_endpoint_all_down_does_not_clear_failures(self, clock: _Clock):
        """The old stall bug cleared all failure state every cycle, so the
        first endpoint was re-hammered forever. The fix must NOT clear."""
        a, b = "https://a.invalid", "https://b.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(a, reason="http_429")
            ban_endpoint(b, reason="http_429")
        get_best_endpoint([a, b])
        assert is_endpoint_banned(a) is True
        assert is_endpoint_banned(b) is True

    def test_desperation_probe_gated_by_interval(self, clock: _Clock):
        breaker = RpcCircuitBreaker(desperation_probe_interval=30.0)
        assert breaker.desperation_probe_due("ethereum") is True  # never probed
        breaker.mark_desperation_probe("ethereum")
        assert breaker.desperation_probe_due("ethereum") is False
        clock.advance(29.0)
        assert breaker.desperation_probe_due("ethereum") is False
        clock.advance(1.1)
        assert breaker.desperation_probe_due("ethereum") is True

    def test_mark_desperation_probe_pool_wrapper(self, clock: _Clock):
        eps = ["https://a.invalid"]
        assert desperation_probe_due(eps) is True
        mark_desperation_probe(eps)
        assert desperation_probe_due(eps) is False

    def test_least_banned_prefers_closed_over_open(self, clock: _Clock):
        breaker = RpcCircuitBreaker(failure_threshold=2)
        open_ep = "https://open.invalid"
        closed_ep = "https://closed.invalid"
        breaker.record_failure(open_ep)
        breaker.record_failure(open_ep)
        assert breaker.least_banned_endpoint([open_ep, closed_ep]) == closed_ep

    def test_least_banned_empty_raises(self):
        breaker = RpcCircuitBreaker()
        with pytest.raises(ValueError):
            breaker.least_banned_endpoint([])

    # ── 2026-09-19 deadlock fix: the probe must not dial doomed endpoints ──

    def test_probe_eligible_excludes_hard_banned(self, clock: _Clock):
        """A hard-banned endpoint is blacklisted by the provider; probing it
        fails and extends its own ban (the self-sustaining deadlock)."""
        banned = "https://banned.invalid"
        open_ok = "https://open.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(banned, reason="http_403")
        with patch("lite_indexer.rpc.routing.is_circuit_open", lambda ep: ep == open_ok):
            eligible = probe_eligible_endpoints([banned, open_ok])
        assert eligible == [open_ok]

    def test_probe_eligible_excludes_logs_incapable(self, clock: _Clock):
        """A capability-excluded endpoint structurally cannot serve getLogs —
        the probe must never pick it."""
        incapable = "https://incapable.invalid"
        good = "https://good.invalid"
        with patch(
            "lite_indexer.rpc.capabilities.is_logs_capability_excluded",
            lambda ep: ep == incapable,
        ):
            assert probe_eligible_endpoints([incapable, good]) == [good]

    def test_probe_eligible_all_doomed_is_empty(self, clock: _Clock):
        """Every endpoint hard-banned → nothing to probe; caller waits out
        the ban instead of deepening it."""
        a, b = "https://a.invalid", "https://b.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(a, reason="http_403")
            ban_endpoint(b, reason="http_403")
        assert probe_eligible_endpoints([a, b]) == []

    def test_probe_eligible_includes_rate_limit_softban(self, clock: _Clock):
        """A 429-streak soft-ban is transient: the probe MUST be allowed to
        re-test it, else an all-soft-banned pool (celo, 2026-09-19) can never
        self-recover."""
        ep = "https://softbanned.invalid"
        for _ in range(3):  # streak >= threshold arms the soft-ban
            record_rate_limit(ep)
        assert is_endpoint_banned(ep) is True
        assert probe_eligible_endpoints([ep]) == [ep]

    def test_probe_eligible_readmits_stale_capability_exclusion(self):
        """A ``logs_capable=False`` verdict is local, not a provider ban: once
        stale it MUST re-enter the probe set, else a chain whose whole log
        pool is capability-excluded deadlocks forever."""
        import time as _time

        from lite_indexer.rpc import capabilities as caps

        stale = "https://stale-incapable.invalid"
        fresh = "https://fresh-incapable.invalid"
        records = {
            stale: caps.CapabilityRecord(
                endpoint=stale,
                chain="celo",
                logs_capable=False,
                probed_at=_time.time() - caps.CAPABILITY_REPROBE_INTERVAL - 1,
            ),
            fresh: caps.CapabilityRecord(
                endpoint=fresh,
                chain="celo",
                logs_capable=False,
                probed_at=_time.time(),
            ),
        }
        with patch.dict(caps._CAPABILITY_RECORDS, records, clear=True):
            assert probe_eligible_endpoints([stale, fresh]) == [stale]

    def test_probe_eligible_all_stale_excluded_not_empty(self):
        """Bounded-time recovery: an all-capability-excluded pool with stale
        verdicts yields probe candidates instead of an empty set + hard raise."""
        import time as _time

        from lite_indexer.rpc import capabilities as caps

        eps = ["https://s1.invalid", "https://s2.invalid"]
        records = {
            ep: caps.CapabilityRecord(
                endpoint=ep,
                chain="celo",
                logs_capable=False,
                probed_at=_time.time() - caps.CAPABILITY_REPROBE_INTERVAL - 1,
            )
            for ep in eps
        }
        with patch.dict(caps._CAPABILITY_RECORDS, records, clear=True):
            assert probe_eligible_endpoints(eps) == eps

    def test_probe_eligible_fresh_capability_exclusion_stays_out(self):
        """A FRESH capability exclusion is still skipped (no wasted probes)."""
        import time as _time

        from lite_indexer.rpc import capabilities as caps

        fresh = "https://fresh-incapable.invalid"
        records = {
            fresh: caps.CapabilityRecord(
                endpoint=fresh,
                chain="celo",
                logs_capable=False,
                probed_at=_time.time(),
            )
        }
        with patch.dict(caps._CAPABILITY_RECORDS, records, clear=True):
            assert probe_eligible_endpoints([fresh]) == []

    def test_rate_limit_streak_softban_capped_at_300s(self, clock: _Clock):
        """Was capped at 3600s → a transient 429 burst froze the chain for an
        hour past provider recovery. Cap must be 300s at every streak."""
        ep = "https://streaky.invalid"
        for _ in range(6):
            record_rate_limit(ep)
        import lite_indexer.rpc.ban_state as ban_state_mod

        ban_s = ban_state_mod._endpoint_bans[ep] - clock.now
        assert ban_s == pytest.approx(300.0)


# ═══════════════════════════════════════════════════════════════════════
# 2. Endpoint recovery → circuit closes, chain resumes
# ═══════════════════════════════════════════════════════════════════════


class TestEndpointRecovery:
    def test_force_probe_bypasses_open_circuit(self, clock: _Clock):
        breaker = RpcCircuitBreaker(failure_threshold=2)
        ep = "https://recover.invalid"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        assert breaker.is_open(ep) is True
        assert breaker.allow(ep) is False  # normally blocked
        assert breaker.force_probe(ep) is True  # desperation bypass

    def test_force_probe_success_closes_circuit(self, clock: _Clock):
        breaker = RpcCircuitBreaker(failure_threshold=2)
        ep = "https://recover.invalid"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        breaker.force_probe(ep)
        breaker.record_success(ep)
        assert breaker.state(ep) == "closed"
        assert breaker.allow(ep) is True

    def test_force_probe_failure_reopens_circuit(self, clock: _Clock):
        breaker = RpcCircuitBreaker(failure_threshold=2)
        ep = "https://recover.invalid"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        breaker.force_probe(ep)
        breaker.record_failure(ep)  # probe failed
        assert breaker.is_open(ep) is True

    def test_force_probe_endpoint_pool_wrapper(self, clock: _Clock):
        """pool.force_probe_endpoint bypasses the shared breaker for one call."""
        ep = "https://wrapped.invalid"
        breaker = RpcCircuitBreaker(failure_threshold=2)
        with patch("lite_indexer.rpc.routing.CIRCUIT_BREAKER", breaker):
            breaker.record_failure(ep)
            breaker.record_failure(ep)
            assert force_probe_endpoint(ep) is True
            assert breaker.state(ep) == "probe"

    def test_record_success_unbans_and_closes(self, clock: _Clock):
        ep = "https://recover.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(ep, reason="http_429")
        record_endpoint_success(ep)
        assert is_endpoint_banned(ep) is False

    def test_half_open_after_cooldown_still_probes(self, clock: _Clock):
        """Existing half-open behavior must keep working alongside force_probe."""
        breaker = RpcCircuitBreaker(failure_threshold=2, cooldown_seconds=30)
        ep = "https://recover.invalid"
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        assert breaker.state(ep) == "open"
        clock.advance(30)
        assert breaker.state(ep) == "half_open"
        assert breaker.allow(ep) is True


# ═══════════════════════════════════════════════════════════════════════
# 3. Single endpoint failure → other endpoints take over
# ═══════════════════════════════════════════════════════════════════════


class TestSingleEndpointFailure:
    def test_best_endpoint_skips_single_banned(self, clock: _Clock):
        healthy = "https://healthy.invalid"
        banned = "https://banned.invalid"
        with patch("lite_indexer.config.ENDPOINT_BAN_BASE_SECONDS", 3600):
            ban_endpoint(banned, reason="http_429")
        for _ in range(20):
            assert get_best_endpoint([healthy, banned]) == healthy

    def test_best_endpoint_skips_tip_failed_until_recovery(self, clock: _Clock):
        """A tip-incapable endpoint must not hold the tip poll."""
        from lite_indexer.rpc.routing import (
            record_endpoint_block_height,
            record_endpoint_tip_failure,
        )

        healthy = "https://healthy-tip.invalid"
        stalled = "https://stalled-tip.invalid"
        record_endpoint_block_height(healthy, 1000)
        record_endpoint_tip_failure(stalled)
        record_endpoint_tip_failure(stalled)  # threshold reached
        for _ in range(20):
            assert get_best_endpoint([healthy, stalled]) == healthy

        record_endpoint_block_height(stalled, 1000)  # recovers
        assert get_best_endpoint([healthy, stalled]) in {healthy, stalled}

    def test_best_endpoint_skips_single_circuit_open(self, clock: _Clock):
        healthy = "https://healthy.invalid"
        broken = "https://broken.invalid"
        breaker = RpcCircuitBreaker(failure_threshold=2)
        breaker.record_failure(broken)
        breaker.record_failure(broken)
        with patch("lite_indexer.rpc.routing.is_circuit_open", lambda ep: ep == broken):
            for _ in range(20):
                assert get_best_endpoint([healthy, broken]) == healthy

    def test_single_endpoint_failure_records_breaker_failure(self, clock: _Clock):
        ep = "https://single.invalid"
        # Shared breaker uses the configured threshold (10 since 2026-08-07;
        # raised from 5 so transient throttling does not trip it).
        for _ in range(10):
            record_endpoint_failure(ep)
        from lite_indexer.rpc.routing import CIRCUIT_BREAKER

        assert CIRCUIT_BREAKER.is_open(ep) is True


# ═══════════════════════════════════════════════════════════════════════
# 3b. 429 rate limits → backoff only, never trip the circuit / hard-ban
# ═══════════════════════════════════════════════════════════════════════


class TestRateLimitDoesNotTripCircuit:
    def test_rate_limits_never_open_circuit(self, clock: _Clock):
        """429s never trip the BREAKER (that is for genuine failures).

        Sustained consecutive 429s DO escalate to a soft-ban window since
        2026-08-24 (production incident: a dying provider absorbed an
        unbounded retry storm because 429s could never ban). The breaker
        itself stays closed — bans are the escalation path, not circuits.
        """
        ep = "https://ratelimited.invalid"
        from lite_indexer.rpc.routing import CIRCUIT_BREAKER

        try:
            for _ in range(50):
                record_rate_limit(ep)
            assert CIRCUIT_BREAKER.is_open(ep) is False
            # Streak far past threshold → soft-banned (capped at 1h).
            assert is_endpoint_banned(ep) is True
        finally:
            unban_endpoint(ep)

    def test_transient_429s_do_not_ban(self, clock: _Clock):
        """1-2 consecutive 429s = ordinary throttling: backoff only."""
        ep = "https://transient.invalid"
        record_rate_limit(ep)
        record_rate_limit(ep)
        assert is_endpoint_banned(ep) is False

    def test_sustained_429_streak_escalates_to_softban(self, clock: _Clock):
        """Streak >= 3 → escalating soft-ban: 5min, then doubling, cap 1h.

        This is the anti-ban guard: parallel chunk workers must stop
        aggregating into a permanent 429 storm against a dying provider.
        """
        ep = "https://streaky.invalid"
        try:
            record_rate_limit(ep)
            record_rate_limit(ep)
            assert is_endpoint_banned(ep) is False  # still transient
            record_rate_limit(ep)  # streak 3 → first soft-ban
            assert is_endpoint_banned(ep) is True
        finally:
            unban_endpoint(ep)

    def test_success_clears_429_streak(self, clock: _Clock):
        """A success anywhere resets the streak — recovery is honored."""
        ep = "https://recovering.invalid"
        try:
            for _ in range(3):
                record_rate_limit(ep)
            assert is_endpoint_banned(ep) is True
            record_endpoint_success(ep)
            assert is_endpoint_banned(ep) is False
            # And the slate is clean: two more 429s stay transient.
            record_rate_limit(ep)
            record_rate_limit(ep)
            assert is_endpoint_banned(ep) is False
        finally:
            unban_endpoint(ep)

    def test_rate_limit_still_deprioritizes_endpoint(self, clock: _Clock):
        """Recent 429s must still add pool penalty (backoff) for selection."""
        healthy = "https://healthy.invalid"
        throttled = "https://throttled.invalid"
        # 4 recent 429s → penalty 4*1000 + 1 = 4001 >= MAX_FAILURES*1000
        # (3000) → excluded from the healthy random-choice pool entirely.
        for _ in range(4):
            record_rate_limit(throttled)
        for _ in range(20):
            assert get_best_endpoint([healthy, throttled]) == healthy

    def test_genuine_failures_still_trip_circuit(self, clock: _Clock):
        """Real failures must keep tripping the breaker (unchanged behavior)."""
        ep = "https://genuine-fail.invalid"
        breaker = RpcCircuitBreaker(failure_threshold=3, window_seconds=120)
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        breaker.record_failure(ep)
        assert breaker.state(ep) == "open"

    def test_rate_limits_do_not_count_toward_failure_threshold(self, clock: _Clock):
        """record_rate_limit never feeds the breaker — only genuine failures do."""
        ep = "https://mixed.invalid"
        for _ in range(30):
            record_rate_limit(ep)
        from lite_indexer.rpc.routing import CIRCUIT_BREAKER

        # 30 rate limits: no failure recorded on the breaker.
        assert CIRCUIT_BREAKER.state(ep) == "closed"


# ═══════════════════════════════════════════════════════════════════════
# 4. Adaptive batch/chunk-size reduction on repeated failures
# ═══════════════════════════════════════════════════════════════════════


class TestAdaptiveChunkReduction:
    def test_initial_chunk_is_default(self):
        from lite_indexer.strategies.evm.range_planning import (
            _adaptive_chunk_size,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("ethereum")
        assert _adaptive_chunk_size("ethereum", 50) == 50

    def test_failure_halves_chunk(self):
        from lite_indexer.strategies.evm.range_planning import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("base")
        _bump_adaptive_chunk("base")
        assert _adaptive_chunk_size("base", 50) == 25

    def test_repeated_failures_halve_twice(self):
        from lite_indexer.strategies.evm.range_planning import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("bsc")
        _bump_adaptive_chunk("bsc")
        _bump_adaptive_chunk("bsc")
        assert _adaptive_chunk_size("bsc", 50) == 12

    def test_chunk_never_below_floor(self):
        from lite_indexer.strategies.evm.range_planning import (
            ADAPTIVE_CHUNK_FLOOR,
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("polygon")
        for _ in range(10):
            _bump_adaptive_chunk("polygon")
        assert _adaptive_chunk_size("polygon", 50) == ADAPTIVE_CHUNK_FLOOR

    def test_success_resets_chunk(self):
        from lite_indexer.strategies.evm.range_planning import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("arbitrum")
        _bump_adaptive_chunk("arbitrum")
        assert _adaptive_chunk_size("arbitrum", 50) == 25
        _reset_adaptive_chunk("arbitrum")
        assert _adaptive_chunk_size("arbitrum", 50) == 50

    def test_adaptive_level_persists_across_restart(self):
        """The watchdog restarts the daemon mid-adaptation; a process-local
        dict was wiped every restart so base never converged to 5-block
        chunks. The level must survive on disk (conftest isolates the path)."""
        import lite_indexer.strategies.evm.range_planning as rp

        rp._reset_adaptive_chunk("base")
        for _ in range(4):
            rp._bump_adaptive_chunk("base")
        assert rp._adaptive_chunk_size("base", 50) == rp.ADAPTIVE_CHUNK_FLOOR
        # Simulate a fresh process: forget memory, keep the file.
        rp._ADAPTIVE_CHUNK_LEVELS.clear()
        rp._ADAPTIVE_CHUNK_STREAKS.clear()
        rp._ADAPTIVE_CHUNK_LOADED.clear()
        assert rp._adaptive_chunk_size("base", 50) == rp.ADAPTIVE_CHUNK_FLOOR
        rp._reset_adaptive_chunk("base")

    def test_success_decays_one_level_not_full_reset(self):
        """A single success must not snap back to 50 blocks (base
        oscillation); decay is one step after a sustained streak."""
        import lite_indexer.strategies.evm.range_planning as rp

        rp._reset_adaptive_chunk("base")
        rp._bump_adaptive_chunk("base")
        rp._bump_adaptive_chunk("base")
        assert rp._adaptive_chunk_size("base", 50) == 12
        for _ in range(rp.ADAPTIVE_RECOVERY_SUCCESSES):
            rp._record_adaptive_success("base")
        assert rp._adaptive_chunk_size("base", 50) == 25  # one level, not 50
        rp._reset_adaptive_chunk("base")

    def test_chains_independent(self):
        from lite_indexer.strategies.evm.range_planning import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("optimism")
        _reset_adaptive_chunk("linea")
        _bump_adaptive_chunk("optimism")
        assert _adaptive_chunk_size("optimism", 50) == 25
        assert _adaptive_chunk_size("linea", 50) == 50

    def test_halves_capped(self):
        from lite_indexer.strategies.evm.range_planning import (
            ADAPTIVE_CHUNK_MAX_HALVES,
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("scroll")
        for _ in range(ADAPTIVE_CHUNK_MAX_HALVES + 3):
            _bump_adaptive_chunk("scroll")
        # 50 // 2**4 = 3, floored to 5.
        assert _adaptive_chunk_size("scroll", 50) == 5


# ═══════════════════════════════════════════════════════════════════════
# 5. Atomic state-file writes
# ═══════════════════════════════════════════════════════════════════════


class TestAtomicStateWrites:
    def test_atomic_write_json_creates_file(self, tmp_path: Path):
        from lite_indexer.state import atomic_write_json

        dest = tmp_path / "lite_ethereum.json"
        atomic_write_json(dest, {"last_block": 100, "chain": "ethereum"})
        assert dest.exists()
        assert json.loads(dest.read_text())["last_block"] == 100

    def test_atomic_write_leaves_no_tmp_files(self, tmp_path: Path):
        from lite_indexer.state import atomic_write_json

        dest = tmp_path / "lite_ethereum.json"
        atomic_write_json(dest, {"last_block": 100})
        leftovers = [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
        assert leftovers == []

    def test_atomic_write_overwrites_previous(self, tmp_path: Path):
        from lite_indexer.state import atomic_write_json

        dest = tmp_path / "lite_ethereum.json"
        atomic_write_json(dest, {"last_block": 100})
        atomic_write_json(dest, {"last_block": 200})
        assert json.loads(dest.read_text())["last_block"] == 200

    def test_atomic_write_json_accepts_str_payload(self, tmp_path: Path):
        from lite_indexer.state import atomic_write_json

        dest = tmp_path / "highwater_ethereum.json"
        atomic_write_json(dest, '{"last_block": 999}')
        assert json.loads(dest.read_text())["last_block"] == 999

    def test_state_manager_save_is_atomic(self, tmp_path: Path):
        from lite_indexer.state import StateManager

        with patch("lite_indexer.state.STATE_DIR", tmp_path):
            StateManager("ethereum").save(last_block=1234, events_count=7)
        data = json.loads((tmp_path / "lite_ethereum.json").read_text())
        assert data["last_block"] == 1234
        assert data["events_indexed"] == 7
        assert data["chain"] == "ethereum"

    def test_highwater_update_is_atomic(self, tmp_path: Path):
        from lite_indexer.state import StateHighWater

        with patch("lite_indexer.state.STATE_DIR", tmp_path):
            hw = StateHighWater("ethereum")
            hw.update(5000)
        data = json.loads((tmp_path / "highwater_ethereum.json").read_text())
        assert data["last_block"] == 5000

    def test_pair_cache_save_is_atomic(self, tmp_path: Path):
        from lite_indexer.state import PairCache

        with patch("lite_indexer.state.STATE_DIR", tmp_path):
            cache = PairCache("ethereum")
            cache.set("0xpair", ("0xt0", "0xt1"))
            cache.save()
        data = json.loads((tmp_path / "pair_tokens_ethereum.json").read_text())
        assert data["0xpair"] == ["0xt0", "0xt1"]

    def test_concurrent_writers_never_corrupt(self, tmp_path: Path):
        """Two threads writing the same state file repeatedly must always
        leave a complete, parseable file (unique tmp names + rename)."""
        from lite_indexer.state import atomic_write_json

        dest = tmp_path / "lite_ethereum.json"
        errors: list[Exception] = []

        def _writer(tag: int) -> None:
            try:
                for i in range(50):
                    atomic_write_json(dest, {"last_block": tag * 1000 + i, "tag": tag})
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=_writer, args=(1,))
        t2 = threading.Thread(target=_writer, args=(2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == []
        # Final file is one writer's complete state, not a torn mix.
        data = json.loads(dest.read_text())
        assert data["tag"] in (1, 2)
        assert "last_block" in data


# ═══════════════════════════════════════════════════════════════════════
# 6. Redis-backed circuit breaker (lite_indexer/rpc/circuit.py)
# ═══════════════════════════════════════════════════════════════════════


class _FakeRedis:
    """Minimal async Redis double: incr/setex/expire/exists/delete."""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.ttls: dict[str, int] = {}

    async def incr(self, key: str) -> int:
        val = int(self.strings.get(key, "0")) + 1
        self.strings[key] = str(val)
        return val

    async def setex(self, key: str, ttl: int, value: object) -> None:
        self.strings[key] = str(value)
        self.ttls[key] = ttl

    async def expire(self, key: str, ttl: int) -> None:
        if ttl <= 0:
            self.strings.pop(key, None)
            self.ttls.pop(key, None)
        else:
            self.ttls[key] = ttl

    async def exists(self, key: str) -> int:
        return 1 if key in self.strings else 0

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.strings.pop(key, None)
            self.ttls.pop(key, None)


@pytest.fixture()
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeRedis:
    """Wire the circuit module to an in-memory Redis double."""
    import lite_indexer.rpc.circuit as circuit_mod

    fake = _FakeRedis()
    monkeypatch.setattr(circuit_mod, "CB_ENABLED", True)
    monkeypatch.setattr(circuit_mod, "REDIS_URL", "redis://fake.invalid")
    monkeypatch.setattr(circuit_mod, "_redis", fake)
    return fake


class TestRedisCircuitBreaker:
    async def test_circuit_opens_after_5_failures(self, fake_redis: _FakeRedis):
        import lite_indexer.rpc.circuit as circuit_mod

        ep = "https://cb-open.invalid"
        for _ in range(circuit_mod.CB_FAILURE_THRESHOLD - 1):
            await circuit_mod.record_circuit_failure(ep)
            assert await circuit_mod.circuit_open(ep) is False
        await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is True

    async def test_circuit_half_open_after_ttl(self, fake_redis: _FakeRedis):
        """After the open-TTL expires the circuit is half-open: calls pass,
        and a single failed probe re-trips immediately (counter pre-seeded
        at threshold - 1)."""
        import lite_indexer.rpc.circuit as circuit_mod

        ep = "https://cb-halfopen.invalid"
        for _ in range(circuit_mod.CB_FAILURE_THRESHOLD):
            await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is True

        # Simulate CB_OPEN_TTL_S elapsing: the open key's TTL expires.
        await fake_redis.expire(circuit_mod._open_key(ep), 0)
        assert await circuit_mod.circuit_open(ep) is False  # half-open

        # One failed probe → pre-seeded counter (threshold-1) + 1 → re-trip.
        await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is True

    async def test_circuit_resets_on_success(self, fake_redis: _FakeRedis):
        """A successful half-open probe clears the failure counter, so the
        circuit does NOT re-trip on the next single failure."""
        import lite_indexer.rpc.circuit as circuit_mod

        ep = "https://cb-reset.invalid"
        for _ in range(circuit_mod.CB_FAILURE_THRESHOLD):
            await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is True

        await circuit_mod.record_circuit_success(ep)
        # TTL expires, probe succeeds → counters cleared.
        await fake_redis.expire(circuit_mod._open_key(ep), 0)
        assert await circuit_mod.circuit_open(ep) is False

        # A single fresh failure must NOT re-trip (counter was reset).
        await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is False

    async def test_circuit_disabled_when_env_off(self, fake_redis: _FakeRedis, monkeypatch: pytest.MonkeyPatch):
        """RPC_CIRCUIT_ENABLED=0 → breaker is a no-op (fail-open)."""
        import lite_indexer.rpc.circuit as circuit_mod

        monkeypatch.setattr(circuit_mod, "CB_ENABLED", False)
        ep = "https://cb-disabled.invalid"
        for _ in range(circuit_mod.CB_FAILURE_THRESHOLD + 2):
            await circuit_mod.record_circuit_failure(ep)
        assert await circuit_mod.circuit_open(ep) is False
        assert fake_redis.strings == {}  # nothing was ever written


# ═══════════════════════════════════════════════════════════════════════
# 7. RpcClient retry: exponential backoff, transient-only
# ═══════════════════════════════════════════════════════════════════════


async def _noop_async(*args: object, **kwargs: object) -> None:
    return None


@pytest.fixture()
def hermetic_rpc(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Silence every side-effecting dependency of RpcClient.call.

    Returns the list of asyncio.sleep delays observed (backoff trace).
    """
    import lite_indexer.rpc.autotune as autotune_mod
    import lite_indexer.rpc.circuit as circuit_mod
    import lite_indexer.rpc.client as client_mod
    import lite_indexer.rpc.ledger as ledger_mod
    import lite_indexer.rpc.routing as routing_mod
    import lite_indexer.rpc.throttle as throttle_mod

    sleeps: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    def _noop(*args: object, **kwargs: object) -> None:
        return None

    async def _circuit_open_false(endpoint: str) -> bool:
        return False

    monkeypatch.setattr(client_mod.asyncio, "sleep", _fake_sleep)
    monkeypatch.setattr(throttle_mod, "throttle_before", _noop_async)
    monkeypatch.setattr(throttle_mod, "record_rpc_response", _noop)
    monkeypatch.setattr(client_mod, "record_endpoint_success", _noop)
    monkeypatch.setattr(client_mod, "record_endpoint_failure", _noop)
    monkeypatch.setattr(client_mod, "record_rate_limit", _noop)
    monkeypatch.setattr(client_mod, "ban_endpoint", _noop)
    monkeypatch.setattr(autotune_mod, "autotune_wait", _noop_async)
    monkeypatch.setattr(autotune_mod, "record_call", _noop)
    monkeypatch.setattr(ledger_mod, "record_endpoint_call", _noop)
    monkeypatch.setattr(routing_mod, "incr_in_flight", _noop)
    monkeypatch.setattr(routing_mod, "decr_in_flight", _noop)
    monkeypatch.setattr(routing_mod, "record_rpc_latency", _noop)
    monkeypatch.setattr(routing_mod, "record_rpc_outcome", _noop)
    monkeypatch.setattr(routing_mod, "record_rpc_usage", _noop)
    monkeypatch.setattr(routing_mod, "record_endpoint_block_height", _noop)
    monkeypatch.setattr(circuit_mod, "circuit_open", _circuit_open_false)
    monkeypatch.setattr(circuit_mod, "record_circuit_failure", _noop_async)
    monkeypatch.setattr(circuit_mod, "record_circuit_success", _noop_async)
    import lite_indexer.rpc.ratelimit as ratelimit_mod

    monkeypatch.setattr(ratelimit_mod, "apply_provider_rate_limit", _noop_async)
    return sleeps


def _install_mock_transport(monkeypatch: pytest.MonkeyPatch, handler: Any) -> list[dict[str, Any]]:
    """Route RpcClient.call through httpx.MockTransport; return sent payloads."""
    import httpx

    import lite_indexer.rpc.client as client_mod

    sent: list[dict[str, Any]] = []

    def _transport_handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content.decode()))
        return handler(request)

    mock_client = httpx.AsyncClient(transport=httpx.MockTransport(_transport_handler))
    monkeypatch.setattr(client_mod, "_get_call_client", _mock_get_client(mock_client))
    return sent


def _mock_get_client(mock_client: Any) -> Any:
    async def _get(timeout: float) -> Any:
        return mock_client

    return _get


class TestRpcRetry:
    async def test_retry_exponential_backoff(self, hermetic_rpc: list[float], monkeypatch: pytest.MonkeyPatch):
        """Transient failures retry with 0.1 → 0.2 → 0.4 backoff, then give
        up returning None (the existing failure shape). Jitter is disabled
        here so the deterministic exponential ceiling is asserted; the
        full-jitter behavior is covered in tests/test_backoff.py."""
        import httpx

        import lite_indexer.rpc.backoff as backoff_mod
        import lite_indexer.rpc.client as client_mod

        monkeypatch.setattr(backoff_mod, "RPC_BACKOFF_JITTER", False)

        def _always_down(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        _install_mock_transport(monkeypatch, _always_down)
        result = await client_mod.RpcClient().call("https://retry.invalid", "eth_blockNumber")

        assert result is None
        assert len(hermetic_rpc) == client_mod.RPC_RETRY_ATTEMPTS - 1
        # Exponential: each delay doubles the previous one.
        for i in range(1, len(hermetic_rpc)):
            assert hermetic_rpc[i] == pytest.approx(hermetic_rpc[i - 1] * 2)

    async def test_retry_skips_non_transient_errors(self, hermetic_rpc: list[float], monkeypatch: pytest.MonkeyPatch):
        """429, 403 and JSON-RPC errors return immediately — no retry, no
        backoff sleep."""
        import httpx

        import lite_indexer.rpc.client as client_mod

        def _rate_limited(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429, text="too many requests")

        _install_mock_transport(monkeypatch, _rate_limited)
        result = await client_mod.RpcClient().call("https://rl.invalid", "eth_blockNumber")
        assert result is None
        assert hermetic_rpc == []  # no backoff sleeps at all

        def _forbidden(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, text="forbidden")

        _install_mock_transport(monkeypatch, _forbidden)
        result = await client_mod.RpcClient().call("https://fb.invalid", "eth_blockNumber")
        assert result is None
        assert hermetic_rpc == []

        def _rpc_error(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"code": -32000, "message": "boom"}})

        _install_mock_transport(monkeypatch, _rpc_error)
        result = await client_mod.RpcClient().call("https://rpcerr.invalid", "eth_blockNumber")
        assert result is not None and "error" in result
        assert hermetic_rpc == []
