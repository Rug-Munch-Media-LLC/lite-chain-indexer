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
from unittest.mock import patch

import pytest

from lite_indexer.rpc.pool import (
    ban_endpoint,
    desperation_probe_due,
    force_probe_endpoint,
    get_best_endpoint,
    is_endpoint_banned,
    least_banned_endpoint,
    mark_desperation_probe,
    pool_fully_down,
    record_endpoint_failure,
    record_endpoint_success,
    record_rate_limit,
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
def _clean_pool(monkeypatch: pytest.MonkeyPatch):
    """Reset pool state between tests so bans/failures never leak."""
    import lite_indexer.rpc.pool as pool_mod
    import lite_indexer.rpc.routing as routing_mod

    monkeypatch.setattr(pool_mod, "_endpoint_failures", {})
    monkeypatch.setattr(pool_mod, "_endpoint_bans", {})
    monkeypatch.setattr(pool_mod, "_endpoint_ban_level", {})
    monkeypatch.setattr(routing_mod, "CIRCUIT_BREAKER", RpcCircuitBreaker())
    yield


# ═══════════════════════════════════════════════════════════════════════
# 1. Pool-wide outage → desperation probe, chain does not stall
# ═══════════════════════════════════════════════════════════════════════


class TestPoolWideOutage:
    def test_pool_fully_down_detects_all_banned(self, clock: _Clock):
        a, b = "https://a.invalid", "https://b.invalid"
        ban_endpoint(a, reason="http_429")
        ban_endpoint(b, reason="http_429")
        assert pool_fully_down([a, b]) is True

    def test_pool_fully_down_false_when_one_healthy(self, clock: _Clock):
        a, b = "https://a.invalid", "https://b.invalid"
        ban_endpoint(a, reason="http_429")
        assert pool_fully_down([a, b]) is False

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
        """Even dozens of 429s must not trip the breaker (threshold 10/120s)."""
        ep = "https://ratelimited.invalid"
        for _ in range(50):
            record_rate_limit(ep)
        from lite_indexer.rpc.routing import CIRCUIT_BREAKER

        assert CIRCUIT_BREAKER.is_open(ep) is False
        assert is_endpoint_banned(ep) is False

    def test_rate_limit_does_not_hard_ban(self, clock: _Clock):
        """A 429 must not put the endpoint under the exponential ban window."""
        ep = "https://ratelimited.invalid"
        record_rate_limit(ep)
        record_rate_limit(ep)
        assert is_endpoint_banned(ep) is False

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
        from lite_indexer.strategies.evm.transfer_handler import (
            _adaptive_chunk_size,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("ethereum")
        assert _adaptive_chunk_size("ethereum", 50) == 50

    def test_failure_halves_chunk(self):
        from lite_indexer.strategies.evm.transfer_handler import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("base")
        _bump_adaptive_chunk("base")
        assert _adaptive_chunk_size("base", 50) == 25

    def test_repeated_failures_halve_twice(self):
        from lite_indexer.strategies.evm.transfer_handler import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("bsc")
        _bump_adaptive_chunk("bsc")
        _bump_adaptive_chunk("bsc")
        assert _adaptive_chunk_size("bsc", 50) == 12

    def test_chunk_never_below_floor(self):
        from lite_indexer.strategies.evm.transfer_handler import (
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
        from lite_indexer.strategies.evm.transfer_handler import (
            _adaptive_chunk_size,
            _bump_adaptive_chunk,
            _reset_adaptive_chunk,
        )

        _reset_adaptive_chunk("arbitrum")
        _bump_adaptive_chunk("arbitrum")
        assert _adaptive_chunk_size("arbitrum", 50) == 25
        _reset_adaptive_chunk("arbitrum")
        assert _adaptive_chunk_size("arbitrum", 50) == 50

    def test_chains_independent(self):
        from lite_indexer.strategies.evm.transfer_handler import (
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
        from lite_indexer.strategies.evm.transfer_handler import (
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
