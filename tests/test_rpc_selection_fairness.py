"""Fair-spread endpoint selection — no endpoint absorbs a chain.

Covers ``lite_indexer/rpc/pool.get_best_endpoint``'s fairness/rotation term:
the least-used half of the healthy set is the candidate window (so load
round-robins) and P2C remains the latency tiebreak inside it. A
pre-emptively cooled identity is skipped.

Hermetic — no network, no disk.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.rpc import pool
from lite_indexer.rpc.routing.seams import set_circuit_breaker, set_selector


@pytest.fixture(autouse=True)
def _clean_pool(_mp: pytest.MonkeyPatch, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Reset pool + fairness state so nothing leaks between tests."""
    from lite_indexer.rpc.routing import RpcCircuitBreaker

    monkeypatch.setattr(pool, "_endpoint_failures", {})
    from lite_indexer.rpc import ban_state

    _mp.setattr(ban_state, "_endpoint_bans", {})
    _mp.setattr(ban_state, "_endpoint_ban_level", {})
    monkeypatch.setattr(pool, "_SELECTION_COUNTS", {})
    set_circuit_breaker(RpcCircuitBreaker())
    yield
    set_circuit_breaker(None)
    set_selector(None)


ENDPOINTS = [f"https://rpc{i}.example.com" for i in range(4)]


class TestFairSpread:
    def test_load_spreads_across_healthy_endpoints(self) -> None:
        for _ in range(40):
            pool.get_best_endpoint(ENDPOINTS)
        counts = pool.selection_counts()
        assert set(counts) == set(ENDPOINTS)
        # No endpoint absorbs the chain: every peer is used and the spread
        # between the busiest and idlest is tiny (rotation window).
        assert max(counts.values()) - min(counts.values()) <= 2

    def test_single_endpoint_returned(self) -> None:
        only = "https://solo.example.com"
        for _ in range(5):
            assert pool.get_best_endpoint([only]) == only

    def test_p2c_remains_the_tiebreak(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[list[str]] = []

        def _capture(candidates: list[str]) -> str:
            calls.append(list(candidates))
            return candidates[0]

        set_selector(_capture)
        chosen = pool.get_best_endpoint(["https://a.example.com", "https://b.example.com"])
        assert calls, "P2C must be consulted inside the fairness window"
        assert chosen in calls[0]

    def test_cooled_endpoint_is_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cold = ENDPOINTS[0]
        monkeypatch.setattr(pool, "budget_available", lambda ep: ep != cold)
        seen = {pool.get_best_endpoint(ENDPOINTS) for _ in range(20)}
        assert cold not in seen
        assert seen  # a budget-available peer always wins
