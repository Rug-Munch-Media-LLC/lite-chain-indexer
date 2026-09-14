"""Per-provider / per-endpoint request budgets + pre-emptive cool-down.

Covers ``lite_indexer/rpc/budgets.py`` and the budget mechanism in
``lite_indexer/config/rpc.py``:

* the generic keyless default is a no-op (OSS behaviour unchanged)
* an operator-supplied budget is enforced (rps pacing + concurrency)
* the identity is pre-emptively cooled at ~80% of its daily budget,
  BEFORE the provider's 429 arrives
* a 429 cools defensively; a success / UTC day rollover recovers
* cooled identities report ``budget_available`` False so selection routes
  around them

Hermetic — no network, no disk.
"""

from __future__ import annotations

import asyncio

import pytest

from lite_indexer.config import ProviderBudget, resolve_provider_budget
from lite_indexer.rpc.budgets import ProviderBudgetTracker


class _Clock:
    """Mutable fake clock for deterministic budget timing."""

    def __init__(self) -> None:
        # Mid-day UTC so a small advance stays inside the same day.
        self.now = 1_700_000_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _provider_budget(**kw: object) -> ProviderBudget:
    base: dict[str, object] = {"rps": 10.0, "max_concurrency": 2, "daily_budget": 0, "explicit": True}
    base.update(kw)
    return ProviderBudget(**base)  # type: ignore[arg-type]


def _tracker(clock: _Clock, **kw: object) -> ProviderBudgetTracker:
    return ProviderBudgetTracker(time_fn=clock, **kw)  # type: ignore[arg-type]


EP = "https://rpc.example.com/v1"


class TestConfigMechanism:
    def test_default_is_not_explicit(self) -> None:
        budget = resolve_provider_budget("example.com", EP, provider_budgets={}, endpoint_budgets={})
        assert budget.explicit is False

    def test_provider_and_endpoint_precedence(self) -> None:
        provider = _provider_budget(rps=5.0)
        endpoint = _provider_budget(rps=1.0)
        assert (
            resolve_provider_budget(
                "example.com", EP, provider_budgets={"example.com": provider}, endpoint_budgets={EP: endpoint}
            )
            == endpoint
        )
        assert (
            resolve_provider_budget("example.com", EP, provider_budgets={"example.com": provider}, endpoint_budgets={})
            == provider
        )


class TestBudgetEnforcement:
    def test_generic_default_is_noop(self) -> None:
        tracker = _tracker(_Clock())
        assert tracker.enforces(EP) is False
        tracker.record_dispatch(EP)  # must not create state / cool
        assert tracker.is_cooled(EP) is False
        assert tracker.budget_available(EP) is True
        assert tracker.wait_seconds(EP) == 0.0

    def test_explicit_budget_is_enforced(self) -> None:
        tracker = _tracker(_Clock(), provider_budgets={"example.com": _provider_budget()})
        assert tracker.enforces(EP) is True
        assert tracker.budget_for(EP).rps == 10.0

    def test_preemptive_cool_at_threshold_before_429(self) -> None:
        clock = _Clock()
        tracker = _tracker(clock, provider_budgets={"example.com": _provider_budget(daily_budget=10)})
        for _ in range(7):
            tracker.record_dispatch(EP)
        assert tracker.is_cooled(EP) is False  # 70% < 80%
        tracker.record_dispatch(EP)  # 8 == 80% → pre-emptive cool
        assert tracker.is_cooled(EP) is True
        assert tracker.budget_available(EP) is False

    def test_cool_down_expires_then_recovers_on_success(self) -> None:
        clock = _Clock()
        tracker = _tracker(
            clock,
            provider_budgets={"example.com": _provider_budget(daily_budget=10)},
            cooldown_seconds=10.0,
            cooldown_backoff=1.0,
        )
        for _ in range(8):
            tracker.record_dispatch(EP)
        assert tracker.is_cooled(EP) is True
        clock.advance(11.0)
        assert tracker.is_cooled(EP) is False  # window elapsed
        tracker.record_success(EP)  # recovery decays the level
        assert tracker.is_cooled(EP) is False

    def test_429_cools_defensively(self) -> None:
        clock = _Clock()
        tracker = _tracker(clock, provider_budgets={"example.com": _provider_budget()})
        tracker.record_response(EP, 429)
        assert tracker.is_cooled(EP) is True

    def test_day_rollover_resets_budget(self) -> None:
        clock = _Clock()
        tracker = _tracker(clock, provider_budgets={"example.com": _provider_budget(daily_budget=10)})
        for _ in range(8):
            tracker.record_dispatch(EP)
        assert tracker.used_today(EP) == 8
        clock.advance(86_400.0)  # next UTC day
        assert tracker.used_today(EP) == 0
        assert tracker.is_cooled(EP) is False

    def test_rps_pacing(self) -> None:
        clock = _Clock()
        tracker = _tracker(clock, provider_budgets={"example.com": _provider_budget(rps=2.0)})
        tracker.record_dispatch(EP)
        assert tracker.wait_seconds(EP) == pytest.approx(0.5, abs=0.01)
        clock.advance(0.5)
        assert tracker.wait_seconds(EP) == 0.0

    def test_daily_budget_is_a_hard_cap(self) -> None:
        clock = _Clock()
        tracker = _tracker(
            clock,
            provider_budgets={"example.com": _provider_budget(daily_budget=5)},
            cooldown_seconds=1.0,
            cooldown_backoff=1.0,
        )
        for _ in range(5):
            tracker.record_dispatch(EP)
        assert tracker.used_today(EP) == 5
        assert tracker.budget_available(EP) is False  # hard cap
        clock.advance(60.0)  # cool-down would have elapsed long ago
        assert tracker.budget_available(EP) is False  # still capped
        clock.advance(86_400.0)  # next UTC day resets
        assert tracker.budget_available(EP) is True

    def test_daily_cap_aggregates_per_provider_identity(self) -> None:
        clock = _Clock()
        ep2 = "https://rpc2.example.com/v1"
        tracker = _tracker(
            clock,
            provider_budgets={"example.com": _provider_budget(daily_budget=5)},
            cooldown_seconds=1.0,
            cooldown_backoff=1.0,
        )
        for _ in range(3):
            tracker.record_dispatch(EP)
        for _ in range(2):
            tracker.record_dispatch(ep2)
        # Provider-level cap is shared across all of the provider's endpoints.
        assert tracker.used_today(EP) == tracker.used_today(ep2) == 5
        assert tracker.budget_available(EP) is False
        assert tracker.budget_available(ep2) is False

    def test_daily_exhaustion_emits_metric(self) -> None:
        from lite_indexer import metrics

        before = metrics.get("rpc_budget_daily_exhausted")
        clock = _Clock()
        tracker = _tracker(clock, provider_budgets={"example.com": _provider_budget(daily_budget=2)})
        for _ in range(2):
            tracker.record_dispatch(EP)
        assert metrics.get("rpc_budget_daily_exhausted") > before

    @pytest.mark.asyncio
    async def test_concurrency_cap_enforced(self) -> None:
        tracker = _tracker(_Clock(), provider_budgets={"example.com": _provider_budget(max_concurrency=1)})
        active = 0
        peak = 0

        async def worker() -> None:
            nonlocal active, peak
            await tracker.acquire_slot(EP)
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1
            tracker.release_slot(EP)

        await asyncio.gather(*(worker() for _ in range(5)))
        assert peak == 1
        assert tracker.snapshot()[EP]["in_flight"] == 0
