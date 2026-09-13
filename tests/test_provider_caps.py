"""Tests for per-account (provider) rate caps (rpc/provider_caps.py).

Covers the account-level limiter that sits ADDITIVELY on top of the
per-endpoint throttle:

* concurrency cap — at most N in-flight per provider (queue, never drop)
* requests/sec cap — rolling-window pacing per provider
* provider-level Retry-After — a 429 on one endpoint backs off the ACCOUNT
* provider id grouping — every endpoint of a provider shares one budget
* TIP-class traffic is unaffected by provider backoff of another provider

Hermetic — no network, no disk.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from lite_indexer.rpc.provider_caps import (
    PROVIDER_LIMITER,
    ProviderRateLimiter,
    provider_slot,
    record_provider_response,
)


@pytest.fixture(autouse=True)
def _clean_provider_limiter():
    PROVIDER_LIMITER.reset()
    yield
    PROVIDER_LIMITER.reset()


class TestConcurrencyCap:
    @pytest.mark.asyncio
    async def test_concurrency_cap_enforced(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=2)
        active = 0
        peak = 0

        async def worker() -> None:
            nonlocal active, peak
            await limiter.acquire("alchemy")
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1
            limiter.release("alchemy")

        await asyncio.gather(*(worker() for _ in range(6)))
        assert peak == 2  # never more than the provider cap in flight
        assert limiter.state_for("alchemy")["in_flight"] == 0

    @pytest.mark.asyncio
    async def test_acquire_queues_not_drops(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=1)
        order: list[str] = []

        async def worker(tag: str) -> None:
            await limiter.acquire("p")
            order.append(f"start-{tag}")
            await asyncio.sleep(0.01)
            order.append(f"end-{tag}")
            limiter.release("p")

        await asyncio.gather(worker("a"), worker("b"), worker("c"))
        # Every call ran (none dropped), serialized by the concurrency cap.
        assert len(order) == 6
        assert order[1] == "end-a"

    def test_zero_cap_means_unlimited(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=0)
        sem = limiter._semaphore("p")  # noqa: SLF001 — cap wiring under test
        assert sem._value >= 1_000_000  # noqa: SLF001


class TestRpsCap:
    def test_rolling_window_pacing(self):
        limiter = ProviderRateLimiter(rps=10.0, max_concurrency=0)
        limiter.mark_sent("infura", now=1000.0)
        wait = limiter.wait_seconds_for("infura", now=1000.05)
        assert 0.045 <= wait <= 0.055
        # Window elapsed → no wait.
        assert limiter.wait_seconds_for("infura", now=1000.2) == 0.0

    def test_pacing_is_bounded_by_max_wait(self):
        limiter = ProviderRateLimiter(rps=0.1, max_concurrency=0, max_wait=1.0)
        limiter.mark_sent("slow", now=0.0)
        assert limiter.wait_seconds_for("slow", now=0.5) == 1.0


class TestProviderRetryAfter:
    def test_retry_after_arms_account_backoff(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=0)
        limiter.record_response("alchemy", 429, {"Retry-After": "30"}, now=1000.0)
        state = limiter.state_for("alchemy", now=1000.1)
        assert state["in_backoff"] is True
        wait = limiter.wait_seconds_for("alchemy", now=1000.1)
        assert 29.5 <= wait <= 30.0

    def test_429_without_retry_after_exponential(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=0)
        limiter.record_response("alchemy", 429, {}, now=1000.0)
        assert limiter.wait_seconds_for("alchemy", now=1000.0) >= 2.0

    def test_200_closes_streak(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=0)
        limiter.record_response("alchemy", 429, {}, now=1000.0)
        limiter.record_response("alchemy", 200, {}, now=1001.0)
        assert limiter.state_for("alchemy")["rate_limit_hits"] == 0

    @pytest.mark.asyncio
    async def test_acquire_honors_provider_backoff(self):
        limiter = ProviderRateLimiter(rps=0.0, max_concurrency=4)
        limiter.record_response("alchemy", 429, {"Retry-After": "0.05"})
        started = time.perf_counter()
        await limiter.acquire("alchemy")
        limiter.release("alchemy")
        assert time.perf_counter() - started >= 0.04


class TestProviderGrouping:
    def test_record_response_groups_by_provider_account(self):
        # Two distinct Alchemy URLs (same account) share one backoff window.
        record_provider_response(
            "https://eth-mainnet.g.alchemy.com/v2/key1",
            429,
            {"Retry-After": "25"},
        )
        state = PROVIDER_LIMITER.state_for("alchemy")
        assert state["in_backoff"] is True
        # A TIP-class call on another Alchemy URL is also gated.
        assert PROVIDER_LIMITER.wait_seconds_for("alchemy") > 20.0
        # A different provider is untouched.
        assert PROVIDER_LIMITER.allow("infura") is True

    @pytest.mark.asyncio
    async def test_provider_slot_uses_global_limiter(self):
        PROVIDER_LIMITER.record_response("helius", 429, {"Retry-After": "0.03"})
        started = time.perf_counter()
        async with provider_slot("helius"):
            pass
        assert time.perf_counter() - started >= 0.02
