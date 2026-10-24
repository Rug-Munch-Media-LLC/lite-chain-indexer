"""Freemium-aware quota enforcement — one code path, policy-driven tiers.

Free (keyless) endpoints follow the tip and serve aggregated reads at low
concurrency with a strict interval; keyed endpoints serve fresh historical
reads at higher concurrency. Nothing in the module lets a caller opt out —
these tests pin that, including against environment "switches" that must not
exist.
"""

from __future__ import annotations

import os

import pytest

from lite_indexer.rpc.rpc_unavailable import QuotaExceeded
from lite_indexer.rpc.tier_quota import (
    FREE_POLICY,
    PAID_POLICY,
    ReadClass,
    TierQuota,
    tier_for,
)

FREE_EP = "https://quota-free.example/rpc"
PAID_EP = "https://quota-paid.example/v2/abcd1234SECRETKEY"


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_tier_is_derived_from_the_endpoint() -> None:
    assert tier_for(FREE_EP) == "free"
    assert tier_for(PAID_EP) == "paid"


def test_free_tier_admits_head_and_aggregated() -> None:
    clock = Clock()
    quota = TierQuota(now=clock)
    clock.advance(1.0)

    assert quota.admit(FREE_EP, ReadClass.HEAD) == "free"
    clock.advance(1.0)
    assert quota.admit(FREE_EP, ReadClass.AGGREGATED) == "free"


def test_free_tier_refuses_fresh_historical_reads() -> None:
    quota = TierQuota(now=Clock())

    with pytest.raises(QuotaExceeded) as info:
        quota.admit(FREE_EP, ReadClass.HISTORICAL)

    assert info.value.tier == "free"
    assert info.value.read_class == "historical"
    assert info.value.retry_after_s == 0.0


def test_paid_tier_admits_fresh_historical_reads() -> None:
    quota = TierQuota(now=Clock())

    assert quota.admit(PAID_EP, ReadClass.HISTORICAL) == "paid"


def test_free_concurrency_is_stricter_than_paid() -> None:
    clock = Clock()
    quota = TierQuota(now=clock)

    for _ in range(FREE_POLICY.max_concurrency):
        quota.admit(FREE_EP, ReadClass.HEAD)
        clock.advance(FREE_POLICY.min_interval_s)
    with pytest.raises(QuotaExceeded, match="concurrency"):
        quota.admit(FREE_EP, ReadClass.HEAD)

    for _ in range(PAID_POLICY.max_concurrency):
        quota.admit(PAID_EP, ReadClass.HISTORICAL)
    with pytest.raises(QuotaExceeded, match="concurrency"):
        quota.admit(PAID_EP, ReadClass.HISTORICAL)

    assert PAID_POLICY.max_concurrency > FREE_POLICY.max_concurrency


def test_free_interval_reports_a_retry_after() -> None:
    clock = Clock()
    quota = TierQuota(now=clock)
    quota.admit(FREE_EP, ReadClass.HEAD)
    quota.release(FREE_EP)

    with pytest.raises(QuotaExceeded) as info:
        quota.admit(FREE_EP, ReadClass.HEAD)

    assert info.value.retry_after_s > 0.0
    clock.advance(FREE_POLICY.min_interval_s)
    quota.admit(FREE_EP, ReadClass.HEAD)


def test_release_returns_the_slot() -> None:
    clock = Clock()
    quota = TierQuota(now=clock)
    for _ in range(FREE_POLICY.max_concurrency):
        quota.admit(FREE_EP, ReadClass.HEAD)
        clock.advance(FREE_POLICY.min_interval_s)
    with pytest.raises(QuotaExceeded, match="concurrency"):
        quota.admit(FREE_EP, ReadClass.HEAD)

    quota.release(FREE_EP)
    clock.advance(FREE_POLICY.min_interval_s)

    assert quota.admit(FREE_EP, ReadClass.HEAD) == "free"


@pytest.mark.parametrize("switch", ["RPC_QUOTA_DISABLED", "RPC_QUOTA_BYPASS", "RPC_FREEMIUM_UNLIMITED"])
def test_no_environment_switch_can_bypass_the_free_tier(switch: str) -> None:
    os.environ[switch] = "1"
    try:
        quota = TierQuota(now=Clock())
        with pytest.raises(QuotaExceeded):
            quota.admit(FREE_EP, ReadClass.HISTORICAL)
    finally:
        os.environ.pop(switch, None)


def test_snapshot_exposes_tier_and_counters() -> None:
    clock = Clock()
    quota = TierQuota(now=clock)
    quota.admit(FREE_EP, ReadClass.HEAD)
    with pytest.raises(QuotaExceeded):
        quota.admit(FREE_EP, ReadClass.HISTORICAL)

    snap = quota.snapshot()[FREE_EP]
    assert snap["tier"] == "free"
    assert snap["admitted"] == 1
    assert snap["rejected"] == 1
