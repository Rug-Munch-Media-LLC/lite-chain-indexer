"""Provider-aware rotation: fairness must be measured at the provider's granularity.

INCIDENT 2026-11-16 (measured on the live fleet)
    ``_select_fair`` ranked ENDPOINT URLs. Several endpoints of one provider
    share a single rate-limit bucket, so spreading across their sibling URLs
    spreads nothing: tenderly held 21 of our 111 endpoints and pocket 19, and
    those two absorbed 571 of 803 observed calls in 30 minutes. The provider
    answered with 702 ``rpc_rate_limit_streak_softban`` and 186
    ``provider_429_backoff`` events in two hours.

    Per-URL fairness was working exactly as designed and was still the cause:
    21 "different" endpoints were one account.

These tests pin the contract: a provider cannot win share by owning more URLs.
"""

from __future__ import annotations

from collections import Counter

import pytest
from lite_indexer.rpc.ledger_keys import provider_id_for_endpoint
from lite_indexer.rpc.pool import (
    _PROVIDER_COUNTS,
    _SELECTION_COUNTS,
    _select_fair,
)


@pytest.fixture(autouse=True)
def _reset_counters():
    """Isolate every test from the module-level selection counters."""
    _SELECTION_COUNTS.clear()
    _PROVIDER_COUNTS.clear()
    yield
    _SELECTION_COUNTS.clear()
    _PROVIDER_COUNTS.clear()


def _shares(endpoints, picks=300):
    """Provider pick counts over ``picks`` selections."""
    counts = Counter()
    for _ in range(picks):
        counts[provider_id_for_endpoint(_select_fair(list(endpoints)))] += 1
    return counts


def test_many_urls_one_provider_cannot_take_most_traffic():
    """21 tenderly URLs vs 2 single-URL providers: tenderly must not dominate.

    Before the fix this returned ~91% tenderly, because it owned 21 of 23
    URLs and every URL scored as its own provider.
    """
    endpoints = [f"https://gw{i}.tenderly.co" for i in range(21)]
    endpoints += ["https://a.publicnode.com", "https://b.1rpc.io"]

    counts = _shares(endpoints, picks=200)
    tenderly_share = counts["tenderly.co"] / 200.0

    assert tenderly_share < 0.60, (
        f"tenderly.co took {tenderly_share:.0%} of traffic despite being 1 of 3 "
        f"providers - rotation is ranking URLs, not provider accounts: {dict(counts)}"
    )


def test_every_provider_gets_a_share():
    """No provider that is in the pool is starved to zero."""
    endpoints = [f"https://gw{i}.tenderly.co" for i in range(10)]
    endpoints += [
        "https://a.publicnode.com",
        "https://b.1rpc.io",
        "https://c.drpc.org",
    ]

    counts = _shares(endpoints, picks=200)

    assert set(counts) == {"tenderly.co", "publicnode.com", "1rpc.io", "drpc.org"}
    for provider, n in counts.items():
        assert n > 0, f"{provider} was never selected: {dict(counts)}"


def test_single_endpoint_still_returns_and_counts():
    """A one-endpoint pool must still work and record both counters."""
    chosen = _select_fair(["https://only.example.com/rpc"])

    assert chosen == "https://only.example.com/rpc"
    assert _SELECTION_COUNTS[chosen] == 1
    assert _PROVIDER_COUNTS["example.com"] == 1


def test_degenerate_pool_is_deterministic():
    """Two URLs on the SAME provider is one account; it must not be skipped."""
    endpoints = ["https://a.example.com/rpc", "https://b.example.com/rpc"]

    counts = _shares(endpoints, picks=50)

    assert counts["example.com"] == 50
