"""The distinction the whole pool-walk design turns on (2026-09-26).

``refused`` and ``unserved`` used to be indistinguishable, and the
classifier is the only place that can tell them apart, so it is pinned
here rather than exercised live. Two regressions are guarded against:

* broadening ``refused`` to include 200-with-a-null-body, which is the
  "everything looks refused" failure the split exists to prevent (a chain
  merely behind its tip would page as a provider problem);
* narrowing it so a genuine pool-wide 401/403/451 is no longer reported
  as a refusal, which is the case that actually needs a human.

Network-free and clock-free: pure functions over a list of statuses.
"""

from __future__ import annotations

import pytest

from lite_indexer.rpc.pool_refusal import REFUSAL_STATUSES, PoolRefusal, classify_pool_refusal


def test_no_endpoints_is_not_a_refusal() -> None:
    assert classify_pool_refusal([]).kind == "none"


def test_empty_pool_reason_is_named() -> None:
    assert classify_pool_refusal([]).reason == "no_endpoints"


@pytest.mark.parametrize("status", sorted(REFUSAL_STATUSES))
def test_single_refusal_status_is_not_yet_a_pool_refusal(status: int) -> None:
    # One refusing endpoint out of one is only refused if EVERY endpoint
    # refused. A single refusal with no peers is a mixed outage.
    verdict = classify_pool_refusal([status])
    assert verdict.kind == "none"
    assert verdict.reason == "pool_unreachable"


@pytest.mark.parametrize("status", sorted(REFUSAL_STATUSES))
def test_every_endpoint_refused_is_refused(status: int) -> None:
    verdict = classify_pool_refusal([status, status, status])
    assert verdict.kind == "refused"
    assert verdict.reason == "pool_refused_by_provider"
    assert verdict.all_refused


def test_refusal_set_is_exactly_four_401_family_statuses() -> None:
    # 429 is a rate limit and 5xx is a bad moment; neither is a refusal.
    # A 200 is "answered, no block", which is the case that must NOT be here.
    assert frozenset({401, 402, 403, 451}) == REFUSAL_STATUSES


def test_null_bodies_on_every_endpoint_are_unserved_not_refused() -> None:
    # THE regression. A 200 with a pruned result is a window boundary, not
    # a provider refusal: the crawl moves into range and it self-heals.
    verdict = classify_pool_refusal([200, 200, 200, 200])
    assert verdict.kind == "unserved"
    assert verdict.reason == "no_endpoint_served_block"
    assert not verdict.all_refused
    assert verdict.refused == 0
    assert verdict.unserved == 4


def test_mixed_nulls_and_one_refusal_is_unserved() -> None:
    # One endpoint refusing does not make the pool a refusal when the rest
    # simply do not retain the block.
    verdict = classify_pool_refusal([200, 403, 200])
    assert verdict.kind == "unserved"
    assert verdict.refused == 1
    assert verdict.unserved == 2
    assert not verdict.all_refused


def test_one_200_null_beats_a_pool_of_refusals() -> None:
    # Even if four endpoints refused, one 200-with-null means we were
    # SERVED — the block is outside the retained window, not forbidden.
    verdict = classify_pool_refusal([403, 401, 200, 451])
    assert verdict.kind == "unserved"
    assert not verdict.all_refused


def test_transport_failures_alone_are_unreachable_not_refused() -> None:
    # None = never got a response. Unreachable is its own named condition.
    verdict = classify_pool_refusal([None, None])
    assert verdict.kind == "none"
    assert verdict.reason == "pool_unreachable"
    assert verdict.tried == 2
    assert verdict.refused == 0


def test_refusals_plus_transport_failures_are_not_all_refused() -> None:
    # A refusal alongside an endpoint that never answered is a partial
    # outage, and must not page as a policy refusal.
    verdict = classify_pool_refusal([403, None])
    assert verdict.kind == "none"
    assert verdict.reason == "pool_unreachable"
    assert not verdict.all_refused


def test_pool_of_403s_reports_counts() -> None:
    verdict = classify_pool_refusal([403, 403])
    assert (verdict.tried, verdict.refused, verdict.unserved) == (2, 2, 0)


def test_verdict_is_frozen() -> None:
    verdict = PoolRefusal(kind="refused", reason="r", tried=1, refused=1, unserved=0)
    with pytest.raises(AttributeError):
        verdict.kind = "unserved"  # type: ignore[misc]
