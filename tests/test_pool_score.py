"""Endpoint scoring: cost must dominate TRANSIENT failure, not sustained.

The defect this pins: a flat ``len(failures) * 1000`` term meant one recorded
failure (1000) outranked the free->freemium cost gap (900), so a public keyless
endpoint that blipped once lost to a healthy billable peer for the whole
cooldown window. Public RPCs fail transiently as a matter of course, which is
how the provider quota notices happen: not a billing decision, a scoring
artifact.

These tests are the control. Revert the scoring to flat-1000 and
``test_single_transient_failure_keeps_a_free_endpoint_in_front`` fails.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("KEYED_RPC_KEY", "test-key")
os.environ.setdefault("SOLANA_PROVIDER_KEY", "test-key")

from lite_indexer.rpc.cost_tier import cost_tier  # noqa: E402
from lite_indexer.rpc.cost_tier_map import cost_penalty  # noqa: E402
from lite_indexer.rpc.pool_score import (  # noqa: E402
    FIRST_FAILURE_COST,
    REPEAT_FAILURE_COST,
    endpoint_score,
    failure_cost,
)

FREE = "https://arb1.arbitrum.io"
FREEMIUM = "https://lb.drpc.org/ogrpc?network=arbitrum&dkey=abc123"


def _old_score(endpoint: str, position: int, failures: int) -> int:
    """The pre-fix arithmetic, kept so the control is explicit."""
    return failures * 1000 + position + cost_penalty(endpoint)


class TestFailureCost:
    def test_no_failures_costs_nothing(self) -> None:
        assert failure_cost(0) == 0

    def test_first_failure_is_cheap(self) -> None:
        assert failure_cost(1) == FIRST_FAILURE_COST
        assert cost_penalty(FREEMIUM) - cost_penalty(FREE) > FIRST_FAILURE_COST, (
            "one blip must not outrank the free->freemium cost gap, or every "
            "transient public-endpoint failure escalates the pool to paid"
        )

    def test_second_failure_is_expensive(self) -> None:
        assert failure_cost(2) == FIRST_FAILURE_COST + REPEAT_FAILURE_COST
        assert failure_cost(2) > cost_penalty(FREEMIUM) - cost_penalty(FREE), (
            "a sustained-failing endpoint must be deprioritised in favour of a working paid peer"
        )

    def test_cost_is_monotonic_in_failures(self) -> None:
        scores = [failure_cost(n) for n in range(6)]
        assert scores == sorted(scores)

    def test_negative_failures_are_treated_as_healthy(self) -> None:
        assert failure_cost(-1) == 0


class TestSingleTransientFailureKeepsFreeEndpointInFront:
    """THE regression test."""

    def test_free_endpoint_with_one_blip_beats_healthy_freemium(self) -> None:
        assert cost_tier(FREE) == "free"
        assert cost_tier(FREEMIUM) != "free"
        free_score = endpoint_score(FREE, position=0, failures=1)
        paid_score = endpoint_score(FREEMIUM, position=1, failures=0)
        assert free_score < paid_score, f"free={free_score} must beat freemium={paid_score} after ONE transient failure"

    def test_the_old_arithmetic_would_have_failed_this(self) -> None:
        """Control: the pre-fix score really did prefer the billable endpoint."""
        assert _old_score(FREE, 0, 1) > _old_score(FREEMIUM, 1, 0), (
            "if this ever passes, the old bug is gone and this control is no longer proving anything"
        )


class TestSustainedFailureStillEscalates:
    def test_two_failures_hand_over_to_freemium(self) -> None:
        assert endpoint_score(FREE, 0, 2) > endpoint_score(FREEMIUM, 1, 0)

    def test_three_failures_escalate_further_still(self) -> None:
        assert endpoint_score(FREE, 0, 3) > endpoint_score(FREE, 0, 2)

    def test_a_totally_free_pool_still_works(self) -> None:
        """With no billable peer, escalation has nothing to escalate TO.

        A sustained-failing free endpoint must still rank against a healthy
        free peer, and the score must stay finite rather than exploding.
        """
        broken = endpoint_score(FREE, 0, 99)
        healthy = endpoint_score(FREE, 5, 0)
        assert isinstance(broken, int) and broken < 10**6
        # Ranked below the healthy peer, but present and selectable.
        assert broken > healthy


class TestPositionStillMatters:
    def test_equal_tier_prefers_earlier_position(self) -> None:
        assert endpoint_score(FREE, 0, 0) < endpoint_score(FREE, 3, 0)

    def test_cost_still_outranks_position(self) -> None:
        """The original design intent, preserved."""
        assert cost_penalty(FREEMIUM) > 3, "cost must exceed any list position"

    @pytest.mark.parametrize("failures", [0, 1, 2, 5])
    def test_score_is_finite_and_ordered(self, failures: int) -> None:
        assert endpoint_score(FREE, 0, failures) < endpoint_score(FREE, 1, failures)
