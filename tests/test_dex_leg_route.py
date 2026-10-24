"""Tests for the per-leg endpoint rotation of the EVM dex fetcher.

Coverage target is the rotation arithmetic: the six legs must NOT share a
start index (that was the 30.1 s bug) and an unknown label must degrade to
the old behaviour instead of raising inside a fetch path.
"""

from __future__ import annotations

from lite_indexer.strategies.evm.dex_leg_route import DEX_LEG_CONNECT_TIMEOUT_S, leg_start_index

_LEGS = ("v2", "v3", "lp_v2_mint", "lp_v2_burn", "lp_v3_mint", "lp_v3_burn")


def test_legs_do_not_share_a_start_index() -> None:
    starts = [leg_start_index(leg, 10) for leg in _LEGS]
    assert len(set(starts)) == len(_LEGS)


def test_offset_wraps_into_the_pool_size() -> None:
    for count in (1, 2, 3, 5, 10):
        for leg in _LEGS:
            assert 0 <= leg_start_index(leg, count) < count


def test_empty_pool_is_zero() -> None:
    assert leg_start_index("v2", 0) == 0
    assert leg_start_index("v2", -3) == 0


def test_unknown_label_degrades_to_first_leg() -> None:
    assert leg_start_index("brand_new_leg", 7) == 0


def test_connect_timeout_is_a_few_seconds() -> None:
    # A dead endpoint must cost seconds, not the 30 s attempt budget.
    assert 0 < DEX_LEG_CONNECT_TIMEOUT_S <= 5.0
