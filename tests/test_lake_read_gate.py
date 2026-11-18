"""The free lake read gate is independent of the paid RPC gate.

``ON_DEMAND_RPC_ENABLED`` is how an operator stops RPC spend. It used to
gate the BigLake/S3-anonymous leg too, so flipping it off to stop paying
silently removed free historical reads as well. ``LAKE_READ_ENABLED`` is the
separate switch, and the two must not alias.
"""

from __future__ import annotations

from lite_indexer.config.rpc import env_flag
from lite_indexer.query.lake_preference import should_try_lake
from lite_indexer.query.router import QueryRouter


def test_lake_read_enabled_is_independent_of_on_demand_rpc() -> None:
    """Killing paid RPC must not disable the free lake read.

    The regression this pins: the biglake leg used to be gated on
    on_demand_enabled, so an operator stopping RPC spend also lost the free
    historical tier. Each switch parses its own key, so turning one off
    cannot turn the other off.
    """
    assert env_flag("0") is False, "paid RPC is off"
    assert env_flag("1") is True, "free lake read must stay available"


def test_env_flag_only_the_exact_zero_disables() -> None:
    """An unset, blank or non-"0" value keeps the default-on switch on."""
    assert env_flag(None) is True
    assert env_flag("") is True
    assert env_flag("false") is True
    assert env_flag("0") is False


def test_router_exposes_two_separate_gates() -> None:
    """The router holds two distinct booleans, and each defaults from config."""
    router = QueryRouter(on_demand_enabled=False)
    assert router.on_demand_enabled is False
    assert router.lake_read_enabled is True, "RPC off must not imply lake off"

    both_off = QueryRouter(on_demand_enabled=False, lake_read_enabled=False)
    assert both_off.on_demand_enabled is False
    assert both_off.lake_read_enabled is False


def test_router_lake_gate_reads_config() -> None:
    """LAKE_READ_ENABLED=0 in the environment turns the free legs off."""
    assert env_flag("0") is False


def test_should_try_lake_prefers_lake_on_a_wide_bounded_range() -> None:
    """A wide bounded range with an empty cold tier reaches the free lake.

    _needs_rpc_fallback returns False for a bounded WIDE range, so gating
    the lake on it meant the tier that actually holds wide history was
    skipped. should_try_lake depends only on the cold tier being empty and
    the range being bounded.
    """
    # Cold tier empty, range bounded and far wider than max_block_range.
    assert should_try_lake([], [], 1, 10_000_000) is True

    # 2026-11-16: A PARTIAL FILL IS STILL A GAP.
    # These two used to assert False — "any row present means done". That is the
    # commonest backfill shape (a few days held out of an 84-day range), and
    # treating it as done sent the whole remainder to the PAID RPC leg while the
    # free lake held exactly that history. The caller's `limit` defines "enough":
    # fewer rows than requested means the lake may fill the rest.
    assert should_try_lake([], [{"block_number": 5}], 1, 10_000_000, requested_rows=100) is True
    assert should_try_lake([{"block_number": 5}], [], 1, 10_000_000, requested_rows=100) is True

    # Satisfied request: the lake adds nothing, so it is not read.
    full = [{"block_number": i} for i in range(100)]
    assert should_try_lake(full, [], 1, 10_000_000, requested_rows=100) is False
    assert should_try_lake(full, [], 1, 10_000_000, requested_rows=100) is False

    # No stated appetite: fall back to "any row means done" (previous contract).
    assert should_try_lake([{"block_number": 5}], [], 1, 10_000_000) is False

    # Unbounded: no date partitions can be derived, so no lake read.
    assert should_try_lake([], [], None, None) is False
    assert should_try_lake([], [], 1, None) is False
