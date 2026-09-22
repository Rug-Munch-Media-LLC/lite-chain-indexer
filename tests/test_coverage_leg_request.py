"""Coverage tests for ``lite_indexer.query.leg_request`` cache-key behaviour.

Hermetic — pure dataclass logic, no network, no ClickHouse. Pins the
cache-key contract the QueryRouter legs rely on: filter normalization
(dict → sorted tuple, list → tuple of sorted tuples) and the exact key
shape so two equivalent requests always collide in the cache.
"""

from __future__ import annotations

import pytest

from lite_indexer.query.leg_request import LegRequest


def test_cache_key_shape_for_plain_range_query() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", from_block=1, to_block=2, limit=10)
    assert req.cache_key() == ("ethereum", "evm_transfers", 1, 2, (), 10)


def test_cache_key_normalizes_dict_filters_to_sorted_tuple() -> None:
    req = LegRequest(
        table="evm_transfers",
        chain="base",
        filters={"tx_hash": "0xabc", "log_index": 3},
    )
    assert req.cache_key()[4] == (("log_index", 3), ("tx_hash", "0xabc"))


def test_cache_key_dict_filter_order_does_not_matter() -> None:
    a = LegRequest(table="evm_transfers", chain="base", filters={"a": 1, "b": 2})
    b = LegRequest(table="evm_transfers", chain="base", filters={"b": 2, "a": 1})
    assert a.cache_key() == b.cache_key()


def test_cache_key_normalizes_list_filters_to_tuple_of_tuples() -> None:
    req = LegRequest(
        table="evm_transfers",
        chain="ethereum",
        filters=[{"col": "from_address", "value": "0x1"}, {"col": "to_address", "value": "0x1"}],
    )
    assert req.cache_key()[4] == (
        (("col", "from_address"), ("value", "0x1")),
        (("col", "to_address"), ("value", "0x1")),
    )


def test_cache_key_none_filters_is_empty_tuple() -> None:
    req = LegRequest(table="deployer_events", chain="ethereum", filters=None)
    assert req.cache_key()[4] == ()


def test_cache_key_limit_is_coerced_to_int() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", limit=100)
    assert req.cache_key()[-1] == 100


def test_cache_key_differs_when_chain_differs() -> None:
    a = LegRequest(table="evm_transfers", chain="ethereum")
    b = LegRequest(table="evm_transfers", chain="base")
    assert a.cache_key() != b.cache_key()


def test_cache_key_differs_when_block_range_differs() -> None:
    a = LegRequest(table="evm_transfers", chain="ethereum", from_block=1, to_block=2)
    b = LegRequest(table="evm_transfers", chain="ethereum", from_block=1, to_block=3)
    assert a.cache_key() != b.cache_key()


def test_leg_request_is_frozen() -> None:
    import dataclasses

    req = LegRequest(table="evm_transfers", chain="ethereum")
    with pytest.raises(dataclasses.FrozenInstanceError):
        req.chain = "base"  # type: ignore[misc]
