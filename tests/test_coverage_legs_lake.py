"""Coverage tests for ``lite_indexer.query.legs_lake`` row filtering.

Hermetic — no network, no lake. Pins ``filter_rows``: dict filters are
AND equality, list filters are OR equality, matching is case-insensitive
on stringified values, and no filters is a pass-through.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.query.legs_lake import filter_rows

_ROWS: list[dict[str, Any]] = [
    {"tx_hash": "0xABC", "from_address": "0x1", "to_address": "0x2"},
    {"tx_hash": "0xdef", "from_address": "0x2", "to_address": "0x1"},
    {"tx_hash": "0x999", "from_address": "0x3", "to_address": "0x4"},
]


def test_no_filters_returns_all_rows() -> None:
    assert filter_rows("evm_transfers", _ROWS, None) == _ROWS
    assert filter_rows("evm_transfers", _ROWS, {}) == _ROWS


def test_dict_filters_are_and_equality() -> None:
    out = filter_rows("evm_transfers", _ROWS, {"from_address": "0x1", "to_address": "0x2"})
    assert [r["tx_hash"] for r in out] == ["0xABC"]


def test_dict_filters_match_is_case_insensitive() -> None:
    out = filter_rows("evm_transfers", _ROWS, {"tx_hash": "0xabc"})
    assert [r["tx_hash"] for r in out] == ["0xABC"]


def test_list_filters_are_or_equality() -> None:
    out = filter_rows(
        "evm_transfers",
        _ROWS,
        [{"from_address": "0x1"}, {"to_address": "0x1"}],
    )
    assert [r["tx_hash"] for r in out] == ["0xABC", "0xdef"]


def test_list_filters_no_match_returns_empty() -> None:
    assert filter_rows("evm_transfers", _ROWS, [{"from_address": "0xzz"}]) == []


def test_missing_column_does_not_match() -> None:
    assert filter_rows("evm_transfers", _ROWS, {"nonexistent": "x"}) == []


def test_numeric_values_are_stringified_for_comparison() -> None:
    rows = [{"block_number": 42}, {"block_number": 43}]
    assert filter_rows("evm_transfers", rows, {"block_number": 42}) == [{"block_number": 42}]
