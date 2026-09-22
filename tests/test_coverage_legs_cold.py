"""Coverage tests for ``lite_indexer.query.legs_cold`` WHERE rendering.

Hermetic — no network, no DuckDB. Pins the cold-tier filter rendering:
dict → AND equality, list → OR equality, values rendered as DuckDB
literals (strings quoted/escaped, bools as ints, None as NULL).
"""

from __future__ import annotations

from typing import Any

from lite_indexer.query.legs_cold import build_cold_where


def test_no_filters_renders_empty_where() -> None:
    assert build_cold_where(None) == ""
    assert build_cold_where({}) == ""


def test_dict_filters_render_and_equality() -> None:
    assert (
        build_cold_where({"tx_hash": "0xabc", "log_index": 3})
        == "tx_hash = '0xabc' AND log_index = 3"
    )


def test_list_filters_render_or_equality() -> None:
    where = build_cold_where(
        [{"col": "from_address", "value": "0x1"}, {"col": "to_address", "value": "0x1"}]
    )
    assert where == "(from_address = '0x1' OR to_address = '0x1')"


def test_string_values_are_escaped() -> None:
    assert build_cold_where({"name": "O'Brien"}) == "name = 'O''Brien'"


def test_bool_values_render_as_ints() -> None:
    assert build_cold_where({"flag": True}) == "flag = 1"


def test_none_value_renders_as_null() -> None:
    assert build_cold_where({"x": None}) == "x = NULL"


def test_numeric_values_render_verbatim() -> None:
    assert build_cold_where({"block_number": 42}) == "block_number = 42"


def test_where_is_a_plain_string() -> None:
    result: Any = build_cold_where({"a": 1})
    assert isinstance(result, str)
