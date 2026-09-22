"""Coverage tests for ``lite_indexer.query.legs_clickhouse`` filter rendering.

Hermetic — no network, no ClickHouse. Pins the AND/OR filter semantics the
hot-tier leg renders into a parameterized SELECT: dict → AND equality,
list of ``{"col", "value"}`` → OR equality, and the empty/blank cases.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.query.legs_clickhouse import append_ch_filters, check_table


def _render(filters: Any) -> tuple[list[str], dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    append_ch_filters(clauses, params, filters)
    return clauses, params


# ── check_table ─────────────────────────────────────────────────────────


def test_check_table_accepts_routable_table() -> None:
    assert check_table("evm_transfers") == "evm_transfers"


def test_check_table_rejects_unknown_table() -> None:
    import pytest

    with pytest.raises(ValueError, match="unknown table"):
        check_table("nope")


# ── append_ch_filters ───────────────────────────────────────────────────


def test_no_filters_adds_nothing() -> None:
    assert _render(None) == ([], {})
    assert _render({}) == ([], {})


def test_dict_filters_render_and_equality() -> None:
    clauses, params = _render({"tx_hash": "0xabc", "log_index": 3})
    assert clauses == ["tx_hash = %(f_tx_hash)s", "log_index = %(f_log_index)s"]
    assert params == {"f_tx_hash": "0xabc", "f_log_index": 3}


def test_list_filters_render_or_equality() -> None:
    clauses, params = _render(
        [{"col": "from_address", "value": "0x1"}, {"col": "to_address", "value": "0x1"}]
    )
    assert clauses == ["(from_address = %(f_from_address_0)s OR to_address = %(f_to_address_1)s)"]
    assert params == {"f_from_address_0": "0x1", "f_to_address_1": "0x1"}


def test_list_filters_skip_entries_without_col() -> None:
    clauses, params = _render([{"value": "0x1"}, {"col": "to_address", "value": "0x2"}])
    assert clauses == ["(to_address = %(f_to_address_1)s)"]
    assert params == {"f_to_address_1": "0x2"}


def test_list_filters_all_blank_adds_nothing() -> None:
    assert _render([{"col": "", "value": "x"}]) == ([], {})


def test_list_filters_use_indexed_param_names() -> None:
    clauses, params = _render([{"col": "a", "value": 1}, {"col": "a", "value": 2}])
    assert clauses == ["(a = %(f_a_0)s OR a = %(f_a_1)s)"]
    assert params == {"f_a_0": 1, "f_a_1": 2}
