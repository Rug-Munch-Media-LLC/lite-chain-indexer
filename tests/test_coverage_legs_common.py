"""Coverage tests for ``lite_indexer.query.legs_common`` pure helpers.

Hermetic — no network, no ClickHouse. Pins the shared table maps and the
small pure functions every QueryRouter leg depends on: block-column
selection (slot for Solana), DuckDB literal rendering, and the
mixed-type-safe sort key.
"""

from __future__ import annotations

from datetime import UTC, datetime

from lite_indexer.query.legs_common import (
    CH_TABLE_REF,
    ROW_KEYS,
    SORT_COLS,
    block_col,
    cold_lit,
    sort_value,
)


# ── table maps ──────────────────────────────────────────────────────────


def test_every_routable_table_has_row_keys_and_sort_cols() -> None:
    for table in CH_TABLE_REF:
        assert table in ROW_KEYS, f"{table} missing ROW_KEYS"
        assert table in SORT_COLS, f"{table} missing SORT_COLS"


def test_solana_transfers_uses_slot_column() -> None:
    assert block_col("solana_transfers") == "slot"


def test_evm_tables_use_block_number_column() -> None:
    assert block_col("evm_transfers") == "block_number"
    assert block_col("dex_events") == "block_number"


# ── cold_lit ────────────────────────────────────────────────────────────


def test_cold_lit_quotes_strings() -> None:
    assert cold_lit("0xabc") == "'0xabc'"


def test_cold_lit_escapes_single_quotes() -> None:
    assert cold_lit("O'Brien") == "'O''Brien'"


def test_cold_lit_renders_bools_as_ints() -> None:
    assert cold_lit(True) == "1"
    assert cold_lit(False) == "0"


def test_cold_lit_renders_none_as_null() -> None:
    assert cold_lit(None) == "NULL"


def test_cold_lit_renders_numbers_verbatim() -> None:
    assert cold_lit(42) == "42"
    assert cold_lit(3.5) == "3.5"


# ── sort_value ──────────────────────────────────────────────────────────


def test_sort_value_missing_sorts_first() -> None:
    assert sort_value({}, "block_number") == (0, 0)
    assert sort_value({"block_number": ""}, "block_number") == (0, 0)


def test_sort_value_numeric_bucket() -> None:
    assert sort_value({"block_number": 5}, "block_number") == (1, 5.0)


def test_sort_value_bool_is_numeric_bucket() -> None:
    assert sort_value({"flag": True}, "flag") == (1, 1)


def test_sort_value_datetime_uses_timestamp() -> None:
    dt = datetime(2026, 1, 1, tzinfo=UTC)
    assert sort_value({"ts": dt}, "ts") == (1, dt.timestamp())


def test_sort_value_list_is_numeric_bucket_zero() -> None:
    assert sort_value({"tags": ["a", "b"]}, "tags") == (1, 0)


def test_sort_value_string_bucket() -> None:
    assert sort_value({"tx_hash": "0xabc"}, "tx_hash") == (2, "0xabc")


def test_sort_value_mixed_types_never_raise() -> None:
    rows = [{"v": 1}, {"v": "x"}, {"v": None}, {"v": 2.5}]
    ordered = sorted(rows, key=lambda r: sort_value(r, "v"))
    assert [r["v"] for r in ordered] == [None, 1, 2.5, "x"]
