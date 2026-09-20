"""Shared tables + pure helpers for the :class:`QueryRouter` legs.

Single canonical home for the logical-table → ClickHouse/identity/sort maps
and the small pure functions the legs and the router facade both need.
Kept dependency-free of ``router`` so leg modules can import it without a
circular import.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lite_indexer.config import (
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_SOLANA_TABLE,
    CH_TABLE,
    CH_TRACE_TABLE,
)

# Logical event table → ClickHouse table reference. Mirrors
# lite_indexer.storage.historical._TABLE_REF (kept local so the query
# package is self-contained).
CH_TABLE_REF: dict[str, str] = {
    "evm_transfers": CH_TABLE,
    "dex_events": CH_DEX_TABLE,
    "deployer_events": CH_DEPLOYER_TABLE,
    "internal_transfers": CH_TRACE_TABLE,
    "solana_transfers": CH_SOLANA_TABLE,
}

# Row identity per table (hot/cold/RPC dedupe). Mirrors the THA merge.
ROW_KEYS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("chain", "block_number", "tx_hash", "log_index"),
    "dex_events": ("chain", "block_number", "tx_hash", "log_index"),
    "deployer_events": ("chain", "block_number", "tx_hash", "token_address", "pair_address"),
    "internal_transfers": ("chain", "block_number", "tx_hash", "trace_address"),
    "solana_transfers": ("chain", "slot", "tx_hash", "mint", "token_account", "owner"),
}

# Sort columns per table (newest first). Mirrors the THA merge.
SORT_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("block_timestamp", "block_number", "log_index"),
    "dex_events": ("block_timestamp", "block_number", "log_index"),
    "deployer_events": ("block_timestamp", "block_number"),
    "internal_transfers": ("block_timestamp", "block_number", "tx_hash"),
    "solana_transfers": ("block_timestamp", "slot"),
}


def block_col(table: str) -> str:
    """Return the block-ordering column for a table (slot for Solana)."""
    return "slot" if table == "solana_transfers" else "block_number"


def cold_lit(value: Any) -> str:
    """Render a scalar as a DuckDB/parquet literal for cold-store WHERE."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "1" if value else "0"
    if value is None:
        return "NULL"
    return str(value)


def sort_value(row: dict[str, Any], col: str) -> tuple[int, Any]:
    """Comparable sort key for a row/column (mixed types never raise).

    Missing values sort first (bucket 0), numerics/datetimes in bucket 1,
    strings in bucket 2.
    """
    value = row.get(col)
    if value is None or value == "":
        return (0, 0)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, (int, float)):
        return (1, float(value))
    if isinstance(value, datetime):
        return (1, value.timestamp())
    if isinstance(value, (list, tuple)):
        return (1, 0)
    return (2, str(value))
