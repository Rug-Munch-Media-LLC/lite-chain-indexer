"""Table metadata + row helpers for Transparent Historical Access (THA).

Extracted from ``storage/historical.py``: the per-table ClickHouse
references, row-identity/sort columns, conservative-SQL keyword blocklist,
the hot/cold split flag, and small pure row helpers.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from lite_indexer.config import (
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_SOLANA_TABLE,
    CH_TABLE,
    CH_TRACE_TABLE,
)

# ClickHouse table reference per THA logical table name.
_TABLE_REF: dict[str, str] = {
    "evm_transfers": CH_TABLE,
    "dex_events": CH_DEX_TABLE,
    "deployer_events": CH_DEPLOYER_TABLE,
    "internal_transfers": CH_TRACE_TABLE,
    "solana_transfers": CH_SOLANA_TABLE,
}

# Logical table → unqualified ClickHouse table name (for view names and
# system.tables lookups).
_TABLE_NAME: dict[str, str] = {
    "evm_transfers": "evm_transfers",
    "dex_events": "dex_events",
    "deployer_events": "deployer_events",
    "internal_transfers": "internal_transfers",
    "solana_transfers": "solana_spl_balance_changes",
}

# Row identity per table (used for hot/cold dedupe).
_ROW_KEY_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("chain", "block_number", "tx_hash", "log_index"),
    "dex_events": ("chain", "block_number", "tx_hash", "log_index"),
    "deployer_events": ("chain", "block_number", "tx_hash", "token_address", "pair_address"),
    "internal_transfers": ("chain", "block_number", "tx_hash", "trace_address"),
    "solana_transfers": ("chain", "slot", "tx_hash", "mint", "token_account", "owner"),
}

# Sort columns per table for the merged result (descending). Solana has no
# log_index; deployer_events/internal_transfers have no log_index either.
_SORT_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("block_timestamp", "block_number", "log_index"),
    "dex_events": ("block_timestamp", "block_number", "log_index"),
    "deployer_events": ("block_timestamp", "block_number"),
    "internal_transfers": ("block_timestamp", "block_number", "tx_hash"),
    "solana_transfers": ("block_timestamp", "slot"),
}

# How long (seconds) a *_historical view-existence check is cached.
_VIEW_CHECK_TTL = 60

# Conservative SQL routing: reject statements that reference anything
# other than a plain SELECT over one supported event table.
_UNCLEAN_KEYWORDS = (
    " INSERT ",
    " ALTER ",
    " CREATE ",
    " DROP ",
    " TRUNCATE ",
    " DELETE ",
    " UPDATE ",
    " GRANT ",
    " INTO ",
    " JOIN ",
    " GROUP BY ",
    " HAVING ",
    " UNION ",
)


@dataclass
class _Split:
    """Whether a query range needs the hot and/or cold tier."""

    hot: bool
    cold: bool


def _block_col(table: str) -> str:
    """Return the block-ordering column for a table (slot for Solana)."""
    return "slot" if table == "solana_transfers" else "block_number"


def _sort_value(row: dict[str, Any], col: str) -> tuple[int, Any]:
    """Return a comparable sort key for a row/column pair.

    Missing values sort first (bucket 0), numerics/datetimes in bucket 1,
    strings in bucket 2 — so mixed-type rows never raise on comparison.
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


def _sql_lit(value: Any) -> str:
    """Render a Python scalar as a SQL literal (DuckDB quote-doubling — SEC-1)."""
    if isinstance(value, (datetime,)):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


async def asyncio_to_thread_with_timeout(
    func: Any, timeout: float, *args: Any, **kwargs: Any
) -> Any:
    """Run a blocking call in a worker thread bounded by ``timeout``.

    DuckDB has no cancellable statement timeout, so ``COLD_QUERY_TIMEOUT``
    is enforced here: the caller never waits longer than ``timeout``, even
    though the worker thread may finish in the background.
    """
    return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout)
