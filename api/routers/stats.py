"""Chain-wide statistics endpoints.

``GET /api/v1/stats`` returns aggregate counts (total transfers, unique
wallets, raw volume) across the warehouse, optionally filtered by chain
and date range. Because aggregations over 297M rows are expensive, the
result is cached in-process for a short TTL (default 60s).
"""

from __future__ import annotations

import threading
import time

from fastapi import APIRouter, Query

from api.db import query
from api.params import chain_clause, date_range_clause
from api.schemas import ChainStats

router = APIRouter(prefix="/api/v1", tags=["stats"])

STATS_TTL_SECONDS = 60

_cache_lock = threading.Lock()
_cache: dict[str, tuple[float, ChainStats]] = {}


@router.get("/stats", response_model=ChainStats, summary="Chain-wide aggregate statistics")
def get_stats(
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum)"),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
) -> ChainStats:
    """Return total transfers, unique wallets/tokens, and per-table counts."""
    cache_key = f"{chain or '*'}::{start_date or ''}::{end_date or ''}"
    now = time.monotonic()
    with _cache_lock:
        cached = _cache.get(cache_key)
        if cached and now - cached[0] < STATS_TTL_SECONDS:
            return cached[1]

    stats = _compute_stats(chain, start_date, end_date)
    with _cache_lock:
        _cache[cache_key] = (time.monotonic(), stats)
    return stats


def _compute_stats(
    chain: str | None,
    start_date: str | None,
    end_date: str | None,
) -> ChainStats:
    chain_sql, chain_params = chain_clause(chain)
    date_sql, date_params = date_range_clause(start_date, end_date)
    params: dict[str, object] = {**chain_params, **date_params}

    main = query(
        "SELECT count() AS total_transfers, "
        "uniqCombined(from_address) AS unique_senders, "
        "uniqCombined(to_address) AS unique_receivers, "
        "uniqCombined(token_address) AS unique_tokens, "
        "sum(toFloat64OrZero(value)) AS total_raw_value, "
        "min(block_timestamp) AS min_ts, "
        "max(block_timestamp) AS max_ts "
        "FROM wallet_memory.evm_transfers "
        f"WHERE 1=1{chain_sql}{date_sql}",
        params,
    )[0]

    dex = query(
        f"SELECT count() FROM wallet_memory.dex_events WHERE 1=1{chain_sql}",
        params,
    )[0]["count()"]
    deployers = query(
        f"SELECT count() FROM wallet_memory.deployer_events WHERE 1=1{chain_sql}",
        params,
    )[0]["count()"]
    solana = query(
        "SELECT count() FROM wallet_memory.solana_spl_balance_changes WHERE 1=1",
        {},
    )[0]["count()"]
    labels = query(
        "SELECT count() FROM wallet_memory.wallet_labels WHERE 1=1",
        {},
    )[0]["count()"]

    return ChainStats(
        chain=chain.strip().lower() if chain else None,
        total_transfers=int(main["total_transfers"]),
        unique_senders=int(main["unique_senders"]),
        unique_receivers=int(main["unique_receivers"]),
        unique_tokens=int(main["unique_tokens"]),
        total_raw_value=_fmt_num(main["total_raw_value"]),
        dex_swaps=int(dex),
        deployer_events=int(deployers),
        solana_balance_changes=int(solana),
        wallet_labels=int(labels),
        min_block_timestamp=_fmt_ts(main["min_ts"]),
        max_block_timestamp=_fmt_ts(main["max_ts"]),
    )


def _fmt_ts(value: object) -> str | None:
    """Format a ClickHouse DateTime value as ISO-8601."""
    if not value:
        return None
    return str(value).replace(" ", "T") + "Z"


def _fmt_num(value: object) -> str:
    """Format a raw numeric sum as a decimal string."""
    try:
        return f"{float(str(value)):.6f}"
    except (TypeError, ValueError):
        return str(value or "0")
