"""Token launch + Jito bundle endpoints.

``GET /api/v1/launches`` returns recent token launches across the three
launch tables the indexer writes (see migrations 007/008/009/011):

* ``solana_launches`` — launchpad create + AMM-graduation signals
  (pump.fun, LaunchLab, PumpSwap, Raydium) detected from the same
  ``getTransaction`` payloads the Solana strategy already fetches.
* ``tron_launches`` — SunPump TokenCreate + SunSwap PairCreated +
  graduation events.
* ``evm_launches`` — DEX pair/pool creation (PairCreated / PoolCreated)
  from known factories, mirrored from the deployer path
  (:mod:`lite_indexer.strategies.evm.launch`).

The three tables are UNIONed with a normalized column shape. ``symbol``
is resolved via ``token_meta_ref`` when a metadata row exists (empty
otherwise); ``liquidity``/``mcap`` are not tracked by any launch table
and are always null (see :class:`api.schemas.LaunchEvent`).

``GET /api/v1/bundles`` returns recent Jito bundled launches from
``solana_bundles`` (same-slot create + fresh-wallet buys + tip transfer).
"""

from __future__ import annotations

import time
from typing import Any

from fastapi import APIRouter, Query

from api.db import query
from api.params import (
    chain_clause,
    clamp_limit,
    clamp_offset,
    date_range_clause,
    has_more,
)
from api.schemas import BundleEvent, LaunchEvent, Page, PageMeta

router = APIRouter(prefix="/api/v1", tags=["launches"])

# The per-branch ORDER BY / LIMIT is applied inside the UNION subquery
# (ClickHouse evaluates LIMIT per branch), so a small default limit per
# table is safe even when the outer page limit is large.
_BRANCH_LIMIT = 500

# Normalized launch columns produced by every UNION branch:
# chain, block_timestamp, address, creator, dex, tx_hash, event_type,
# launch_signal, graduation_signal, confidence, source.
_SOLANA_BRANCH = (
    "SELECT chain, block_timestamp, mint AS address, '' AS symbol, creator, "
    "venue AS dex, signature AS tx_hash, 'launchpad' AS event_type, "
    "launch_signal, graduation_signal, confidence, source "
    "FROM wallet_memory.solana_launches"
)
_TRON_BRANCH = (
    "SELECT chain, block_timestamp, token_address AS address, '' AS symbol, creator, "
    "if(factory != '', factory, event_type) AS dex, tx_hash, "
    "event_type, 0 AS launch_signal, 0 AS graduation_signal, "
    "'high' AS confidence, source "
    "FROM wallet_memory.tron_launches"
)
_EVM_BRANCH = (
    "SELECT chain, block_timestamp, token0 AS address, '' AS symbol, '' AS creator, "
    "factory AS dex, tx_hash, 'pair_created' AS event_type, "
    "0 AS launch_signal, 0 AS graduation_signal, 'high' AS confidence, source "
    "FROM wallet_memory.evm_launches"
)


@router.get("/launches", response_model=Page[LaunchEvent], summary="Recent token launches")
def get_launches(
    chain: str | None = Query(default=None, description="Chain filter (e.g. solana, tron, ethereum, base)"),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[LaunchEvent]:
    """Return recent token launches across Solana / Tron / EVM chains."""
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    chain_sql, chain_params = chain_clause(chain, column="l.chain")
    date_sql, date_params = date_range_clause(start_date, end_date, column="l.block_timestamp")
    params: dict[str, object] = {**chain_params, **date_params}

    # Per-branch LIMIT keeps the UNION bounded; the outer query applies the
    # real page limit after ordering. LEFT JOIN token_meta_ref resolves the
    # symbol for tokens with a metadata row (best-effort — empty otherwise).
    sql = (
        "SELECT l.chain, l.block_timestamp, l.address, "
        "if(tm.symbol = '', l.symbol, tm.symbol) AS symbol, "
        "l.creator, l.dex, l.tx_hash, l.event_type, "
        "l.launch_signal, l.graduation_signal, l.confidence "
        "FROM ("
        f"SELECT * FROM ({_SOLANA_BRANCH} ORDER BY block_timestamp DESC LIMIT {_BRANCH_LIMIT}) "
        "UNION ALL "
        f"SELECT * FROM ({_TRON_BRANCH} ORDER BY block_timestamp DESC LIMIT {_BRANCH_LIMIT}) "
        "UNION ALL "
        f"SELECT * FROM ({_EVM_BRANCH} ORDER BY block_timestamp DESC LIMIT {_BRANCH_LIMIT})"
        ") l "
        "LEFT JOIN wallet_memory.token_meta_ref tm "
        "ON tm.chain = l.chain AND tm.token_address = l.address "
        f"WHERE 1=1{chain_sql}{date_sql} "
        "ORDER BY l.block_timestamp DESC "
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
    rows = query(sql, params)

    now = time.time()
    events = [_launch_event(row, now) for row in rows]

    return Page[LaunchEvent](
        data=events,
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(rows), page_limit),
        ),
    )


@router.get("/bundles", response_model=Page[BundleEvent], summary="Recent Jito bundled launches")
def get_bundles(
    chain: str | None = Query(default=None, description="Chain filter (solana only in practice)"),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[BundleEvent]:
    """Return recent Jito bundled token launches (Solana)."""
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    chain_sql, chain_params = chain_clause(chain)
    date_sql, date_params = date_range_clause(start_date, end_date)
    params: dict[str, object] = {**chain_params, **date_params}

    sql = (
        "SELECT chain, slot, block_timestamp, create_tx_signature, "
        "tip_tx_signature, tip_account, mint, buyer_count, buyer_wallets "
        "FROM wallet_memory.solana_bundles "
        f"WHERE 1=1{chain_sql}{date_sql} "
        "ORDER BY block_timestamp DESC, slot DESC "
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
    rows = query(sql, params)

    now = time.time()
    events = [_bundle_event(row, now) for row in rows]

    return Page[BundleEvent](
        data=events,
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(rows), page_limit),
        ),
    )


def _fmt_ts(value: object) -> str:
    """Format a ClickHouse DateTime value as ISO-8601."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"


def _age_seconds(ts_iso: str, now: float) -> int:
    """Seconds between an ISO timestamp and ``now`` (0 on parse failure)."""
    from datetime import datetime

    if not ts_iso:
        return 0
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
    except ValueError:
        return 0
    return max(0, int(now - dt.timestamp()))


def _launch_event(row: dict[str, Any], now: float) -> LaunchEvent:
    """Map a normalized launch row to the response model."""
    ts = _fmt_ts(row.get("block_timestamp"))
    return LaunchEvent(
        chain=str(row.get("chain") or ""),
        address=str(row.get("address") or ""),
        symbol=str(row.get("symbol") or ""),
        creator=str(row.get("creator") or ""),
        dex=str(row.get("dex") or ""),
        block_timestamp=ts,
        age_seconds=_age_seconds(ts, now),
        tx_hash=str(row.get("tx_hash") or ""),
        event_type=str(row.get("event_type") or ""),
        launch_signal=int(row.get("launch_signal") or 0),
        graduation_signal=int(row.get("graduation_signal") or 0),
        confidence=str(row.get("confidence") or ""),
    )


def _bundle_event(row: dict[str, Any], now: float) -> BundleEvent:
    """Map a solana_bundles row to the response model."""
    ts = _fmt_ts(row.get("block_timestamp"))
    wallets = row.get("buyer_wallets") or []
    if isinstance(wallets, (list, tuple)):
        wallet_list = [str(w) for w in wallets]
    else:
        wallet_list = [str(wallets)] if wallets else []
    return BundleEvent(
        chain=str(row.get("chain") or "solana"),
        slot=int(row.get("slot") or 0),
        block_timestamp=ts,
        create_tx_signature=str(row.get("create_tx_signature") or ""),
        tip_tx_signature=str(row.get("tip_tx_signature") or ""),
        tip_account=str(row.get("tip_account") or ""),
        mint=str(row.get("mint") or ""),
        buyer_count=int(row.get("buyer_count") or 0),
        buyer_wallets=wallet_list,
        age_seconds=_age_seconds(ts, now),
    )
