"""Whale transaction endpoints.

``GET /api/v1/whales`` returns large-value DEX swaps. Because the
warehouse has no ``token_prices`` table, USD value is computed directly
from the stablecoin side of a pair: for a USDC/WETH swap the USDC amount
IS the USD value of the trade (see :mod:`api.stablecoins`).

The endpoint filters ``dex_events`` to swaps where either token is a
known stablecoin and the stablecoin amount is at least ``min_usd``.
Only swaps with an exact USD derivation are returned — non-stablecoin
pairs are excluded (their USD value is unknowable without a price feed).
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import query
from api.params import (
    chain_clause,
    clamp_limit,
    clamp_offset,
    date_range_clause,
    has_more,
)
from api.schemas import Page, PageMeta, WhaleEvent
from api.stablecoins import STABLECOIN_ADDRESSES, stablecoin_symbol

router = APIRouter(prefix="/api/v1/whales", tags=["whales"])

DEFAULT_MIN_USD = 100_000.0


@router.get("", response_model=Page[WhaleEvent], summary="Large-value (whale) DEX swaps")
def get_whales(
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base)"),
    min_usd: float = Query(
        default=DEFAULT_MIN_USD,
        ge=1.0,
        description="Minimum USD value (stablecoin side of the pair)",
    ),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[WhaleEvent]:
    """Return DEX swaps valued at >= ``min_usd`` USD."""
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    stablecoin_terms: list[str] = []
    chains: list[str] = [chain.strip().lower()] if chain else list(STABLECOIN_ADDRESSES)
    for ch in chains:
        for addr in STABLECOIN_ADDRESSES.get(ch, set()):
            stablecoin_terms.append(f"(token0 = '{addr}' OR token1 = '{addr}')")
    if not stablecoin_terms:
        # No stablecoin map for this chain — nothing can be USD-valued.
        return Page[WhaleEvent](
            data=[],
            pagination=PageMeta(limit=page_limit, offset=page_offset, has_more=False),
        )

    chain_sql, chain_params = chain_clause(chain)
    date_sql, date_params = date_range_clause(start_date, end_date)
    params: dict[str, object] = {**chain_params, **date_params, "min_usd": min_usd}

    stablecoin_filter = "(" + " OR ".join(stablecoin_terms) + ")"
    sql = (
        "SELECT chain, block_number, block_timestamp, tx_hash, dex, version, "
        "contract_address, token0, token1, sender, amount0, amount1 "
        "FROM wallet_memory.dex_events "
        f"WHERE {stablecoin_filter} "
        f"AND (toFloat64OrZero(if(token0 IN {_stablecoin_set(chains)}, amount0, '0')) >= %(min_usd)s "
        f"OR toFloat64OrZero(if(token1 IN {_stablecoin_set(chains)}, amount1, '0')) >= %(min_usd)s)"
        f"{chain_sql}{date_sql} "
        "ORDER BY block_timestamp DESC, block_number DESC "
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
    rows = query(sql, params)

    events = [
        WhaleEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            dex=row["dex"],
            version=row["version"],
            pair_address=row["contract_address"],
            token0=row["token0"],
            token1=row["token1"],
            sender=row["sender"],
            usd_value=_usd_value(row),
            usd_asset=_usd_asset(row),
            amount0=row["amount0"],
            amount1=row["amount1"],
        )
        for row in rows
    ]

    return Page[WhaleEvent](
        data=events,
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(rows), page_limit),
        ),
    )


def _stablecoin_set(chains: list[str]) -> str:
    """Render a ClickHouse IN tuple literal for the stablecoin addresses."""
    addrs = sorted({a for ch in chains for a in STABLECOIN_ADDRESSES.get(ch, set())})
    if not addrs:
        return "('')"
    return "(" + ",".join(f"'{a}'" for a in addrs) + ")"


def _usd_value(row: dict[str, object]) -> float:
    """Return the stablecoin-side amount as USD (float)."""
    token0 = str(row["token0"] or "")
    amount0 = str(row["amount0"] or "0")
    amount1 = str(row["amount1"] or "0")
    chain = str(row.get("chain") or "")
    stable: frozenset[str] = STABLECOIN_ADDRESSES.get(chain, frozenset())
    try:
        if token0 in stable:
            return float(amount0)
        return float(amount1)
    except ValueError:
        return 0.0


def _usd_asset(row: dict[str, object]) -> str:
    """Return the stablecoin symbol used for valuation."""
    token0 = str(row["token0"] or "")
    token1 = str(row["token1"] or "")
    stable: frozenset[str] = STABLECOIN_ADDRESSES.get(str(row.get("chain") or ""), frozenset())
    if token0 in stable:
        return stablecoin_symbol(token0)
    return stablecoin_symbol(token1)


def _fmt_ts(value: object) -> str:
    """Format a ClickHouse DateTime value as ISO-8601."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"
