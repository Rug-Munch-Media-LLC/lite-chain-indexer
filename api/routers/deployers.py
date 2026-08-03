"""Contract deployer endpoints.

``GET /api/v1/deployers/{address}`` returns pairs/pools deployed by an
address (EVM factory events + Solana mint creations). The table is small
(~1M rows) so the query is served directly from ``deployer_events`` —
no projection needed.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import query
from api.params import (
    chain_clause,
    clamp_limit,
    clamp_offset,
    date_range_clause,
    has_more,
)
from api.schemas import DeployerEvent, Page, PageMeta

router = APIRouter(prefix="/api/v1/deployers", tags=["deployers"])


@router.get("/{address}", response_model=Page[DeployerEvent], summary="Contracts deployed by an address")
def get_deployers(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, solana)"),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[DeployerEvent]:
    """Return pair/pool deployments attributed to ``address``.

    Matches both the raw ``deployer`` column and the resolved
    ``deployer_eoa`` (the tx sender), so factory-originated rows that
    could not be EOA-resolved are still found.
    """
    deployer = address.strip().lower()
    if not deployer:
        raise HTTPException(status_code=422, detail="address must not be empty")

    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    chain_sql, chain_params = chain_clause(chain)
    date_sql, date_params = date_range_clause(start_date, end_date)
    params: dict[str, object] = {"deployer": deployer, **chain_params, **date_params}

    sql = (
        "SELECT chain, block_number, block_timestamp, tx_hash, deployer, "
        "deployer_eoa, token_address, token0, token1, pair_address, type, factory "
        "FROM wallet_memory.deployer_events "
        "WHERE deployer = %(deployer)s OR deployer_eoa = %(deployer)s"
        f"{chain_sql}{date_sql} "
        "ORDER BY block_timestamp DESC, block_number DESC "
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
    rows = query(sql, params)

    events = [
        DeployerEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            deployer=row["deployer"],
            deployer_eoa=row["deployer_eoa"],
            token_address=row["token_address"],
            token0=row["token0"],
            token1=row["token1"],
            pair_address=row["pair_address"],
            type=row["type"],
            factory=row["factory"],
        )
        for row in rows
    ]

    return Page[DeployerEvent](
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
