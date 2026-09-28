"""Wallet-graph read endpoints — funding and counterparties.

``GET /api/v1/funders/{address}``       the funder chain (earliest inbound)
``GET /api/v1/counterparties/{address}`` the counterparty set, both directions

Both are READ endpoints over data the indexer already writes, so they
follow the same conventions as ``/api/v1/transfers`` and
``/api/v1/deployers``: no auth token (every read route in this repo is
public), the shared ``Page[T]`` envelope, ``clamp_limit``/
``clamp_offset`` against ``MAX_PAGE_LIMIT``, and the app-level slowapi
per-IP limiter inherited from ``create_app``.

The SQL lives in ``lite_indexer.query.wallet_graph`` /
``wallet_counterparties`` (dependencies flow DOWN: api/ -> query/); this
module only maps rows to response models.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.db import query
from api.params import clamp_limit, clamp_offset, has_more
from api.schemas import Page, PageMeta
from api.schemas_wallet_graph import Counterparty, FunderLink
from lite_indexer.logging import get_logger
from lite_indexer.query.wallet_counterparties import counterparties as _counterparties
from lite_indexer.query.wallet_graph import earliest_inbound_funder as _earliest_inbound_funder

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["wallet-graph"])


@router.get("/funders/{address}", response_model=Page[FunderLink], summary="Funding chain for a wallet")
def get_funder(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base)"),
) -> Page[FunderLink]:
    """Return the address that funded this wallet (earliest inbound transfer).

    Heuristic, not proof — a contract deployer's first ERC-20 inbound is
    usually not the funding of the deployer, and a bridge recipient's
    funder is the bridge, not the source-chain sender. A wallet that has
    only ever sent returns ``found=false`` with an empty funder.
    """
    wallet = address.strip().lower()
    if not wallet:
        raise HTTPException(status_code=422, detail="address must not be empty")

    row = _earliest_inbound_funder(query, wallet, chain)
    links = [_funder_link(wallet, row, chain)]
    return Page[FunderLink](
        data=links,
        pagination=PageMeta(limit=1, offset=0, has_more=has_more(len(links), 1)),
    )


@router.get(
    "/counterparties/{address}",
    response_model=Page[Counterparty],
    summary="Addresses a wallet has transacted with",
)
def get_counterparties(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base)"),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[Counterparty]:
    """Return the distinct addresses this wallet has transacted with.

    Both directions, aggregated per counterparty. Volumes are raw token
    amounts, not USD.
    """
    wallet = address.strip().lower()
    if not wallet:
        raise HTTPException(status_code=422, detail="address must not be empty")

    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)
    # The window is fetched before the offset is applied so pagination is
    # a real slice of the full ranked set rather than a truncated one.
    rows = _counterparties(query, wallet, chain, limit=page_limit + page_offset)
    page = rows[page_offset : page_offset + page_limit]
    return Page[Counterparty](
        data=[_counterparty(row) for row in page],
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(rows), page_offset + page_limit),
        ),
    )


def _funder_link(wallet: str, row: dict[str, Any] | None, chain: str | None) -> FunderLink:
    """Map one earliest-inbound row (or its absence) to the response model."""
    if not row:
        return FunderLink(address=wallet, chain=chain or "", funder="", found=False)
    return FunderLink(
        address=wallet,
        chain=str(row.get("chain") or chain or ""),
        funder=str(row.get("from_address") or ""),
        funder_tx_hash=str(row.get("tx_hash") or ""),
        funder_block_number=_as_int(row.get("block_number")),
        funder_block_timestamp=_fmt_ts(row.get("block_timestamp")),
        funder_token_address=str(row.get("token_address") or ""),
        funder_value=str(row.get("value") or "0"),
        found=True,
    )


def _counterparty(row: dict[str, Any]) -> Counterparty:
    """Map one aggregated counterparty row to the response model."""
    return Counterparty(
        counterparty=str(row.get("counterparty") or ""),
        chain=str(row.get("chain") or ""),
        first_seen_block=_as_int(row.get("first_seen_block")),
        last_seen_block=_as_int(row.get("last_seen_block")),
        out_volume=str(row.get("out_volume") or "0"),
        in_volume=str(row.get("in_volume") or "0"),
        transfer_count=_as_int(row.get("transfer_count")),
    )


def _as_int(value: Any) -> int:
    """Coerce a ClickHouse numeric column to int (UInt256/UInt64/str).

    ClickHouse returns a huge value for a UInt256 column and a string for a
    low-cardinality one, so the coercion is genuinely lossy at the edges. A
    value we cannot read is logged rather than swallowed: a silent 0 in a
    volume field reads as "this wallet never transacted", which is worse than
    a visible warning.
    """
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        logger.warning("wallet_graph_value_not_numeric", error=str(exc)[:80], raw=str(value)[:48])
        return 0


def _fmt_ts(value: object) -> str:
    """Format a ClickHouse DateTime value as ISO-8601 (matches transfers.py)."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"
