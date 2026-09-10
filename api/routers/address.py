"""Wallet-tracing endpoints over the immutable address-activity keystone.

Three consumer-facing reads:

* ``GET /api/v1/address/{address}/activity`` — every transaction the
  wallet touched, ordered newest-first and deduplicated, served from
  ``wallet_memory.address_activity`` (the wallet-tracing keystone table).
* ``GET /api/v1/address/{address}/funders`` — inbound trace: who sent
  value *to* this wallet, with per-counterparty counts and first/last
  seen.
* ``GET /api/v1/address/{address}/counters`` — outbound trace: who this
  wallet sent value *to*.

**Closed accounts need no special handling.** ``address_activity`` (and
the trace table behind the funder/counter reads) is append-only, so a
wallet that has since been closed, drained, or burned still returns its
complete history from these endpoints — there is no "account exists"
check and no live-RPC dependency. The query is keyed purely on the
address string.

Scope: the ``address_activity`` keystone is Solana-shaped today (its
ingestion pipeline is Solana-only), so ``activity`` accepts a ``chain``
filter for forward-compatibility but only ``chain=solana`` (or no chain)
returns rows. ``funders``/``counters`` read ``wallet_memory.solana_transfers``
for value direction and amounts, since ``address_activity`` records
involvement, not value flow.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import get_query_router, normalize_date, query
from api.ondemand import run_ondemand
from api.params import clamp_limit, clamp_offset, has_more
from api.schemas import AddressActivity, AddressCounterparty, Page, PageMeta
from lite_indexer.config import ADDRESS_BACKFILL_LAZY_MAX_TX, ON_DEMAND_RPC_ENABLED

router = APIRouter(prefix="/api/v1/address", tags=["address"])

_ACTIVITY_COLUMNS = "address, tx_signature, slot, block_time, involved_accounts, program_ids, is_signer"


def _normalize_address(address: str) -> str:
    """Trim the wallet address.

    Solana base58 addresses are case-sensitive, so the value is **not**
    lowercased (unlike the EVM ``/transfers`` path).
    """
    wallet = address.strip()
    if not wallet:
        raise HTTPException(status_code=422, detail="address must not be empty")
    return wallet


def _time_clause(after: str | None, before: str | None) -> tuple[str, dict[str, object]]:
    """Return (SQL fragment, params) for an inclusive ``after``/``before`` range.

    Values accept ``YYYY-MM-DD`` or full ISO-8601 datetimes. Unparsable
    values are ignored (best-effort, matching ``date_range_clause``).
    """
    clauses: list[str] = []
    params: dict[str, object] = {}
    after_ts = normalize_date(after)
    if after_ts is not None:
        clauses.append(" AND block_time >= %(after_ts)s")
        params["after_ts"] = after_ts
    before_ts = normalize_date(before)
    if before_ts is not None:
        clauses.append(" AND block_time <= %(before_ts)s")
        params["before_ts"] = before_ts
    return "".join(clauses), params


@router.get(
    "/{address}/activity",
    response_model=Page[AddressActivity],
    summary="Full wallet history (closed accounts included)",
)
def get_address_activity(
    address: str,
    chain: str | None = Query(
        default=None,
        description="Chain filter (e.g. 'solana', 'ethereum', 'base', 'tron')",
    ),
    after: str | None = Query(
        default=None,
        description="Inclusive lower bound on block_time (YYYY-MM-DD or ISO-8601)",
    ),
    before: str | None = Query(
        default=None,
        description="Inclusive upper bound on block_time (YYYY-MM-DD or ISO-8601)",
    ),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[AddressActivity]:
    """Return every transaction that touched ``address``, newest first.

    Reads ``wallet_memory.address_activity`` with ``FINAL`` so the
    ReplacingMergeTree dedups re-indexed transactions on
    ``(address, tx_signature)`` before ordering. Unknown addresses and
    wallets with no history return an empty ``data`` list (HTTP 200), not
    an error — an address with no rows is a valid, if uneventful, answer.

    Because the history is immutable this endpoint works identically for
    closed accounts (closed token accounts, drained wallets, burned
    addresses): no account-existence check is performed.
    """
    wallet = _normalize_address(address)
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    time_sql, time_params = _time_clause(after, before)
    params: dict[str, object] = {"address": wallet, **time_params}
    chain_clause = ""
    if chain:
        params["chain"] = chain.strip().lower()
        chain_clause = "AND chain = %(chain)s"
    sql = (
        f"SELECT {_ACTIVITY_COLUMNS} "
        "FROM wallet_memory.address_activity FINAL "
        "WHERE address = %(address)s"
        f"{chain_clause} "
        f"{time_sql} "
        "ORDER BY block_time DESC, slot DESC "
        f"LIMIT {page_limit} OFFSET {page_offset}"
    )
    rows = query(sql, params)

    # Lazy backfill on first touch: when the keystone has no rows for this
    # address yet, fetch its history once from the free RPC pool (guarded by
    # the never-re-fetch table), then re-read locally. Best-effort — a
    # disabled/unreachable backfill leaves the (empty) local answer intact.
    if not rows and ON_DEMAND_RPC_ENABLED:
        od_router = get_query_router()
        if od_router is not None:
            try:
                run_ondemand(od_router.backfill_address(chain or "solana", wallet, max_tx=ADDRESS_BACKFILL_LAZY_MAX_TX))
                rows = query(sql, params)
            except Exception:  # noqa: BLE001 — lazy backfill is best-effort
                rows = []

    events = [
        AddressActivity(
            address=row["address"],
            tx_signature=row["tx_signature"],
            slot=int(row["slot"]),
            block_time=_fmt_ts(row["block_time"]),
            involved_accounts=[str(a) for a in (row.get("involved_accounts") or [])],
            program_ids=[str(p) for p in (row.get("program_ids") or [])],
            is_signer=bool(row.get("is_signer")),
        )
        for row in rows
    ]
    return Page[AddressActivity](
        data=events,
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(rows), page_limit),
        ),
    )


@router.get(
    "/{address}/funders",
    response_model=Page[AddressCounterparty],
    summary="Inbound trace — who funded this wallet",
)
def get_address_funders(
    address: str,
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[AddressCounterparty]:
    """Return the addresses that sent value *to* ``address``.

    Aggregates ``wallet_memory.solana_transfers`` (one row per value
    movement) by ``from_address``, so each funder carries its distinct
    transaction count, transfer-row count and first/last seen times.
    Rows with an empty sender (account-create / rent events) are
    excluded. An address with no inbound value returns an empty list.
    """
    wallet = _normalize_address(address)
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)
    return _counterparty_trace(wallet, "in", page_limit, page_offset)


@router.get(
    "/{address}/counters",
    response_model=Page[AddressCounterparty],
    summary="Outbound trace — who this wallet sent to",
)
def get_address_counters(
    address: str,
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[AddressCounterparty]:
    """Return the addresses that received value *from* ``address``.

    The outbound mirror of :func:`get_address_funders`: aggregates
    ``wallet_memory.solana_transfers`` by ``to_address``. An address with
    no outbound value returns an empty list.
    """
    wallet = _normalize_address(address)
    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)
    return _counterparty_trace(wallet, "out", page_limit, page_offset)


def _counterparty_trace(
    wallet: str,
    direction: str,
    limit: int,
    offset: int,
) -> Page[AddressCounterparty]:
    """Aggregate one side of a wallet's value trace by counterparty.

    ``direction='in'`` groups inbound transfers by sender (funders);
    ``direction='out'`` groups outbound transfers by recipient (counters).
    """
    if direction == "in":
        match_col, group_col = "to_address", "from_address"
    else:
        match_col, group_col = "from_address", "to_address"

    sql = (
        f"SELECT {group_col} AS counterparty, "
        "count() AS transfer_count, uniqExact(signature) AS tx_count, "
        "min(block_timestamp) AS first_seen, max(block_timestamp) AS last_seen "
        "FROM wallet_memory.solana_transfers "
        f"WHERE {match_col} = %(address)s AND {group_col} != '' "
        f"GROUP BY {group_col} "
        "ORDER BY transfer_count DESC, counterparty ASC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    rows = query(sql, {"address": wallet})
    counterparties = [
        AddressCounterparty(
            address=str(row["counterparty"]),
            tx_count=int(row["tx_count"]),
            transfer_count=int(row["transfer_count"]),
            first_seen=_fmt_ts(row["first_seen"]),
            last_seen=_fmt_ts(row["last_seen"]),
        )
        for row in rows
    ]
    return Page[AddressCounterparty](
        data=counterparties,
        pagination=PageMeta(
            limit=limit,
            offset=offset,
            has_more=has_more(len(rows), limit),
        ),
    )


def _fmt_ts(value: object) -> str:
    """Format a ClickHouse DateTime/DateTime64 value as ISO-8601 UTC."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"
