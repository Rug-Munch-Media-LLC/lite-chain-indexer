"""Wallet transfer history endpoints.

``GET /api/v1/transfers/{address}`` returns transfers where the wallet
appeared as sender or receiver, across all indexed chains.

Two query modes:

* **summary** (default) — served by the address-first wallet projections
  (``wallet_out_proj`` / ``wallet_in_proj``). Sub-second after the
  projections are materialized. Returns the wallet side as ``address``
  with a ``direction`` label ('in'/'out'); the counterparty is omitted.
* **full** (``include_counterparty=true``) — reads the main
  ``evm_transfers`` table and returns the real ``from_address`` and
  ``to_address`` for every row. Slower (address is not in the table
  primary key) but complete.

Both modes support ``chain``, ``direction``, ``start_date``/``end_date``
filters and ``limit``/``offset`` pagination.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from api.db import query
from api.ondemand import run_ondemand
from api.params import (
    chain_clause,
    clamp_limit,
    clamp_offset,
    date_range_clause,
    has_more,
)
from api.schemas import Page, PageMeta, TransferEvent
from lite_indexer.config import ON_DEMAND_RPC_ENABLED

router = APIRouter(prefix="/api/v1/transfers", tags=["transfers"])

_FULL_COLUMNS = (
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "from_address",
    "to_address",
    "token_address",
    "value",
)


@router.get("/{address}", response_model=Page[TransferEvent], summary="All transfers for a wallet")
def get_transfers(
    address: str,
    chain: str | None = Query(default=None, description="Chain filter (e.g. ethereum, base, solana)"),
    direction: str = Query(default="all", pattern="^(all|in|out)$"),
    start_date: str | None = Query(default=None, description="Inclusive start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Inclusive end date (YYYY-MM-DD)"),
    include_counterparty: bool = Query(
        default=False,
        description="Return real from/to addresses (slower full-table read)",
    ),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int | None = Query(default=None, ge=0),
) -> Page[TransferEvent]:
    """Return transfer events involving ``address`` as sender or receiver."""
    wallet = address.strip().lower()
    if not wallet:
        raise HTTPException(status_code=422, detail="address must not be empty")

    page_limit = clamp_limit(limit)
    page_offset = clamp_offset(offset)

    chain_sql, chain_params = chain_clause(chain)
    date_sql, date_params = date_range_clause(start_date, end_date)
    base_params = {"address": wallet, **chain_params, **date_params}

    if chain and chain.strip().lower() == "solana":
        # Solana transfers live in dedicated tables — SPL balance changes
        # (summary mode) and trace-level solana_transfers (full mode). The
        # EVM tables below are empty for chain='solana'.
        if include_counterparty:
            events = _solana_full_history(wallet, direction, chain_sql, date_sql, base_params, page_limit, page_offset)
        else:
            events = _solana_summary_history(
                wallet, direction, chain_sql, date_sql, base_params, page_limit, page_offset
            )
    elif include_counterparty:
        events = _full_history(wallet, chain_sql, date_sql, base_params, page_limit, page_offset)
    else:
        events = _summary_history(wallet, direction, chain_sql, date_sql, base_params, page_limit, page_offset)

    # On-demand RPC fallback: the indexer never says "data not available".
    # When the indexed stores (ClickHouse + B2 parquet) have no rows for
    # this wallet on a *specific* chain, the QueryRouter dials the chain's
    # RPC pool for the most recent on-demand window (bounded, cached,
    # rate-limit protected). Unbounded (all-chain) queries skip the fallback
    # because RPC eth_getLogs is per-chain.
    if not events and chain and ON_DEMAND_RPC_ENABLED:
        events = _on_demand_transfers(wallet, chain, page_limit, include_counterparty)

    return Page[TransferEvent](
        data=events,
        pagination=PageMeta(
            limit=page_limit,
            offset=page_offset,
            has_more=has_more(len(events), page_limit),
        ),
    )


def _summary_history(
    wallet: str,
    direction: str,
    chain_sql: str,
    date_sql: str,
    base_params: dict[str, object],
    limit: int,
    offset: int,
) -> list[TransferEvent]:
    """Wallet history served by the address-first projections (fast).

    Only the projection columns are selected (no counterparty), keeping
    the query satisfiable by ``wallet_out_proj`` / ``wallet_in_proj``.
    """
    # Inner selects carry only projection columns; the wallet address is
    # known from the URL and attached in the outer select as `address`.
    selects: list[str] = []
    for addr_col, label in (("from_address", "out"), ("to_address", "in")):
        if direction != "all" and direction != label:
            continue
        selects.append(
            f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
            f"token_address, value, '{label}' AS direction "
            f"FROM wallet_memory.evm_transfers "
            f"WHERE {addr_col} = %(address)s{chain_sql}{date_sql}"
        )
    if not selects:
        return []
    union = " UNION ALL ".join(selects)
    sql = (
        f"SELECT * FROM ("
        f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
        f"token_address, value, %(address)s AS address, direction FROM ({union})"
        f") ORDER BY block_timestamp DESC, block_number DESC, log_index DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    rows = query(sql, base_params)
    return [
        TransferEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            log_index=row["log_index"],
            from_address=row["address"] if row["direction"] == "out" else "",
            to_address=row["address"] if row["direction"] == "in" else "",
            token_address=row["token_address"],
            value=row["value"],
            direction=row["direction"],
        )
        for row in rows
    ]


def _full_history(
    wallet: str,
    chain_sql: str,
    date_sql: str,
    base_params: dict[str, object],
    limit: int,
    offset: int,
) -> list[TransferEvent]:
    """Wallet history from the main table (full fidelity, slower)."""
    cols = ", ".join(_FULL_COLUMNS)
    sql = (
        f"SELECT {cols} FROM ("
        f"SELECT {cols} FROM wallet_memory.evm_transfers "
        f"WHERE from_address = %(address)s{chain_sql}{date_sql} "
        f"UNION ALL "
        f"SELECT {cols} FROM wallet_memory.evm_transfers "
        f"WHERE to_address = %(address)s{chain_sql}{date_sql}"
        f") ORDER BY block_timestamp DESC, block_number DESC, log_index DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    rows = query(sql, base_params)
    return [
        TransferEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            log_index=row["log_index"],
            from_address=row["from_address"],
            to_address=row["to_address"],
            token_address=row["token_address"],
            value=row["value"],
            direction="out" if row["from_address"] == wallet else "in",
        )
        for row in rows
    ]


def _solana_summary_history(
    wallet: str,
    direction: str,
    chain_sql: str,
    date_sql: str,
    base_params: dict[str, object],
    limit: int,
    offset: int,
) -> list[TransferEvent]:
    """Wallet history from SPL balance changes (fast, summary shape).

    Reads ``solana_spl_balance_changes`` where the wallet is the token
    account ``owner``. The per-account balance ``delta`` encodes
    direction: positive = received (in), negative = sent (out). The
    slot takes the role of ``block_number`` so the response keeps the
    same sort shape as the EVM path; ``log_index`` has no Solana
    equivalent and is 0.
    """
    direction_sql = ""
    if direction == "in":
        direction_sql = " AND toInt256(delta) > 0"
    elif direction == "out":
        direction_sql = " AND toInt256(delta) < 0"
    sql = (
        "SELECT chain, slot AS block_number, block_timestamp, tx_hash, "
        "mint AS token_address, owner AS address, "
        "if(toInt256(delta) < 0, 'out', 'in') AS direction, abs(toInt256(delta)) AS value "
        "FROM wallet_memory.solana_spl_balance_changes "
        f"WHERE owner = %(address)s{chain_sql}{date_sql}{direction_sql} "
        "ORDER BY block_timestamp DESC, slot DESC "
        f"LIMIT {limit} OFFSET {offset}"
    )
    rows = query(sql, base_params)
    return [
        TransferEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            log_index=0,
            from_address=row["address"] if row["direction"] == "out" else "",
            to_address=row["address"] if row["direction"] == "in" else "",
            token_address=row["token_address"],
            value=str(row["value"]),
            direction=row["direction"],
        )
        for row in rows
    ]


def _solana_full_history(
    wallet: str,
    direction: str,
    chain_sql: str,
    date_sql: str,
    base_params: dict[str, object],
    limit: int,
    offset: int,
) -> list[TransferEvent]:
    """Wallet history from trace-level ``solana_transfers`` (full shape).

    Rows carry real ``from_address`` / ``to_address`` / ``mint``. The
    slot takes the role of ``block_number``; ``signature`` maps to
    ``tx_hash`` and ``log_index`` is 0 (the trace table has no EVM-style
    log index).
    """
    selects: list[str] = []
    for addr_col, label in (("from_address", "out"), ("to_address", "in")):
        if direction != "all" and direction != label:
            continue
        selects.append(
            "SELECT chain, slot AS block_number, block_timestamp, "
            "signature AS tx_hash, mint AS token_address, "
            "from_address, to_address, amount AS value "
            f"FROM wallet_memory.solana_transfers "
            f"WHERE {addr_col} = %(address)s{chain_sql}{date_sql}"
        )
    if not selects:
        return []
    union = " UNION ALL ".join(selects)
    sql = f"SELECT * FROM ({union}) ORDER BY block_timestamp DESC, block_number DESC LIMIT {limit} OFFSET {offset}"
    rows = query(sql, base_params)
    return [
        TransferEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]),
            tx_hash=row["tx_hash"],
            log_index=0,
            from_address=row["from_address"],
            to_address=row["to_address"],
            token_address=row["token_address"],
            value=str(row["value"]),
            direction="out" if row["from_address"] == wallet else "in",
        )
        for row in rows
    ]


def _fmt_ts(value: object) -> str:
    """Format a ClickHouse DateTime value as ISO-8601."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"


def _on_demand_transfers(
    wallet: str,
    chain: str,
    limit: int,
    include_counterparty: bool,
) -> list[TransferEvent]:
    """On-demand RPC fallback for a wallet the indexed stores don't cover.

    Runs the async :class:`lite_indexer.query.QueryRouter` on the API's
    dedicated event loop (see :mod:`api.ondemand`) and returns transfers
    from the chain's most recent on-demand window. Best-effort: any
    failure (timeout, RPC error, ClickHouse probe) degrades to the empty
    indexed-only result.
    """
    from api.db import get_query_router

    router = get_query_router()
    if router is None:
        return []
    try:
        rows = run_ondemand(router.get_transfers(chain, wallet, limit=limit))
    except Exception:  # noqa: BLE001 — fallback is best-effort
        return []
    events: list[TransferEvent] = []
    for row in rows:
        from_address = str(row.get("from_address", ""))
        to_address = str(row.get("to_address", ""))
        direction = "out" if from_address == wallet else "in"
        if include_counterparty:
            shown_from, shown_to = from_address, to_address
        else:
            # Summary-mode shape: expose only the wallet side as `address`.
            shown_from = wallet if direction == "out" else ""
            shown_to = wallet if direction == "in" else ""
        events.append(
            TransferEvent(
                chain=str(row.get("chain", chain)),
                block_number=int(row.get("block_number", 0) or 0),
                block_timestamp=_fmt_ts(row.get("block_timestamp")),
                tx_hash=str(row.get("tx_hash", "")),
                log_index=int(row.get("log_index", 0) or 0),
                from_address=shown_from,
                to_address=shown_to,
                token_address=str(row.get("token_address", "")),
                value=str(row.get("value", "")),
                direction=direction,
            )
        )
    return events
