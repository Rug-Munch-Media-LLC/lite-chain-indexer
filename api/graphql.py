"""GraphQL read API — competitive parity with GraphQL-first indexers.

Ponder, Subsquid, HyperIndex and The Graph all expose **GraphQL** as their
primary read interface. LiteChainIndexer's REST API already covers the same
queries; this module adds a thin, read-only GraphQL schema over the same
ClickHouse warehouse so GraphQL-native tooling can consume the data without
leaving the GraphQL ecosystem.

Scope is deliberately minimal — three high-value queries:

* ``transfers(address, ...)`` — wallet-native transfer history (address-first)
* ``chainStats(chain)``       — chain-wide aggregate counts
* ``whales(chain, minUsd)``   — large-value DEX swaps (USD via stablecoin side)

The REST API (``api/routers``) remains the full-featured surface. Resolvers
reuse the exact same SQL and guardrails (``api.db.query``) as the REST
routers, so behavior and rate limits are identical. Because
``clickhouse_driver`` is not async, every resolver hops to a worker thread
with ``asyncio.to_thread`` to keep the event loop free.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import strawberry
from strawberry.fastapi import GraphQLRouter

from api.db import query
from api.params import chain_clause, clamp_limit
from api.stablecoins import STABLECOIN_ADDRESSES, stablecoin_symbol

# ── GraphQL object types ──────────────────────────────────────────────


@strawberry.type
class Transfer:
    """A single ERC-20/TRC-20/SPL transfer event involving a wallet."""

    chain: str
    block_number: int
    block_timestamp: str
    tx_hash: str
    log_index: int
    token_address: str
    value: str
    address: str
    direction: str


@strawberry.type
class ChainStats:
    """Chain-wide aggregate statistics."""

    chain: str | None
    total_transfers: int
    unique_senders: int
    unique_receivers: int
    unique_tokens: int
    total_raw_value: str
    dex_swaps: int
    deployer_events: int
    solana_balance_changes: int
    wallet_labels: int
    min_block_timestamp: str | None
    max_block_timestamp: str | None


@strawberry.type
class WhaleEvent:
    """A large-value DEX swap, USD-valued via its stablecoin side."""

    chain: str
    block_number: int
    block_timestamp: str
    tx_hash: str
    dex: str
    version: str
    pair_address: str
    token0: str
    token1: str
    sender: str
    usd_value: float
    usd_asset: str


# ── Resolvers ─────────────────────────────────────────────────────────


def _fmt_ts(value: object) -> str | None:
    """Format a ClickHouse DateTime value as ISO-8601."""
    if not value:
        return None
    return str(value).replace(" ", "T") + "Z"


def _fmt_num(value: object) -> str:
    """Format a large numeric count as a compact string."""
    try:
        return f"{int(cast(Any, value)):,}"
    except (TypeError, ValueError):
        return "0"


async def _transfers(
    address: str,
    chain: str | None,
    direction: str,
    limit: int,
) -> list[Transfer]:
    """Wallet transfer history (address-first projection reads)."""
    wallet = address.strip().lower()
    if not wallet:
        return []
    page_limit = clamp_limit(limit)
    chain_sql, chain_params = chain_clause(chain)
    base_params: dict[str, object] = {"address": wallet, **chain_params}

    selects: list[str] = []
    for addr_col, label in (("from_address", "out"), ("to_address", "in")):
        if direction not in ("all", label):
            continue
        selects.append(
            f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
            f"token_address, value, '{label}' AS direction "
            f"FROM wallet_memory.evm_transfers "
            f"WHERE {addr_col} = %(address)s{chain_sql}"
        )
    if not selects:
        return []
    union = " UNION ALL ".join(selects)
    sql = (
        f"SELECT * FROM ("
        f"SELECT chain, block_number, block_timestamp, tx_hash, log_index, "
        f"token_address, value, %(address)s AS address, direction FROM ({union})"
        f") ORDER BY block_timestamp DESC, block_number DESC, log_index DESC "
        f"LIMIT {page_limit}"
    )
    rows = await asyncio.to_thread(query, sql, base_params)
    return [
        Transfer(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]) or "",
            tx_hash=row["tx_hash"],
            log_index=row["log_index"],
            token_address=row["token_address"],
            value=str(row["value"]),
            address=row["address"],
            direction=row["direction"],
        )
        for row in rows
    ]


async def _chain_stats(chain: str | None) -> ChainStats:
    """Chain-wide aggregate counts (mirrors the REST /stats endpoint)."""
    chain_sql, chain_params = chain_clause(chain)
    params: dict[str, Any] = chain_params

    main = (
        await asyncio.to_thread(
            query,
            "SELECT count() AS total_transfers, "
            "uniqCombined(from_address) AS unique_senders, "
            "uniqCombined(to_address) AS unique_receivers, "
            "uniqCombined(token_address) AS unique_tokens, "
            "sum(toFloat64OrZero(value)) AS total_raw_value, "
            "min(block_timestamp) AS min_ts, "
            "max(block_timestamp) AS max_ts "
            "FROM wallet_memory.evm_transfers "
            f"WHERE 1=1{chain_sql}",
            params,
        )
    )[0]

    dex = (
        await asyncio.to_thread(
            query,
            f"SELECT count() FROM wallet_memory.dex_events WHERE 1=1{chain_sql}",
            params,
        )
    )[0]["count()"]
    deployers = (
        await asyncio.to_thread(
            query,
            f"SELECT count() FROM wallet_memory.deployer_events WHERE 1=1{chain_sql}",
            params,
        )
    )[0]["count()"]
    solana = (
        await asyncio.to_thread(
            query,
            "SELECT count() FROM wallet_memory.solana_spl_balance_changes WHERE 1=1",
            {},
        )
    )[0]["count()"]
    labels = (
        await asyncio.to_thread(
            query,
            "SELECT count() FROM wallet_memory.wallet_labels WHERE 1=1",
            {},
        )
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
    except ValueError:  # pragma: no cover — defensive
        return 0.0


def _usd_asset(row: dict[str, object]) -> str:
    """Return the stablecoin symbol used for USD valuation."""
    token0 = str(row["token0"] or "")
    token1 = str(row["token1"] or "")
    chain = str(row.get("chain") or "")
    stable: frozenset[str] = STABLECOIN_ADDRESSES.get(chain, frozenset())
    if token0 in stable:
        return stablecoin_symbol(token0) or "USD"
    if token1 in stable:
        return stablecoin_symbol(token1) or "USD"
    return "USD"


async def _whales(chain: str | None, min_usd: float, limit: int) -> list[WhaleEvent]:
    """Large-value DEX swaps, USD-valued via the stablecoin side of the pair."""
    page_limit = clamp_limit(limit)
    stablecoin_terms: list[str] = []
    chains: list[str] = [chain.strip().lower()] if chain else list(STABLECOIN_ADDRESSES)
    for ch in chains:
        for addr in STABLECOIN_ADDRESSES.get(ch, set()):
            stablecoin_terms.append(f"(token0 = '{addr}' OR token1 = '{addr}')")
    if not stablecoin_terms:
        return []

    chain_sql, chain_params = chain_clause(chain)
    params: dict[str, object] = {**chain_params, "min_usd": min_usd}
    stablecoin_filter = "(" + " OR ".join(stablecoin_terms) + ")"
    sql = (
        "SELECT chain, block_number, block_timestamp, tx_hash, dex, version, "
        "contract_address, token0, token1, sender, amount0, amount1 "
        "FROM wallet_memory.dex_events "
        f"WHERE {stablecoin_filter} "
        f"AND (toFloat64OrZero(if(token0 IN {_stablecoin_set(chains)}, amount0, '0')) >= %(min_usd)s "
        f"OR toFloat64OrZero(if(token1 IN {_stablecoin_set(chains)}, amount1, '0')) >= %(min_usd)s)"
        f"{chain_sql} "
        "ORDER BY block_timestamp DESC, block_number DESC "
        f"LIMIT {page_limit}"
    )
    rows = await asyncio.to_thread(query, sql, params)
    return [
        WhaleEvent(
            chain=row["chain"],
            block_number=row["block_number"],
            block_timestamp=_fmt_ts(row["block_timestamp"]) or "",
            tx_hash=row["tx_hash"],
            dex=row["dex"],
            version=row["version"],
            pair_address=row["contract_address"],
            token0=row["token0"],
            token1=row["token1"],
            sender=row["sender"],
            usd_value=_usd_value(row),
            usd_asset=_usd_asset(row),
        )
        for row in rows
    ]


# ── Schema ────────────────────────────────────────────────────────────


@strawberry.type
class Query:
    """Read-only GraphQL surface over the ClickHouse warehouse."""

    @strawberry.field(description="Transfer history for a wallet (address-first)")  # type: ignore[untyped-decorator]  # strawberry stubs untyped
    async def transfers(
        self,
        address: str,
        chain: str | None = None,
        direction: str = "all",
        limit: int | None = 100,
    ) -> list[Transfer]:
        return await _transfers(address, chain, direction, limit or 100)

    @strawberry.field(description="Chain-wide aggregate statistics")  # type: ignore[untyped-decorator]  # strawberry stubs untyped
    async def chain_stats(self, chain: str | None = None) -> ChainStats:
        return await _chain_stats(chain)

    @strawberry.field(description="Large-value DEX swaps (USD via stablecoin side)")  # type: ignore[untyped-decorator]  # strawberry stubs untyped
    async def whales(
        self,
        chain: str | None = None,
        min_usd: float = 100_000.0,
        limit: int | None = 20,
    ) -> list[WhaleEvent]:
        return await _whales(chain, min_usd, limit or 20)


schema = strawberry.Schema(query=Query)

graphql_router = GraphQLRouter(schema, path="/graphql")
