"""Pydantic response models and shared helpers for the Chain Query API."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageMeta(BaseModel):
    """Pagination metadata included in every list response."""

    limit: int = Field(description="Page size used for this response")
    offset: int = Field(description="Offset used for this response")
    has_more: bool = Field(description="True when more rows exist past this page")
    total: int | None = Field(
        default=None,
        description="Total matching rows (present when the query can count them cheaply)",
    )


class Page(BaseModel, Generic[T]):
    """Generic paginated response envelope."""

    data: list[T]
    pagination: PageMeta


class TransferEvent(BaseModel):
    """A single ERC-20/TRC-20 transfer event."""

    chain: str
    block_number: int
    block_timestamp: str
    tx_hash: str
    log_index: int
    from_address: str
    to_address: str
    token_address: str
    value: str = Field(description="Raw token amount as decimal string")
    direction: str = Field(description="'in' when the wallet received, 'out' when it sent")


class DeployerEvent(BaseModel):
    """A contract deployment (pair/pool creation) by an address."""

    chain: str
    block_number: int
    block_timestamp: str
    tx_hash: str
    deployer: str
    deployer_eoa: str = Field(description="Resolved deployer EOA (tx sender)")
    token_address: str = Field(description="First token of the pair (token0)")
    token0: str = ""
    token1: str = ""
    pair_address: str = ""
    type: str = ""
    factory: str | None = None


class WhaleEvent(BaseModel):
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
    sender: str = ""
    usd_value: float = Field(description="USD value of the swap (stablecoin side amount)")
    usd_asset: str = Field(description="Stablecoin symbol used for valuation")
    amount0: str | None = None
    amount1: str | None = None


class ChainStats(BaseModel):
    """Chain-wide aggregate statistics."""

    chain: str | None = Field(default=None, description="Chain filtered (null = all)")
    total_transfers: int
    unique_senders: int
    unique_receivers: int
    unique_tokens: int
    total_raw_value: str = Field(description="Sum of raw transfer values (not USD — values span all tokens)")
    dex_swaps: int
    deployer_events: int
    solana_balance_changes: int
    wallet_labels: int
    min_block_timestamp: str | None = None
    max_block_timestamp: str | None = None


class QueryResult(BaseModel):
    """Raw SQL query result (columns + rows)."""

    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool = Field(description="True when the result was capped by the API limit")


class StatusInfo(BaseModel):
    """Warehouse status: per-table row counts and freshness."""

    tables: dict[str, dict[str, Any]]
    source: str = Field(description="ClickHouse version string")


# ── Cross-chain forensics (api.routers.trace) ──────────────────────────


class TraceNodeOut(BaseModel):
    """One address on one chain in a fund-trace graph."""

    address: str
    chain: str
    label: str = ""
    entity_type: str = Field(default="", description="bridge | cex_hot_wallet | defi | mixer | scam | eoa | ...")
    risk_score: float = Field(default=0.0, ge=0, le=1, description="0 (clean) .. 1 (high risk)")
    is_scam: bool = False
    first_seen: str = ""


class TraceEdgeOut(BaseModel):
    """One money movement between two trace nodes."""

    from_address: str
    to_address: str
    tx_hash: str
    chain: str
    amount: str
    token: str = ""
    edge_type: str = Field(
        default="transfer",
        description="transfer | internal | swap | bridge_deposit | bridge_withdraw | cex_deposit",
    )
    timestamp: str = ""
    protocol: str = ""
    confidence: float = Field(default=1.0, ge=0, le=1)


class TraceGraphOut(BaseModel):
    """Directed graph of fund movement."""

    nodes: list[TraceNodeOut]
    edges: list[TraceEdgeOut]
    stats: dict[str, Any]


class BridgeInteractionOut(BaseModel):
    """A wallet's interaction with a bridge contract."""

    address: str
    bridge_name: str
    direction: str = Field(description="deposit | withdraw")
    chain: str
    tx_hash: str
    token: str
    amount: str
    timestamp: str
    counterpart_chain: str = ""
    counterpart_tx: str = ""
    matched: bool = False
    confidence: float = Field(default=0.0, ge=0, le=1)


class CexDepositOut(BaseModel):
    """A deposit to a known CEX hot wallet."""

    address: str
    cex_name: str
    chain: str
    to_address: str
    tx_hash: str
    token: str
    amount: str
    timestamp: str
    confidence: float = Field(default=1.0, ge=0, le=1)
    pattern: str = Field(default="direct", description="direct | sweep")
