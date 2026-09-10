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


class LaunchEvent(BaseModel):
    """A token-launch event (Solana launchpad create, Tron SunPump/SunSwap, EVM pair/pool creation).

    One row per launch. ``address`` is the token (mint for Solana, token
    contract for EVM/Tron); ``dex`` is the venue the launch was detected
    on (Solana ``venue``, Tron ``factory``/``event_type``, EVM factory
    address). ``liquidity``/``mcap`` are reserved — the launch tables do
    not track them yet, so they are always null.
    """

    chain: str
    address: str = Field(description="Token address (mint for Solana, token contract for EVM/Tron)")
    symbol: str = ""
    creator: str = ""
    dex: str = ""
    block_timestamp: str
    age_seconds: int = Field(description="Seconds between the launch and the query time")
    tx_hash: str = ""
    event_type: str = ""
    launch_signal: int = 0
    graduation_signal: int = 0
    confidence: str = ""
    liquidity: float | None = Field(
        default=None,
        description="Reserved — not tracked by the launch tables yet (always null)",
    )
    mcap: float | None = Field(
        default=None,
        description="Reserved — not tracked by the launch tables yet (always null)",
    )


class BundleEvent(BaseModel):
    """A Jito bundled token launch on Solana (same-slot create + fresh-wallet buys + tip)."""

    chain: str
    slot: int
    block_timestamp: str
    create_tx_signature: str = Field(description="Token-create transaction signature")
    tip_tx_signature: str = Field(description="Last tx of the run (carries the Jito tip)")
    tip_account: str = Field(description="Jito tip account that received the tip")
    mint: str = ""
    buyer_count: int = Field(description="Distinct fresh-wallet buyers (lower bound)")
    buyer_wallets: list[str] = Field(default_factory=list, description="Fresh buyer wallet addresses")
    age_seconds: int = Field(description="Seconds between the bundle and the query time")


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


class AddressActivity(BaseModel):
    """One transaction that touched a wallet (``wallet_memory.address_activity``).

    The wallet-tracing keystone row: the queried ``address`` appears in a
    transaction, alongside every account key the transaction loaded and
    the program ids it invoked. History is immutable, so the same row
    shape answers for live and closed accounts alike.
    """

    address: str = Field(description="Involved account pubkey (the queried wallet)")
    tx_signature: str = Field(description="Transaction signature (one per transaction)")
    slot: int = Field(description="Slot the transaction landed in")
    block_time: str = Field(description="Transaction block time (ISO-8601 UTC)")
    involved_accounts: list[str] = Field(
        default_factory=list,
        description="Every account key loaded by the transaction",
    )
    program_ids: list[str] = Field(
        default_factory=list,
        description="Distinct program ids invoked (incl. CPI inner instructions)",
    )
    is_signer: bool = Field(description="Whether the queried address signed the transaction")


class AddressCounterparty(BaseModel):
    """A wallet counterparty in the inbound/outbound value trace.

    ``address`` is the other side of the value movement: the funder for
    ``/funders`` (who sent value in) or the recipient for ``/counters``
    (who received value out). Counts and first/last-seen are aggregated
    across every matching transfer row.
    """

    address: str = Field(description="Counterparty wallet address")
    tx_count: int = Field(description="Distinct transactions involving this counterparty")
    transfer_count: int = Field(description="Value-moving rows involving this counterparty")
    first_seen: str = Field(description="Earliest transfer block time (ISO-8601 UTC)")
    last_seen: str = Field(description="Latest transfer block time (ISO-8601 UTC)")
