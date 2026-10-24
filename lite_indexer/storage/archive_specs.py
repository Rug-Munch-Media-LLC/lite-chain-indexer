"""Declarative archive contract: which tables ship, and how their cursor works.

The B2 parquet archive is permanent; ClickHouse is a rolling window. A table
that is not in :data:`ARCHIVE_TABLES` has **no export path at all** and will be
eaten by its own TTL — exactly how ``evm_transfers`` lost its history. This
module is the single source of truth for that decision, so the omission is a
data error rather than a silent gap.

Two modes:

``cursor``
    The table has a monotonic column (``ingested_at`` or a chain timestamp).
    One unit = (chain, table, UTC date of the cursor). Rows are append-only,
    so a unit is final once the date is closed and re-exports are idempotent.

``snapshot``
    No monotonic column exists. There is no correct "date" to cut on, so the
    unit is a full-table image per run, dated by the export day. Snapshot
    tables are never deleted from and never claimed to be complete for a date.
"""

from __future__ import annotations

from dataclasses import dataclass

CURSOR = "cursor"
SNAPSHOT = "snapshot"


@dataclass(frozen=True)
class TableSpec:
    """One archivable table: cursor semantics + TTL exposure."""

    table: str
    kind: str
    cursor: str
    mode: str
    chain_column: str | None
    ttl_column: str
    ttl_days: int
    columns: tuple[str, ...]
    dataset: str = ""

    @property
    def date_column(self) -> str:
        """Column the unit date is derived from."""
        return self.cursor


ARCHIVE_TABLES: dict[str, TableSpec] = {
    "evm_transfers": TableSpec(
        "evm_transfers", "transfers", "block_timestamp", CURSOR, "chain",
        "block_timestamp", 30,
        ("chain", "block_number", "tx_hash", "from_address", "to_address", "token_address",
         "value", "block_timestamp", "log_index", "ingested_at"),
    ),
    "dex_events": TableSpec(
        "dex_events", "dex", "block_timestamp", CURSOR, "chain",
        "block_timestamp", 90,
        ("type", "chain", "block_number", "block_timestamp", "tx_hash", "log_index",
         "contract_address", "dex", "version", "token0", "token1", "sender", "recipient",
         "amount0_in", "amount1_in", "amount0_out", "amount1_out", "amount0", "amount1",
         "whale", "price_usd", "volume_usd", "ingested_at", "source"),
    ),
    "internal_transfers": TableSpec(
        "internal_transfers", "internal", "ingested_at", CURSOR, "chain",
        "block_timestamp", 90,
        ("chain", "block_number", "block_timestamp", "tx_hash", "trace_address",
         "from_address", "to_address", "value_wei", "call_type", "gas_used", "error",
         "input_method_id", "ingested_at"),
    ),
    "solana_spl_balance_changes": TableSpec(
        "solana_spl_balance_changes", "solana", "ingested_at", CURSOR, None,
        "block_timestamp", 60,
        ("slot", "block_timestamp", "tx_hash", "token_account", "mint", "owner",
         "pre_amount", "post_amount", "delta", "decimals", "token_symbol", "token_name",
         "ingested_at"),
    ),
    "address_activity": TableSpec(
        "address_activity", "activity", "ingested_at", CURSOR, "chain",
        "block_time", 365,
        ("address", "tx_signature", "slot", "block_time", "involved_accounts", "program_ids",
         "is_signer", "ingested_at", "chain"),
    ),
    "dex_ohlcv_hourly": TableSpec(
        "dex_ohlcv_hourly", "ohlcv", "timestamp", CURSOR, "chain",
        "timestamp", 90,
        ("chain", "pair_address", "timestamp", "open", "high", "low", "close", "volume_usd"),
    ),
    "holder_balances": TableSpec(
        "holder_balances", "holders", "", SNAPSHOT, "chain", "", 0,
        ("chain", "token_address", "holder", "balance", "updated_at"),
    ),
    "wallet_labels": TableSpec(
        "wallet_labels", "wallet_labels", "", SNAPSHOT, None, "", 0,
        ("address", "label", "category", "source", "confidence", "updated_at"),
    ),
    # Polymarket: 1,221 rows crawled once on 2026-10-23, no TTL, no archive.
    # No producer exists (one part per table, one insert) — see REPORT.
    "pm_trades": TableSpec(
        "pm_trades", "pm_trades", "ingested_at", CURSOR, None, "ts", 0,
        ("condition_id", "ts", "proxy_wallet", "asset", "side", "outcome",
         "outcome_index", "size", "price", "notional", "title", "slug",
         "tx_hash", "ingested_at"),
        dataset="polymarket",
    ),
    "pm_markets": TableSpec(
        "pm_markets", "pm_markets", "", SNAPSHOT, None, "", 0,
        ("market_id", "condition_id", "question", "slug", "end_date", "liquidity",
         "volume", "closed", "outcomes", "outcome_prices", "asof"),
        dataset="polymarket",
    ),
    "pm_holders": TableSpec(
        "pm_holders", "pm_holders", "", SNAPSHOT, None, "", 0,
        ("asof", "condition_id", "token", "proxy_wallet", "amount",
         "outcome_index", "name", "pseudonym"),
        dataset="polymarket",
    ),
}

# The tables this task proved were unexported (325 GiB, no path at all).
GAP_TABLES: tuple[str, ...] = (
    "address_activity",
    "solana_spl_balance_changes",
    "internal_transfers",
)

TABLE_KINDS: dict[str, str] = {name: spec.kind for name, spec in ARCHIVE_TABLES.items()}


def gaps_outside_registry(clickhouse_tables: set[str]) -> list[str]:
    """Return data tables with no archive spec — the silent-loss set."""
    return sorted(t for t in clickhouse_tables if t not in ARCHIVE_TABLES)


def validate() -> list[str]:
    """Static self-check: every spec must declare a coherent cursor/mode."""
    problems: list[str] = []
    for name, spec in ARCHIVE_TABLES.items():
        if spec.mode not in (CURSOR, SNAPSHOT):
            problems.append(f"{name}: unknown mode {spec.mode!r}")
        if spec.mode == CURSOR and not spec.cursor:
            problems.append(f"{name}: cursor mode without a cursor column")
        if spec.mode == SNAPSHOT and spec.cursor:
            problems.append(f"{name}: snapshot mode must not declare a cursor")
        if not spec.columns:
            problems.append(f"{name}: no exported columns")
        if spec.cursor and spec.cursor not in spec.columns:
            problems.append(f"{name}: cursor {spec.cursor!r} missing from columns")
    return problems


__all__ = [
    "ARCHIVE_TABLES",
    "CURSOR",
    "GAP_TABLES",
    "SNAPSHOT",
    "TableSpec",
    "gaps_outside_registry",
    "validate",
]
