"""Archive specs for the cross-chain and derived tables."""

from __future__ import annotations

from lite_indexer.storage.archive_specs._spec import CURSOR, SNAPSHOT, TableSpec

MULTICHAIN_SPECS: dict[str, TableSpec] = {
    "solana_spl_balance_changes": TableSpec(
        "solana_spl_balance_changes",
        "solana",
        "block_timestamp",
        CURSOR,
        None,
        "block_timestamp",
        60,
        (
            "slot",
            "block_timestamp",
            "tx_hash",
            "token_account",
            "mint",
            "owner",
            "pre_amount",
            "post_amount",
            "delta",
            "decimals",
            "token_symbol",
            "token_name",
            "ingested_at",
        ),
    ),
    "address_activity": TableSpec(
        "address_activity",
        "activity",
        "ingested_at",
        CURSOR,
        "chain",
        "block_time",
        365,
        (
            "address",
            "tx_signature",
            "slot",
            "block_time",
            "involved_accounts",
            "program_ids",
            "is_signer",
            "ingested_at",
            "chain",
        ),
    ),
    "dex_ohlcv_hourly": TableSpec(
        "dex_ohlcv_hourly",
        "ohlcv",
        "timestamp",
        CURSOR,
        "chain",
        "timestamp",
        90,
        ("chain", "pair_address", "timestamp", "open", "high", "low", "close", "volume_usd"),
    ),
    "holder_balances": TableSpec(
        "holder_balances",
        "holders",
        "",
        SNAPSHOT,
        "chain",
        "",
        0,
        ("chain", "token_address", "holder", "balance", "updated_at"),
    ),
    "wallet_labels": TableSpec(
        "wallet_labels",
        "wallet_labels",
        "",
        SNAPSHOT,
        None,
        "",
        0,
        ("address", "label", "category", "source", "confidence", "updated_at"),
    ),
    # Polymarket: 1,221 rows crawled once on 2026-10-23, no TTL, no archive.
    # No producer exists (one part per table, one insert) — see REPORT.
}
