"""Producer↔consumer parity *contract* for the B2 parquet cold tier.

Producer (canonical): ``scripts/lite-indexer-parquet-daily.sh`` runs
``clickhouse-client ... FORMAT Parquet`` hourly and uploads::

    {remote}/{chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_{YYYY-MM-DD}.parquet
    {remote}/{chain}/{YYYY}/{MM}/{DD}/{HH}/{chain}_dex_{YYYY-MM-DD}.parquet

The date lives in the *filename*, the hour + chain in *directory* components;
no Hive ``chain=``/``date=`` keys are emitted. ClickHouse writes table columns
verbatim: ``String``/``LowCardinality(String)`` -> UTF-8 VARCHAR (addresses are
hex text, verified against archived objects — not binary), ``DateTime`` ->
TIMESTAMP[ms, tz=UTC], ``Nullable(String)`` -> VARCHAR NULL. This module owns
the layout/kind/column contract; ``cold_rows`` applies it to a read.
"""

from __future__ import annotations

from typing import Any

# The hourly producer writes the v2 bucket; v1 (legacy) stopped 2026-08-31.
PRODUCER_REMOTE = "b2-crypt-v2:chain-data/parquet"
LEGACY_REMOTE = "b2-crypt:chain-data/parquet"

# Table → parquet file-kind. The export writes transfers + dex today;
# deployer/internal/solana mappings are reserved so the engine is ready
# when the export pipeline grows.
TABLE_KINDS: dict[str, str] = {
    "evm_transfers": "transfers",
    "dex_events": "dex",
    "deployer_events": "deployer",
    "internal_transfers": "internal",
    "solana_transfers": "solana",
}

# Canonical column list per table — the shared hot/cold result shape.
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "evm_transfers": (
        "chain",
        "block_number",
        "tx_hash",
        "from_address",
        "to_address",
        "token_address",
        "value",
        "block_timestamp",
        "log_index",
        "ingested_at",
    ),
    "dex_events": (
        "type",
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "contract_address",
        "dex",
        "version",
        "token0",
        "token1",
        "sender",
        "recipient",
        "amount0_in",
        "amount1_in",
        "amount0_out",
        "amount1_out",
        "amount0",
        "amount1",
        "whale",
        "price_usd",
        "volume_usd",
        "ingested_at",
        "source",
    ),
    "deployer_events": (
        "deployer",
        "deployer_eoa",
        "token_address",
        "token0",
        "token1",
        "pair_address",
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "type",
        "factory",
        "ingested_at",
        "source",
    ),
    "internal_transfers": (
        "chain",
        "block_number",
        "block_timestamp",
        "tx_hash",
        "trace_address",
        "from_address",
        "to_address",
        "value_wei",
        "call_type",
        "gas_used",
        "error",
        "input_method_id",
        "ingested_at",
    ),
    "solana_transfers": (
        "chain",
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
}

# Canonical ORDER BY per table (newest first), matching real columns.
TABLE_ORDER: dict[str, str] = {
    "evm_transfers": "block_timestamp DESC, block_number DESC, log_index DESC",
    "dex_events": "block_timestamp DESC, block_number DESC, log_index DESC",
    "deployer_events": "block_timestamp DESC, block_number DESC",
    "internal_transfers": "block_timestamp DESC, block_number DESC, tx_hash DESC",
    "solana_transfers": "block_timestamp DESC, slot DESC",
}

# Tables whose producer file carries no ``chain`` column (the chain is only
# in the directory): the read must not filter on it — it is injected instead.
CHAINLESS_TABLES: frozenset[str] = frozenset({"solana_transfers"})

# Type defaults for canonical columns absent from an older export.
COLUMN_DEFAULTS: dict[str, Any] = {
    "chain": "",
    "block_number": 0,
    "slot": 0,
    "log_index": 0,
    "tx_hash": "",
    "token_address": "",
    "from_address": "",
    "to_address": "",
    "owner": "",
    "mint": "",
    "token_account": "",
    "value": "",
    "value_wei": "",
    "amount": "",
    "pre_amount": "",
    "post_amount": "",
    "delta": "",
    "source": "",
    "error": "",
    "input_method_id": "",
}


def align_remote(remote: str) -> str:
    """Point the legacy v1 cold remote at the live v2 producer bucket.

    ``PARQUET_REMOTE``/``HISTORICAL_PARQUET_REMOTE`` default to the retired
    ``b2-crypt`` bucket (last write 2026-08-31); the hourly producer has
    written ``b2-crypt-v2`` since. Without this the cold leg silently reads
    a three-week-stale tree and never sees a freshly archived date.
    """
    return PRODUCER_REMOTE if remote.rstrip("/") == LEGACY_REMOTE else remote


__all__ = [
    "CHAINLESS_TABLES",
    "COLUMN_DEFAULTS",
    "LEGACY_REMOTE",
    "PRODUCER_REMOTE",
    "TABLE_COLUMNS",
    "TABLE_KINDS",
    "TABLE_ORDER",
    "align_remote",
]
