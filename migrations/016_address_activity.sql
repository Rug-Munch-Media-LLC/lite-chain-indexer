-- Migration 016: address_activity — the wallet-tracing keystone table.
--
-- One row per (address, tx_signature): every transaction an address
-- touched, in the spirit of getSignaturesForAddress parity on
-- free public RPCs. Rows are derived from the same getTransaction
-- (jsonParsed) payloads the indexer already fetches for SPL transfers,
-- DEX swaps, launches and traces — zero extra RPC cost.
--
--   address            involved account pubkey
--   tx_signature       transaction signature (one per transaction)
--   slot               slot the transaction landed in
--   block_time         transaction blockTime (UTC)
--   involved_accounts  every account key loaded by the transaction
--   program_ids        distinct program ids invoked (incl. CPI inners)
--   is_signer          whether `address` signed the transaction
--
-- ReplacingMergeTree dedups re-fetched / re-indexed transactions on
-- (address, tx_signature); ingested_at makes the newest insert win.
-- Retention mirrors RETENTION_DAYS["address_activity"] in
-- lite_indexer/config/storage.py (365 days): a wallet-tracing history
-- table is only valuable while its history survives.
--
-- Also ensured idempotently at runtime by
-- lite_indexer.storage.address_activity.ensure_address_activity_table,
-- so this file is for operators provisioning the schema ahead of a
-- deploy (same convention as migrations/003_*.sql and 004_*.sql).

CREATE TABLE IF NOT EXISTS wallet_memory.address_activity (
    address String CODEC(ZSTD(3)),
    tx_signature String CODEC(ZSTD(3)),
    slot UInt64,
    block_time DateTime,
    involved_accounts Array(String),
    program_ids Array(String),
    is_signer Bool,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (address, tx_signature)
TTL block_time + INTERVAL 365 DAY;
