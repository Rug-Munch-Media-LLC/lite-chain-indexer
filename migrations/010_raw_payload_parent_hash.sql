-- Migration 010: unified schema (additive) — raw payload + parent hash.
--
-- Two additive columns so downstream consumers can re-decode without a
-- re-fetch and walk reorgs without extra RPC calls:
--
--   wallet_memory.transactions.raw_payload — serialized raw RPC dict
--     (the eth_getLogs / eth_getTransactionByHash result) retained for
--     re-decode without re-fetch. The ClickHouseWriter writers populate
--     it at ingestion when the raw payload is in hand (see
--     lite_indexer.storage.clickhouse._serialize_raw_payload); rows
--     without a captured payload keep '' (column default).
--
--   wallet_memory.blocks.parent_hash        — parent block hash for reorg
--     walks. Populated from the fetched block's `parentHash` field where
--     the fetch layer provides it.
--
-- NOTE on table ownership: `wallet_memory.transactions` and
-- `wallet_memory.blocks` are provisioned by the upstream indexer (they
-- are NOT created by migrations 001-009 — those create dex_events,
-- token_prices, internal_transfers, solana_transfers, wallet_entities,
-- solana_launches, tron_launches, solana_bundles, plus the historical
-- views). This migration is idempotent and safe to run against any
-- deployment that has those tables; ADD COLUMN IF NOT EXISTS is a no-op
-- where they already exist or were provisioned without them.
--
-- Run idempotently anytime:
--   clickhouse-client < migrations/010_raw_payload_parent_hash.sql

ALTER TABLE wallet_memory.transactions
    ADD COLUMN IF NOT EXISTS raw_payload String DEFAULT '';

ALTER TABLE wallet_memory.blocks
    ADD COLUMN IF NOT EXISTS parent_hash String DEFAULT '';
