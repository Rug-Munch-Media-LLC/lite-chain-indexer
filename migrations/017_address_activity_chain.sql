-- Migration 017: add a `chain` discriminator to address_activity.
--
-- WHY: address_activity (migration 014) was Solana-only. Extending the
-- wallet-tracing keystone to EVM + TRON reuses the SAME table with a
-- `chain` column rather than a sibling table, so an address-first query
-- (WHERE address = ...) stays a single scan across every chain. The
-- column is appended to the ReplacingMergeTree sort key so the dedup key
-- is explicit per chain:
--
--     ORDER BY (address, tx_signature)  ->  (address, tx_signature, chain)
--
-- `MODIFY ORDER BY` appends to the legacy key; the old key remains a
-- prefix of the new one, so ClickHouse performs a metadata-only change
-- (no data rewrite, no DROP, no DELETE). Existing rows backfill with the
-- DEFAULT 'solana' — correct, since all pre-015 rows are Solana.
--
-- IDEMPOTENCY: ADD COLUMN IF NOT EXISTS is re-runnable; re-running
-- MODIFY ORDER BY with the SAME expression is a harmless metadata no-op.
-- The runtime ensure_address_activity_table() also applies both ALTERs
-- (best-effort) before its CREATE, so this file is for operators
-- provisioning the schema ahead of a deploy (same convention as 014).
--
-- SAFETY: no DROP, no CASCADE, no DELETE, no ALTER ... DELETE.
--
-- Apply once:  clickhouse-client < migrations/018_address_activity_chain.sql
-- Pre-flight (read-only):
--     SELECT name, engine FROM system.tables
--     WHERE database='wallet_memory' AND name='address_activity';
-- Post-apply verify:
--     SELECT name, type, default_expression FROM system.columns
--     WHERE database='wallet_memory' AND table='address_activity' AND name='chain';
--     SELECT sorting_key FROM system.tables
--     WHERE database='wallet_memory' AND name='address_activity';

ALTER TABLE wallet_memory.address_activity
    ADD COLUMN IF NOT EXISTS chain String DEFAULT 'solana' CODEC(ZSTD(3));

ALTER TABLE wallet_memory.address_activity
    MODIFY ORDER BY (address, tx_signature, chain);
