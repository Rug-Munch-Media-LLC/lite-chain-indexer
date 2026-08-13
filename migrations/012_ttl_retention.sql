-- Migration 012: ClickHouse hot-store retention bounding (2026-08-13 disk-full follow-up).
--
-- WHY: the 2026-08-13 disk-full outage hit /data/clickhouse at 607GB. The
-- parquet export (scripts/lite-indexer-parquet-daily.sh → export-parquet)
-- only exports evm_transfers + dex_events for the 8 EVM chains; Solana,
-- Tron, launches, bundles, holder_balances, wallet_labels and the other
-- state/cache tables are NEVER exported, so they rely on TTL to stay
-- bounded. This migration makes the retention policy explicit and adds the
-- missing table-level TTLs.
--
-- POLICY (see docs/CLICKHOUSE_RETENTION.md for the full matrix):
--   * Exported tables (evm_transfers, dex_events) → 180-day safety net on
--     block_timestamp, in case the daily parquet export falls behind
--     (export normally deletes after upload at PARQUET_COLD_DAYS=7).
--   * Un-exported event tables → 90-day TTL on block_timestamp.
--   * Launch tables → 365 days (rare, small, high-value).
--   * State/cache tables → TTL on their *_at / last_updated column.
--
-- IDEMPOTENCY / APPLY-ONCE:
--   MODIFY TTL is a metadata rewrite, NOT an idempotent CREATE IF NOT
--   EXISTS. Re-running this file with the SAME expressions is a harmless
--   no-op, but re-running with DIFFERENT intervals silently overwrites.
--   → Apply once:  clickhouse-client < migrations/012_ttl_retention.sql
--   → Do NOT hand-edit the intervals in place; ship a new migration.
--   → Pre-flight check (read-only) before applying:
--       SELECT name, engine FROM system.tables
--       WHERE database='wallet_memory' AND name IN (...);
--   → Post-apply verify (see docs/CLICKHOUSE_RETENTION.md §Verification).
--
-- SAFETY: no DROP, no CASCADE, no DELETE, no ALTER ... DELETE. Only
-- table-level TTL metadata. TTL deletion happens lazily during part
-- merges — parts are never dropped synchronously.

-- ── Exported tables: 180-day safety net (export normally prunes at ~7d).
--    NOTE: time column switched from ingested_at to block_timestamp so
--    retention tracks EVENT time, not ingestion time (backfills of old
--    blocks no longer extend their retention).

ALTER TABLE wallet_memory.evm_transfers
    MODIFY TTL block_timestamp + INTERVAL 180 DAY;

ALTER TABLE wallet_memory.dex_events
    MODIFY TTL block_timestamp + INTERVAL 180 DAY;

-- ── Un-exported event tables: 90-day event-time TTL.
--    (deployer_events is NOT in the parquet export loop — it previously
--    relied on an ingested_at TTL; align it to event time for consistency.)

ALTER TABLE wallet_memory.deployer_events
    MODIFY TTL block_timestamp + INTERVAL 90 DAY;

-- ── State / cache tables with NO TTL today (added):
--    holder_balances: 36.9M rows / 1.7GB and growing — the largest
--      un-exported table. ReplacingMergeTree(version); TTL on
--      last_updated drops stale cached balances for inactive holders.
ALTER TABLE wallet_memory.holder_balances
    MODIFY TTL last_updated + INTERVAL 90 DAY;

--    token_meta_ref: one row per token; re-fetchable metadata (symbol/
--      name), so a 365-day last-updated TTL is a safe cleanup net.
ALTER TABLE wallet_memory.token_meta_ref
    MODIFY TTL last_updated + INTERVAL 365 DAY;

--    wallet_profiles: profile aggregates; TTL on last_seen_at (last
--      observed activity) rather than the merge version column.
ALTER TABLE wallet_memory.wallet_profiles
    MODIFY TTL last_seen_at + INTERVAL 180 DAY;

--    block_checkpoints: reorg checkpoints; only recent blocks matter.
ALTER TABLE wallet_memory.block_checkpoints
    MODIFY TTL checked_at + INTERVAL 180 DAY;

--    token_prices: one row per (chain, token); drop stale quotes.
ALTER TABLE wallet_memory.token_prices
    MODIFY TTL updated_at + INTERVAL 90 DAY;

-- ── Already bounded — intentionally NOT modified (documented only):
--    solana_transfers          TTL block_timestamp + 90d
--    solana_spl_balance_changes TTL block_timestamp + 60d
--    internal_transfers        TTL block_timestamp + 90d
--    evm_launches              TTL block_timestamp + 365d
--    solana_launches           TTL block_timestamp + 365d (migration 007)
--    tron_launches             TTL block_timestamp + 365d (migration 008)
--    solana_bundles            TTL block_timestamp + 90d  (migration 009)
--    bridge_mappings           TTL bridged_at + 180d
--    wallet_entities           TTL last_updated_at + 180d
--    wallet_labels             TTL loaded_at + 6mo
--    dex_ohlcv_hourly          TTL timestamp + 90d
--    scam_addresses            TTL first_seen_at + 6mo
--    deployer_history          TTL deployed_at + 90d
--    chain_indexer_progress    TTL last_updated (state)
--    token_transfers           TTL block_timestamp (empty)

-- ── NEVER TTL:
--    parquet_export_state — the (chain, date) export marker table.
--    Dropping a row would make the exporter re-export an already-archived
--    day and then re-DELETE those rows from ClickHouse AFTER the parquet
--    was already archived → cold-tier data loss. Leave it untouched.
