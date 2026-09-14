-- Migration 013: hot/cold retention classes (lean hot window, B2 permanence).
--
-- WHY: the transfer/dex tables are now ReplacingMergeTree + PARTITION BY
-- toYYYYMM(<event time>), so TTL drops whole partitions cheaply. The hot
-- window is being tightened to a lean set of per-class TTLs; everything older
-- is archived to B2 Parquet (queried in place via s3()) and the hot copy is
-- dropped. This also fixes the current drift where live evm_transfers uses
-- `ingested_at + 90d` instead of the intended 30-day event-time window — a
-- backfill of old blocks must not extend retention.
--
-- POLICY (single source of truth: lite_indexer/config/storage.py
-- RETENTION_POLICY, mirrored in docs/CLICKHOUSE_RETENTION.md):
--   30d   evm_transfers            on block_timestamp  (volume hog; event time)
--   90d   dex_events               on block_timestamp
--   90d   internal_transfers       on block_timestamp
--   90d   solana_transfers         on block_timestamp
--  180d   scam_addresses           on first_seen_at
--  180d   wallet_labels            on loaded_at
--  180d   deployer_history         on deployed_at
--  365d   evm_launches             on block_timestamp
--  365d   solana_launches          on block_timestamp
--  365d   tron_launches            on block_timestamp
--
-- ARCHIVE-BEFORE-DROP: TTL deletion runs lazily during part merges. The
-- archive pipeline (parquet export → B2) is mandatory and must stay ahead of
-- the TTL; a hot-window drop is only safe once the rows exist in the cold
-- tier. B2 Parquet is the permanence layer — never the ClickHouse hot copy.
--
-- IDEMPOTENCY / APPLY-ONCE:
--   MODIFY TTL is a metadata rewrite, NOT an idempotent CREATE IF NOT EXISTS.
--   Re-running this file with the SAME expressions is a harmless no-op;
--   re-running with DIFFERENT intervals silently overwrites.
--   → Apply once:  clickhouse-client --multiquery < migrations/013_retention_classes.sql
--   → Provisioned deployment only (runtime ensure_* DDL must have created the
--     tables; some are not covered by migrations 001–012).
--   → Do NOT hand-edit intervals in place; ship a new migration.
--   → Post-apply verify: python3 -m lite_indexer.retention
--
-- SAFETY: no DROP, no CASCADE, no DELETE, no ALTER ... DELETE. Only
-- table-level TTL metadata. TTL deletion is lazy (part merges), never
-- synchronous.

-- ── 30-day class: the volume hog. Event-time TTL so backfills do not extend
--    retention. ReplacingMergeTree + partitioned ⇒ cheap partition drops.

ALTER TABLE wallet_memory.evm_transfers
    MODIFY TTL block_timestamp + INTERVAL 30 DAY;

-- ── 90-day event tables.

ALTER TABLE wallet_memory.dex_events
    MODIFY TTL block_timestamp + INTERVAL 90 DAY;

ALTER TABLE wallet_memory.internal_transfers
    MODIFY TTL block_timestamp + INTERVAL 90 DAY;

ALTER TABLE wallet_memory.solana_transfers
    MODIFY TTL block_timestamp + INTERVAL 90 DAY;

-- ── 180-day state/label tables (anchored on their natural time column).

ALTER TABLE wallet_memory.scam_addresses
    MODIFY TTL first_seen_at + INTERVAL 180 DAY;

ALTER TABLE wallet_memory.wallet_labels
    MODIFY TTL loaded_at + INTERVAL 180 DAY;

ALTER TABLE wallet_memory.deployer_history
    MODIFY TTL deployed_at + INTERVAL 180 DAY;

-- ── 365-day launch tables (rare, small, high-value).

ALTER TABLE wallet_memory.evm_launches
    MODIFY TTL block_timestamp + INTERVAL 365 DAY;

ALTER TABLE wallet_memory.solana_launches
    MODIFY TTL block_timestamp + INTERVAL 365 DAY;

ALTER TABLE wallet_memory.tron_launches
    MODIFY TTL block_timestamp + INTERVAL 365 DAY;

-- ── NEVER TTL:
--    parquet_export_state — the (chain, date) archive marker. Dropping a row
--    makes the exporter re-export an already-archived day and DELETE rows
--    after the parquet was written → cold-tier data loss. Leave untouched.
