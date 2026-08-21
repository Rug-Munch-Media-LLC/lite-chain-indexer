-- Migration 013: restore dex_events amount columns to String (canonical schema).
--
-- INCIDENT (2026-08-19 dex_events stall):
--   The canonical dex_events schema (lite_indexer/bootstrap.py) stores raw
--   uint256 swap amounts as String. On 2026-08-18 ~21:39 an AD-HOC ALTER
--   (no source in any repo; likely a manual "numeric conversion" attempt
--   related to the dex_events_numeric view created 08-05) rewrote the six
--   amount columns to Nullable(Float64). Evidence: every active part of the
--   267M-row table has modification_time between 08-18 21:39:27 and
--   08-19 01:27:01 (the mutation rewrite window), and every INSERT after
--   that failed with:
--     Code: 53. Type mismatch in VALUES section. Column amount0_in:
--     required argument is not a float
--   (journal: lite-indexer dex_clickhouse_insert_failed, 4+ chains).
--   All readers (rmi-backend scanner engines, api/routers/whales.py,
--   api/graphql.py, dex_ohlcv_hourly_mv) use toFloat64OrZero() and were
--   written for String columns — Float64 was never the intended schema.
--
-- DAMAGE ALREADY DONE (unrecoverable in-table): the 08-18 String→Float64
--   mutation converted exact uint256 strings to Float64 — precision loss
--   above 2^53, empty strings became NULL. Reverting to String cannot
--   restore the lost precision for pre-08-19 rows. Exact amounts for the
--   affected range remain recoverable from NDJSON/parquet cold store
--   (/srv/data/chain-indexer) if ever needed. Rows written between the
--   ad-hoc ALTER and migration 013 carry float-approximated amounts.
--
-- WHY NOT KEEP Float64: raw swap amounts are uint256 token units; Float64
--   silently corrupts them (18-decimal tokens exceed 2^53 routinely). The
--   writer (storage/clickhouse.py write_dex_events) is schema-adaptive as
--   of the 08-21 fix and works under either schema, so ingestion revives
--   immediately on restart WITHOUT this migration — 013 restores full
--   precision going forward and is the canonical end state.
--
-- COST / WHEN TO RUN: full-table mutation, 267M rows / ~21 GiB. Observed
--   rewrite rate for the same table (the 08-18 mutation) was ~4h.
--   → Run ONLY when the host is idle (NOT while rag-embed-migration or
--     other CPU-bound jobs run). Apply once:
--       clickhouse-client --multiquery < migrations/013_dex_events_restore_string_amounts.sql
--   → Verify after completion:
--       SELECT name, type FROM system.columns
--       WHERE database='wallet_memory' AND table='dex_events'
--         AND name LIKE 'amount%';   -- expect Nullable(String)
--
-- COMPATIBILITY: dex_ohlcv_hourly_mv and dex_events_numeric both use
-- toFloat64OrZero(amount*) which accepts String — no view changes needed.

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount0_in Nullable(String);

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount1_in Nullable(String);

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount0_out Nullable(String);

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount1_out Nullable(String);

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount0 Nullable(String);

ALTER TABLE wallet_memory.dex_events
    MODIFY COLUMN amount1 Nullable(String);
