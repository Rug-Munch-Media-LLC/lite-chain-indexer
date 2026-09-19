-- 017_evm_transfers_dedup_key.sql
-- ---------------------------------------------------------------------------
-- Idempotent rewind for wallet_memory.evm_transfers (the TRON/EVM transfer
-- store). Reconciliation rewinds re-extract a [from_block, to_block] range;
-- the writer is append-only, so an unchanged range used to be inserted again
-- and the ClickHouse row count compounded (the 9700%→9800% TRON divergence).
--
-- evm_transfers is already ReplacingMergeTree, but replacement is applied at
-- MERGE time, so exact re-inserts are visible to count() until a merge runs.
-- This migration extends the dedup/sorting key from
--   (chain, block_number, log_index)
-- to
--   (chain, block_number, log_index, tx_hash)
-- so the full dedup key is (chain, block_number, tx_hash, log_index).
--
-- APPEND-ONLY KEY CHANGE: ClickHouse permits adding columns to the end of a
-- MergeTree sorting key via MODIFY ORDER BY (existing prefix preserved), so
-- this is a metadata-only change — no table rebuild, no data rewrite.
-- The application layer additionally skips already-present keys before
-- INSERT (ClickHouseWriter._existing_transfer_keys) so rewind is idempotent
-- immediately, without waiting for a background merge.
--
-- Apply while the indexer is stopped:
--   clickhouse-client --multiquery < migrations/017_evm_transfers_dedup_key.sql
-- ---------------------------------------------------------------------------

ALTER TABLE wallet_memory.evm_transfers
    MODIFY ORDER BY (chain, block_number, log_index, tx_hash);
