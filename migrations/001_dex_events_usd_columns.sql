-- Migration 001: add USD enrichment columns to wallet_memory.dex_events.
--
-- Whale detection + USD price normalization (lite_indexer/pricing.py) write
-- the USD price and notional onto every DEX row at ingestion time. These
-- columns are also ensured idempotently at runtime by
-- ClickHouseWriter._ensure_dex_usd_columns, so this file is for operators
-- provisioning the schema ahead of a deploy.
--
-- price_usd  : current USD price of token0 at ingestion (0 when unpriced).
-- volume_usd : estimated USD notional of the swap, max leg (0 when unpriced).

ALTER TABLE wallet_memory.dex_events
    ADD COLUMN IF NOT EXISTS price_usd Float64 DEFAULT 0 AFTER whale;

ALTER TABLE wallet_memory.dex_events
    ADD COLUMN IF NOT EXISTS volume_usd Float64 DEFAULT 0 AFTER price_usd;
