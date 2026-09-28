-- Migration 011: Jito bundled-launch detection table.
--
-- Detects bundled token launches on Solana: a token-create transaction
-- immediately followed (same slot) by buys from distinct fresh wallets,
-- with a Jito tip transfer (>= 1000 lamports to one of the 8 canonical
-- tip accounts) riding in the LAST transaction of the run. The tip
-- transfer + same-slot signals are what remain hard for bundler tools
-- to evade (they only spread the buyer wallet count, which is stored as
-- a LOWER bound in buyer_count).
--
-- Detector: lite_indexer.strategies.solana.bundles.
--
-- One row per detected bundled launch (slot, create tx, tip tx).
--
-- Also ensured idempotently at runtime by
-- lite_indexer.strategies.solana.bundles.ensure_bundles_table, so this
-- file is for operators provisioning the schema ahead of a deploy.
--
-- Buyer identity is kept as an Array(String) so downstream scanners can
-- join wallet risk/intelligence without a separate table hop.

CREATE TABLE IF NOT EXISTS wallet_memory.solana_bundles (
    chain String DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    create_tx_signature String,  -- token-create tx signature
    tip_tx_signature String,     -- LAST tx of the run (carries the tip)
    tip_account String,          -- which of the 8 Jito tip accounts received the tip
    mint String DEFAULT '',      -- created token mint ('' when not extractable)
    buyer_count UInt16,          -- distinct fresh-wallet buyers (LOWER bound)
    buyer_wallets Array(String) DEFAULT [],  -- fresh buyer wallet addresses
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (slot, create_tx_signature)
TTL block_timestamp + INTERVAL 90 DAY;
