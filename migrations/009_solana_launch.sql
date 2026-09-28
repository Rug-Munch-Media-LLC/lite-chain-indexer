-- Migration 009: Solana token-launch events.
--
-- Captures meme-coin launch lifecycle events on Solana so scanners can
-- answer "when did this token launch, who created it, and when did it
-- graduate to an AMM?" — detected by
-- lite_indexer.strategies.solana.launch.detect_launch from the same
-- getTransaction (jsonParsed) payloads the Solana strategy already
-- fetches (zero extra RPC cost).
--
-- One row per transaction; a single tx can set both signals (e.g. the
-- pump.fun migration tx runs create + PumpSwap create_pool):
--
--   venue              program / platform
--   ───────────────────────────────────────────────────────────────
--   pumpfun            pump.fun create instruction (exact discriminator
--                      sha256("global:create")[:8])
--   bonkfun            LaunchLab initialize_v2 with the LetsBonk
--                      platform config account present
--   launchlab          LaunchLab initialize_v2 (no Bonk config account;
--                      name-matched only — see launch.py REVIEW)
--   pumpswap           PumpSwap program presence — create_pool is the
--                      bonding-curve graduation signal (confidence low)
--   raydium_amm_v4     Raydium AMM v4 initialize2
--   raydium_cpmm       Raydium CPMM initialize / createPool
--   raydium_clmm       Raydium CLMM initialize
--
--   launch_signal      = 1 when the tx created the token (launchpad
--                        create / initialize_v2)
--   graduation_signal  = 1 when the tx moved the token onto an AMM / off
--                        the bonding curve
--   confidence         = high (exact discriminator / config-account
--                        match), medium (parsed instruction-name match),
--                        low (program-presence graduation match)
--
-- mint is the new token (meta.postTokenBalances[0].mint); creator is the
-- transaction fee payer (accountKeys[0]).
--
-- Also ensured idempotently at runtime by
-- lite_indexer.strategies.solana.launch.ensure_solana_launches_table, so
-- this file is for operators provisioning the schema ahead of a deploy.
--
-- Run idempotently anytime:
--   clickhouse-client < migrations/009_solana_launch.sql

CREATE TABLE IF NOT EXISTS wallet_memory.solana_launches (
    chain LowCardinality(String) DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    mint String CODEC(ZSTD(3)),
    creator String DEFAULT '',
    venue LowCardinality(String) DEFAULT '',
    launch_signal UInt8 DEFAULT 0,
    graduation_signal UInt8 DEFAULT 0,
    confidence LowCardinality(String) DEFAULT 'low',
    instruction_name String DEFAULT '',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (mint, slot)
TTL block_timestamp + INTERVAL 365 DAY;

-- The API user reads launch data (defense-in-depth: SELECT-only).
GRANT SELECT ON wallet_memory.solana_launches TO chain_api;
-- The indexer's write user (rmi) needs INSERT for new tables added
-- after the original deploy (mirrors migrations/002_grants.sql).
GRANT SELECT, INSERT ON wallet_memory.solana_launches TO rmi;
