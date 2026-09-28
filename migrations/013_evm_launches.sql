-- Migration 013: EVM token-launch events.
--
-- Minimal EVM launch detection: DEX pair/pool creation (PairCreated /
-- PoolCreated) from known factories is the closest universal "launch"
-- signal on EVM chains (there is no single launchpad like Solana
-- pump.fun or Tron SunPump). The deployer path already fetches and
-- verifies these events (factory tx receipt status == 1 + deployed code
-- at the pair address, see lite_indexer/strategies/evm/deployer_handler.py);
-- this table mirrors the same verified events so scanners can answer
-- "what launched recently" per EVM chain.
--
-- One row per pair/pool-creation event. token0 is the primary launch
-- token (token1 is usually the paired quote asset). version is the DEX
-- flavor (v2 pair / v3 pool).
--
-- Written by lite_indexer.strategies.evm.launch.write_launch() from the
-- deployer path's enriched pair events (lite_indexer/indexer/_writers.py
-- write_deployer_events), and also ensured idempotently at runtime by
-- ensure_evm_launches_table().
--
-- Run idempotently anytime:
--   clickhouse-client < migrations/013_evm_launches.sql

CREATE TABLE IF NOT EXISTS wallet_memory.evm_launches (
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime64(3),
    tx_hash String,
    token0 String CODEC(ZSTD(3)),
    token1 String CODEC(ZSTD(3)),
    pair_address String CODEC(ZSTD(3)),
    factory String DEFAULT '',
    version LowCardinality(String) DEFAULT 'v2',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (chain, block_number, tx_hash)
TTL block_timestamp + INTERVAL 365 DAY;

-- The API user reads launch data (defense-in-depth: SELECT-only).
GRANT SELECT ON wallet_memory.evm_launches TO chain_api;
-- The indexer's write user (rmi) needs INSERT for new tables added
-- after the original deploy (mirrors migrations/002_grants.sql).
GRANT SELECT, INSERT ON wallet_memory.evm_launches TO rmi;
