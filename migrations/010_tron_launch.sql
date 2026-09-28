-- Migration 010: Tron token-launch detection (SunPump / SunSwap).
--
-- Captures meme-coin launch lifecycle events on Tron so scanners can
-- answer "when did this token launch, who created it, and when did it
-- graduate to a SunSwap pool?":
--
--   event_type = 'sunpump_launch'    — TokenCreate on the SunPump
--     contract (token_address = new token, creator, token_index).
--   event_type = 'sunswap_pair'      — PairCreated on the SunSwap V2
--     factory (token0/token1/pair_address, factory).
--   event_type = 'sunpump_graduation'— launchToDEX call on SunPump that
--     seeds the token onto SunSwap (token_address + pair_address).
--
-- One row per event; dedup is the fetch layer's responsibility (same
-- convention as evm_transfers / dex_events, keyed on tx_hash + log_index).
--
-- Written by lite_indexer.strategies.tron.launch.write_launch() (launches)
-- and the Tron strategy's polling integration point for pairs/graduations.
--
-- Run idempotently anytime:
--   clickhouse-client < migrations/010_tron_launch.sql

CREATE TABLE IF NOT EXISTS wallet_memory.tron_launches (
    chain LowCardinality(String) DEFAULT 'tron',
    block_number UInt64,
    block_timestamp DateTime,
    tx_hash String,
    log_index UInt32 DEFAULT 0,
    event_type LowCardinality(String),
    token_address String CODEC(ZSTD(3)),
    creator String DEFAULT '',
    token_index UInt64 DEFAULT 0,
    contract_address String DEFAULT '',
    factory String DEFAULT '',
    pair_address String DEFAULT '',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (token_address, block_number)
TTL block_timestamp + INTERVAL 365 DAY;

-- The API user reads launch data (defense-in-depth: SELECT-only).
GRANT SELECT ON wallet_memory.tron_launches TO chain_api;
-- The indexer's write user (rmi) needs INSERT for new tables added
-- after the original deploy (mirrors migrations/002_grants.sql).
GRANT SELECT, INSERT ON wallet_memory.tron_launches TO rmi;
