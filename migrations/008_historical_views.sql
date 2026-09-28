-- Migration 008: Transparent Historical Access — unified *_historical views.
--
-- The indexer keeps the last ~30 days in ClickHouse (hot) and the full
-- history back to genesis in B2 parquet (cold, daily exports). These
-- views are the "unified view that spans both" for DIRECT SQL access:
--   SELECT * FROM wallet_memory.evm_transfers_historical WHERE ...
-- ClickHouse UNIONs hot rows with cold rows read on-demand from the B2
-- HTTP proxy (scripts/serve-b2-http.sh, `rclone serve http`).
--
-- The API's transparent layer (lite_indexer.storage.historical) rewrites
-- single-table SELECTs over these tables to the *_historical views when
-- they exist, so every existing endpoint serves full history with zero
-- API changes.
--
-- Requirements / notes:
--   1. Run scripts/serve-b2-http.sh so the proxy answers on :8090
--      (files served from b2-crypt:chain-data → /parquet/...).
--      Alternative: mount `rclone mount b2-crypt:chain-data/parquet
--      /var/data/b2-mount` and replace the url() with
--      file('/var/data/b2-mount/**/*.parquet').
--   2. CREATE VIEW is lazy — the proxy does not need to be up when the
--      migration runs, only when the views are queried.
--   3. url() supports the `**` recursive glob for parquet discovery;
--      filters are pushed down after the file list is resolved, so
--      wide-open scans (no WHERE) are slow by design. The Python
--      HistoricalAccess layer does smart per-window file selection for
--      low-latency reads; these views are for direct SQL access.
--   4. Cold parquet is exported with SELECT *, so the url() schema below
--      mirrors the canonical ClickHouse schemas (keep in sync with
--      lite_indexer.storage.cold_store.TABLE_COLUMNS).
--
-- Run idempotently anytime:
--   clickhouse-client < migrations/008_historical_views.sql

-- evm_transfers: hot + cold.
CREATE OR REPLACE VIEW wallet_memory.evm_transfers_historical AS
SELECT chain, block_number, tx_hash, from_address, to_address,
       token_address, value, block_timestamp, log_index, ingested_at
FROM wallet_memory.evm_transfers
UNION ALL
SELECT chain, block_number, tx_hash, from_address, to_address,
       token_address, value, block_timestamp, log_index, ingested_at
FROM url(
    'http://localhost:8090/parquet/**/*.parquet',
    'Parquet',
    'chain LowCardinality(String), block_number UInt64, tx_hash String,
     from_address String, to_address String, token_address String,
     value String, block_timestamp DateTime, log_index UInt32,
     ingested_at DateTime'
);

-- dex_events: hot + cold.
CREATE OR REPLACE VIEW wallet_memory.dex_events_historical AS
SELECT type, chain, block_number, block_timestamp, tx_hash, log_index,
       contract_address, dex, version, token0, token1, sender, recipient,
       amount0_in, amount1_in, amount0_out, amount1_out, amount0, amount1,
       whale, price_usd, volume_usd, ingested_at, source
FROM wallet_memory.dex_events
UNION ALL
SELECT type, chain, block_number, block_timestamp, tx_hash, log_index,
       contract_address, dex, version, token0, token1, sender, recipient,
       amount0_in, amount1_in, amount0_out, amount1_out, amount0, amount1,
       whale, price_usd, volume_usd, ingested_at, source
FROM url(
    'http://localhost:8090/parquet/**/*.parquet',
    'Parquet',
    'type LowCardinality(String), chain LowCardinality(String),
     block_number UInt64, block_timestamp DateTime, tx_hash String,
     log_index UInt32, contract_address String, dex String,
     version String, token0 String, token1 String, sender String,
     recipient String, amount0_in String, amount1_in String,
     amount0_out String, amount1_out String, amount0 String,
     amount1 String, whale UInt8, price_usd Float64, volume_usd Float64,
     ingested_at DateTime, source String'
);

-- deployer_events: hot + cold (no log_index column in this table).
CREATE OR REPLACE VIEW wallet_memory.deployer_events_historical AS
SELECT deployer, deployer_eoa, token_address, token0, token1, pair_address,
       chain, block_number, block_timestamp, tx_hash, type, factory,
       ingested_at, source
FROM wallet_memory.deployer_events
UNION ALL
SELECT deployer, deployer_eoa, token_address, token0, token1, pair_address,
       chain, block_number, block_timestamp, tx_hash, type, factory,
       ingested_at, source
FROM url(
    'http://localhost:8090/parquet/**/*.parquet',
    'Parquet',
    'deployer String, deployer_eoa String, token_address String,
     token0 String, token1 String, pair_address String,
     chain LowCardinality(String), block_number UInt64,
     block_timestamp DateTime, tx_hash String, type String,
     factory String, ingested_at DateTime, source String'
);

-- internal_transfers: hot + cold (trace_address is an Array(UInt16)).
CREATE OR REPLACE VIEW wallet_memory.internal_transfers_historical AS
SELECT chain, block_number, block_timestamp, tx_hash, trace_address,
       from_address, to_address, value_wei, call_type, gas_used, error,
       input_method_id, ingested_at
FROM wallet_memory.internal_transfers
UNION ALL
SELECT chain, block_number, block_timestamp, tx_hash, trace_address,
       from_address, to_address, value_wei, call_type, gas_used, error,
       input_method_id, ingested_at
FROM url(
    'http://localhost:8090/parquet/**/*.parquet',
    'Parquet',
    'chain LowCardinality(String), block_number UInt64,
     block_timestamp DateTime64(3), tx_hash String,
     trace_address Array(UInt16), from_address String,
     to_address String, value_wei UInt256, call_type String,
     gas_used UInt64, error String, input_method_id String,
     ingested_at DateTime64(3)'
);

-- solana_spl_balance_changes: hot + cold (THA logical name
-- solana_transfers maps to this physical table).
CREATE OR REPLACE VIEW wallet_memory.solana_spl_balance_changes_historical AS
SELECT chain, slot, block_timestamp, tx_hash, token_account, mint, owner,
       pre_amount, post_amount, delta, decimals, token_symbol,
       token_name, ingested_at
FROM wallet_memory.solana_spl_balance_changes
UNION ALL
SELECT chain, slot, block_timestamp, tx_hash, token_account, mint, owner,
       pre_amount, post_amount, delta, decimals, token_symbol,
       token_name, ingested_at
FROM url(
    'http://localhost:8090/parquet/**/*.parquet',
    'Parquet',
    'chain LowCardinality(String), slot UInt64, block_timestamp DateTime,
     tx_hash String, token_account String, mint String, owner String,
     pre_amount String, post_amount String, delta String,
     decimals UInt8, token_symbol String, token_name String,
     ingested_at DateTime'
);

-- The API user reads the views (defense-in-depth: SELECT-only).
GRANT SELECT ON wallet_memory.evm_transfers_historical TO chain_api;
GRANT SELECT ON wallet_memory.dex_events_historical TO chain_api;
GRANT SELECT ON wallet_memory.deployer_events_historical TO chain_api;
GRANT SELECT ON wallet_memory.internal_transfers_historical TO chain_api;
GRANT SELECT ON wallet_memory.solana_spl_balance_changes_historical TO chain_api;
