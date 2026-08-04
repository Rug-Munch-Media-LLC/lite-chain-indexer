-- Migration 003: internal-transfer trace table.
--
-- The indexer normally only sees ERC-20 Transfer logs. Internal ETH flows
-- (WETH unwraps, bridge deposits, fee flows, SELFDESTRUCT payouts) are
-- invisible to eth_getLogs. This table stores one row per value-moving
-- internal call (CALL value>0, CREATE value>0, SELFDESTRUCT) collected
-- from free trace_block / debug_traceBlockByNumber RPCs by
-- lite_indexer.traces.TraceCollector.
--
-- Also ensured idempotently at runtime by
-- lite_indexer.storage.trace_store.ensure_internal_transfers_table, so
-- this file is for operators provisioning the schema ahead of a deploy.
--
-- trace_address: nested call path, e.g. [0,1,2] for the third sub-call
--                of the second sub-call of the root frame ([] = top-level).
-- input_method_id: first 4 bytes of call data (0xa9059cbb = ERC-20
--                transfer, 0x2e1a7d4d = WETH.withdraw, etc.).
-- call_type: 'call' | 'create' | 'selfdestruct'. DELEGATECALL/STATICCALL
--                frames are intentionally NOT stored (they do not move
--                value; a delegatecall's value field is inherited context,
--                not a new transfer).

CREATE TABLE IF NOT EXISTS wallet_memory.internal_transfers (
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime64(3),
    tx_hash String,
    trace_address Array(UInt16),
    from_address String,
    to_address String,
    value_wei UInt256,
    call_type LowCardinality(String),
    gas_used UInt64,
    error String DEFAULT '',
    input_method_id String DEFAULT '',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (chain, block_number, tx_hash, trace_address)
TTL block_timestamp + INTERVAL 90 DAY;
