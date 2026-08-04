-- Migration 004: cross-chain forensics tables.
--
-- Bridge mappings persist confirmed cross-chain fund movements so
-- the forensics FundTracer can reuse matches without re-running the
-- time-window heuristic on every trace. One row per direction.
--
-- source_address / source_chain — depositor on chain A.
-- dest_address / dest_chain       — recipient on chain B.
-- bridge_name                     — registry name of the bridge contract.
-- tx_hash                         — source-chain deposit transaction.
-- bridged_at                      — source-chain deposit timestamp.
-- confidence                      — 0..1 match confidence.
--
-- Also ensured idempotently at runtime by
-- lite_indexer.forensics.bridge_store.ensure_bridge_mappings_table.

CREATE TABLE IF NOT EXISTS wallet_memory.bridge_mappings (
    source_address String CODEC(ZSTD(3)),
    source_chain String CODEC(ZSTD(3)),
    dest_address String CODEC(ZSTD(3)),
    dest_chain String CODEC(ZSTD(3)),
    bridge_name LowCardinality(String),
    tx_hash String,
    bridged_at DateTime,
    confidence Float32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (source_address, source_chain)
TTL bridged_at + INTERVAL 180 DAY;
