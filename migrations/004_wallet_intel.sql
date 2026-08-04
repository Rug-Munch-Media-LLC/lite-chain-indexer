-- Migration 004: wallet intelligence tables.
--
-- The intel package (lite_indexer.intel) reads wallet labels, scam
-- addresses, profiles, and entity clusters from the ClickHouse
-- warehouse. The first three tables are provisioned by the upstream
-- indexer; this migration idempotently ensures the entity-cluster table
-- exists so fresh deployments can run entity resolution + clustering
-- immediately.
--
-- wallet_entities: one row per (entity_id, wallet_address) membership
-- produced by clustering heuristics (shared funding, behavior vectors,
-- label co-occurrence). Read by EntityResolver.cluster_addresses; a
-- background job may populate it, but the resolver also clusters
-- on-the-fly when the table is empty.
--
-- Also ensured idempotently at runtime by
-- lite_indexer.intel — see the ensure step in the API/router docs.

CREATE TABLE IF NOT EXISTS wallet_memory.wallet_entities (
    entity_id String,
    wallet_address String,
    chain_id String DEFAULT '',
    heuristic_type LowCardinality(String) DEFAULT 'shared_funding',
    confidence_score Float32 DEFAULT 0.7,
    entity_label String DEFAULT '',
    entity_category LowCardinality(String) DEFAULT 'unknown',
    first_linked_at DateTime DEFAULT now(),
    last_updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(last_updated_at)
ORDER BY (entity_id, wallet_address)
TTL last_updated_at + INTERVAL 180 DAY;
