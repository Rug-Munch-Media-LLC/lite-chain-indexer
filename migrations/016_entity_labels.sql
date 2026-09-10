-- Migration 016: canonical entity_labels label graph (indexer-owned).
--
-- Labels are DATA: the canonical home of the wallet label graph is the
-- indexer's ClickHouse warehouse (``wallet_memory``), alongside the raw
-- on-chain tables. The scanner previously created/owned this table via its
-- label_importer; it now queries the indexer's ``/api/v1/labels`` API and
-- falls back to a local read only when the indexer is unreachable.
--
-- Schema matches the scanner's canonical 9-column model
-- (rugmunch-scanner .../label_importer/config.py ENTITY_LABELS_DDL) so
-- existing rows and the label-growth pipeline remain compatible.
-- ReplacingMergeTree keyed on (address, chain_id, source): re-importing a
-- source is an idempotent upsert, never a duplicate.
--
-- UP
CREATE DATABASE IF NOT EXISTS wallet_memory;

CREATE TABLE IF NOT EXISTS wallet_memory.entity_labels (
    address     String,
    chain_id    LowCardinality(String),
    entity_type LowCardinality(String) DEFAULT 'unknown',
    label_type  LowCardinality(String) DEFAULT 'unknown',
    name        String DEFAULT '',
    source      LowCardinality(String) DEFAULT '',
    provenance  LowCardinality(String) DEFAULT '',
    first_seen  DateTime DEFAULT now(),
    confidence  Float32 DEFAULT 0.8
) ENGINE = ReplacingMergeTree(first_seen)
ORDER BY (address, chain_id, source);

-- Reads (GET /api/v1/labels) use the SELECT-only API user.
GRANT SELECT ON wallet_memory.entity_labels TO chain_api;

-- Writes (POST /api/v1/labels, label-growth) use the indexer writer user.
GRANT SELECT, INSERT ON wallet_memory.entity_labels TO rmi;

-- DOWN (rollback — run manually only; DROP is intentionally commented so
-- no automated migration runner can ever execute it):
-- DROP TABLE IF EXISTS wallet_memory.entity_labels;
