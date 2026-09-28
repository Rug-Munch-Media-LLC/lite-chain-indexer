-- Migration 023: reconciliation_results + address_backfill_state DDL.
--
-- WHY: both tables were created by `CREATE TABLE IF NOT EXISTS` inside Python
-- string constants (storage/reconciliation.py::_RECONCILIATION_DDL and
-- query/address_backfill.py::ADDRESS_BACKFILL_DDL). A table could therefore
-- exist on a deployment with no recorded migration, and no way to tell which
-- version of the DDL produced it. 24 other CREATE TABLE sites across
-- lite_indexer/ have the same shape; these two are the ones moved to a
-- migration in this pass. The runtime paths keep their CREATE TABLE IF NOT
-- EXISTS so a fresh dev box still self-provisions — the migration is the
-- record, the runtime is the convenience.
--
-- reconciliation_results RETENTION (new, deliberate):
--   The old DDL was ENGINE = MergeTree with NO TTL and NO partition key —
--   append-only and unbounded, one row per (chain, address, checked_at).
--   check_reconciliations samples addresses on a schedule, so this table
--   grows forever. It is partitioned by month and TTL'd at 90 days, which
--   keeps a full quarter of trend history (the table exists to make
--   balance drift trends queryable) and bounds the store. The 90d class
--   matches internal_transfers/dex_events in 021_retention_classes.sql;
--   reconciliation_results is not in RETENTION_POLICY because it holds no
--   chain-derived truth, only samples of it.
--
-- address_backfill_state RETENTION: unchanged, none. ReplacingMergeTree(
-- fetched_at) keyed on (chain, address) keeps exactly one row per address
-- — the current backfill state — so an unbounded TTL is correct here and a
-- TTL would destroy the row that tells the lazy backfill a fetch already
-- happened.

CREATE TABLE IF NOT EXISTS wallet_memory.reconciliation_results (
    chain LowCardinality(String),
    address String,
    checked_at DateTime,
    computed_balance Int128,
    on_chain_balance Int128,
    delta Int128,
    delta_pct Float64,
    status LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(checked_at)
TTL checked_at + INTERVAL 90 DAY
ORDER BY (chain, address, checked_at);

CREATE TABLE IF NOT EXISTS wallet_memory.address_backfill_state (
    address String CODEC(ZSTD(3)),
    chain LowCardinality(String),
    status LowCardinality(String),
    signatures_seen UInt64,
    rows_written UInt64,
    provider LowCardinality(String),
    fetched_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (chain, address);
