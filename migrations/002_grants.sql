-- Migration 002: ClickHouse grants for the rmi user.
--
-- The `rmi` user (used by the backend API and the indexer's read/write
-- paths) needs SELECT+INSERT on tables added since the original deploy.
-- Run after provisioning these tables, or idempotently anytime:
--   clickhouse-client < migrations/002_grants.sql

GRANT SELECT, INSERT ON wallet_memory.internal_transfers TO rmi;
GRANT SELECT, INSERT ON wallet_memory.solana_transfers TO rmi;
GRANT SELECT, INSERT ON wallet_memory.token_prices TO rmi;
GRANT SELECT ON wallet_memory.wallet_entities TO rmi;
