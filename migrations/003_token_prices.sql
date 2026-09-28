-- Migration 003: USD price reference table for DEX whale detection.
--
-- One row per (chain, token_address). ReplacingMergeTree dedupes by the
-- primary key so repeated refreshes collapse to one row and the last
-- write wins. Maintained by lite_indexer/pricing.py::refresh_prices from
-- the DefiLlama + CoinGecko free tiers (no API keys).
--
-- Also ensured idempotently at runtime by
-- pricing.ensure_token_prices_table, so this file is for operators
-- provisioning the schema ahead of a deploy.

CREATE TABLE IF NOT EXISTS wallet_memory.token_prices (
    chain LowCardinality(String),
    token_address String,
    symbol String DEFAULT '',
    price_usd Float64,
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, token_address);
