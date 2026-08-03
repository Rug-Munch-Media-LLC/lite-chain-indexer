# Changelog

All notable changes to Lite Chain Indexer will be documented in this file.

## [Unreleased]

### Features

- **NDJSON write-ahead log now optional (default off)** — the NDJSON
  replay buffer is redundant with ClickHouse (the durable store), so it is
  gated behind the `NDJSON_ENABLED` env flag (default `false`). When
  disabled: the `NDJSONWriter` is never constructed, sync step 6 skips the
  NDJSON writes, and the disk-cap check excludes `*.jsonl` /
  `*.jsonl.zst` files so the cap only counts Parquet/state data. Recovery
  without NDJSON is restart-from-state (`state.to_block`): at most one
  sync cycle (~30s) is lost on crash and re-fetched from RPC. The NDJSON
  code is retained behind the flag for rollback; the fleet rotation cron
  (`rotate-lite-indexer-data.sh`, `compress-stale-ndjson.sh`) no-ops when
  the flag is off.
- **Balance reconciliation** (`storage/reconciliation.py`) — TrueBlocks-style
  completeness check: computes each sampled wallet's balance from
  `evm_transfers` deltas (incoming minus outgoing via
  `sum(toInt128(value))`) and compares it against on-chain
  `eth_getBalance`. Status `MATCH` (delta == 0), `APPROXIMATE` (<1%), or
  `MISMATCH` (with the gap). Results are cached in
  `wallet_memory.reconciliation_results` (MergeTree, one row per
  (chain, address, checked_at)). Lightweight + non-blocking: runs on a
  random sample of 100 wallets per chain (configurable
  `RECONCILIATION_SAMPLE_SIZE`), concurrency-capped RPC calls, and is
  gated by `RECONCILIATION_ENABLED`. Exposed as the `reconcile`
  subcommand for a daily cron.
- **Reorg staging** (`strategies/evm/reorg.py`) — Envio/TrueBlocks-style
  reorg-safe block confirmation. Tracks the last N blocks as "unripe"
  using `wallet_memory.block_checkpoints` (ReplacingMergeTree keyed by
  (chain, block_number)); after `MAX_REORG_DEPTH[chain]` blocks
  (ethereum/bsc/arbitrum/optimism/base/avalanche/gnosis: 12,
  polygon: 32) the stored hash is compared against the current chain
  hash. On mismatch, `evm_transfers` + `dex_events` above the fork
  point are DELETEd and the indexer re-fetches from the fork. Wired
  into `EVMChainStrategy.reorg_stage` after each block batch and gated
  by `REORG_PROTECTION_ENABLED` (default on for EVM, off for
  Solana/Tron).

### Fixes

- **Restore `pair_address` in deployer_events** — the F0 deployer EOA fix
  repurposed `token_address` to hold token0 and dropped the pair/pool
  address, breaking scanner joins on `pair_address`. `pair_address` is
  added back as its own column (`String DEFAULT '' AFTER token1`), the
  insert now writes all address columns (`deployer`, `token_address`,
  `token0`, `token1`, `pair_address`, `factory`, `deployer_eoa`), and
  `_ensure_deployer_columns()` migrates existing tables idempotently.

## [1.3.0] — 2026-08-02

### Performance

- **Address-first wallet access path** (`storage/projections.py`) —
  electrs-inspired projections on `evm_transfers` ordered by
  `(chain, address, block_number, log_index)` so "all transfers for
  wallet X" is served from an address-keyed projection instead of a full
  table scan. ClickHouse projections cannot `UNION ALL`, so the wallet
  history is split into `wallet_out_proj` (from_address, 'out') and
  `wallet_in_proj` (to_address, 'in'), each carrying a direction label.
  Projections are maintained for new inserts automatically; pass
  `materialize_existing=True` once to backfill the existing 264M rows.
  Gated by `WALLET_PROJECTION_ENABLED`.

### Storage

- **Token metadata reference table** (`storage/token_meta.py`) —
  cryo-inspired `wallet_memory.token_meta_ref`: one row per token instead
  of `token_symbol`/`token_name` on every transfer row (~60 bytes x 264M
  rows). When `TOKEN_META_ENABLED`, transfer rows store empty
  symbol/name and the reference table is upserted once per unique token
  per batch (ReplacingMergeTree makes re-upserts idempotent).
- **Per-token incremental balance cache** (`storage/balance_cache.py`) —
  electrs lazy cache: `wallet_memory.holder_balances` (ReplacingMergeTree)
  is read-modify-written per holder per batch so holder queries never
  aggregate the full `evm_transfers` table. `get_token_holders()` serves
  top-holder queries from the pre-computed answer. Gated by
  `BALANCE_CACHE_ENABLED`.
- **Auto-maintained holder balances** (`storage/balance_cache.py`) —
  `wallet_memory.holder_balances_mv`, a SummingMergeTree materialized
  view fed by `evm_transfers` inserts, so ClickHouse maintains balances
  itself (query the inner table via `get_auto_holders()`). Gated by
  `HOLDER_BALANCES_MV_ENABLED`.
- **Indexer wiring** — `ClickHouseWriter.write_wallet_features()` runs
  after transfer inserts succeed (so caches never diverge from
  `evm_transfers`); each feature is independently gated by its own env
  flag and all DDL is idempotent and non-fatal.

## [1.2.0] — 2026-08-02

### Performance

- **Bloom pre-filtering** (`strategies/evm/bloom.py`) — address-indexed
  log queries warm a per-block logs-bloom cache from fetched block
  headers, letting transfer scanning pre-filter candidate blocks without
  extra RPC round trips. LRU-cached with an optional Redis backing store;
  falls back gracefully when Redis is unavailable.
- **Chunked indexing** (`chunks.py`) — block/slot ranges are split into
  configurable chunks (50 EVM blocks, 500 Solana slots) so each chunk is
  one `eth_getLogs` call, fetched in parallel. Cuts RPC round trips by
  the chunk-size factor and avoids "response too large" errors from
  restrictive free RPCs.
- **Chain parallelism** (`chains/parallelism.py`) — per-chain resource
  guidance: CPU affinity (Linux best-effort), memory budgets (Solana
  gets the most headroom), and priorities that scale polling frequency
  for high-value chains (Ethereum, Solana).

### Storage

- **Column TTL** (`storage/clickhouse.py`) — configurable column-level
  TTL (`CH_TTL_ENABLED` / `RETENTION_DAYS`) degrades heavy detail
  columns to their type defaults while summary columns are retained,
  cutting storage 30-50% while keeping every scanner query answerable.
- **Parquet cold tier** (`storage/parquet.py` +
  `scripts/lite-indexer-parquet-daily.sh`) — daily rollup exports rows
  older than `PARQUET_COLD_DAYS` from ClickHouse into per-chain/per-date
  Parquet files, uploads them to B2, then deletes them from ClickHouse to
  reclaim disk. Queries via `storage/duckdb_query.py`.
- **F0 deployer EOA fix** — deployer events now resolve the real deployer
  EOA (the transaction `from` address) via `eth_getTransactionByHash`
  instead of recording the factory address. Batched with a bounded
  semaphore so RPC overhead stays low; falls back gracefully when
  resolution is unavailable.

### RPC & Reliability

- **RPC routing** (`rpc/routing.py`) — per-method endpoint selection:
  heavy `eth_getLogs` calls prefer free public tiers, latency-sensitive
  calls (`eth_getTransactionReceipt`, `eth_call`) prefer dedicated
  API-key nodes. Adds a usage/cost ledger and per-endpoint circuit
  breaker (open on failure bursts, half-open probe, close on success).
- **Narrowed exception handling** — 12 broad `except Exception` clauses
  narrowed to specific transport/parse exception tuples
  (`httpx.HTTPError`, `ConnectionError`, `OSError`, `TimeoutError`,
  `ValueError`, `KeyError`, `TypeError`), and silent `pass`/`logger.debug`
  failure paths upgraded to `logger.warning` so operators can see
  suppressed failures.
- **Version fix** — package version bumped to 1.2.0 in `pyproject.toml`.

## [1.1.0] — 2026-08-01

### Docs (this release)

- **Complete README rewrite** — professional, SEO-focused public face of the
  project: what it does, why it's different, quick start from Forgejo, live
  per-chain stats from ClickHouse, architecture + data-flow diagrams, honest
  performance numbers, comparison table, badges, and FAQ.
- **New `docs/FAQ.md`** — answers the common questions (cost/RPCs, supported
  chains, comparisons, historical queries, adding chains, hardware, contributing).
- **New `docs/COMPARISON.md`** — detailed feature/pricing comparison vs Ponder,
  Subsquid, Helius, and The Graph.
- **Fixed dead links** — clone URLs and related-project links now point at
  Forgejo (git.rugmunch.io) instead of non-existent GitHub repositories.
  Removed the copy-paste link to getlago/lago.
- **CI badge now points at Forgejo Actions** — the repo is not on GitHub; the
  `.github/workflows/ci.yml` never ran. The live badge renders from
  `.forgejo/workflows/ci.yml` on git.rugmunch.io.
- **Corrected production stats** in docs: ~2 GB RAM for all 16 chains
  (previously claimed "<200MB") and 231M+ transfer events / 58M+ DEX events
  (previously "79M+"), verified live from the ClickHouse `wallet_memory` store.
- **Corrected chain table** — 16 chains (14 EVM + Solana + Tron), 123 endpoints,
  updated per-chain event counts from production.
- **Updated project structure** in CONTRIBUTING.md to match the refactored
  package layout (`chains/`, `rpc/`, `dex/`, `storage/`, `strategies/`).

## [1.0.0] — 2026-07-31

### Added (since 0.1.0)

- **Non-EVM support** — Solana (SPL token balance changes via
  `getSignaturesForAddress` + `getTransaction`) and Tron (TRC-20 transfers via
  the TronGrid event API). New strategy pattern: `strategies/evm`,
  `strategies/solana`, `strategies/tron`.
- **Chain reorg detection** — detects and flags reorganizations instead of
  silently ingesting bad data.
- **Per-chain block range limits** (`CHAIN_BLOCK_RANGE_LIMITS`) and
  provider-aware `eth_getLogs` range handling.
- **Free-tier API-key endpoints** — Alchemy, GetBlock, Chainstack, QuickNode,
  Helius (`${ENV_VAR}` placeholders) as Tier-4 fallbacks.
- **Per-chain block times** for accurate timestamp estimation on fast chains
  (Base 2s, Arbitrum 0.25s, etc.).
- **Consolidated metrics module** (`metrics.py`).
- **Forgejo Actions CI workflow** (`.forgejo/workflows/ci.yml`) — lint, type
  check, tests on Python 3.11/3.12.
- **Pruned Base chain RPCs** — removed endpoints that no longer support
  `eth_getLogs` on the free tier.
- **Inline NDJSON compression hook** — stale `.jsonl` files auto-compress to
  `.zst` after 15 minutes.
- **DuckDB query layer** (`storage/duckdb_query.py`) — SQL queries over the B2
  parquet archive with `transfers`, `dex`, and `spl_balance_changes` views.

### Technical Highlights

- Single Python process, `asyncio`-parallel daemon (one independent task per chain).
- ~2 GB RAM for all 16 chains concurrently.
- Thread-safe state management with file locking.
- 3M+ events/hour sustained throughput on free RPCs.
- 231M+ transfer events and 58M+ DEX events indexed in production.

## [0.1.0] — 2026-07-30

### Added

- **Initial open source release** of Lite Chain Indexer — a free, lightweight,
  multi-chain blockchain indexer using only public RPC endpoints.
- **14 EVM chains supported**: Ethereum, Base, BSC, Polygon, Arbitrum, Optimism,
  Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo.
- **111 RPC endpoints** across all chains with automatic health checking,
  tiered rotation, and self-healing backoff.
- **ERC-20 Transfer indexing**: captures all Transfer events across every
  contract via eth_getLogs. No address filter — comprehensive coverage.
- **DEX swap decoding**: Uniswap V2/V3 style swap events with token0/token1
  resolution. Supports PancakeSwap, QuickSwap, and all standard V2/V3 forks.
- **PairCreated/PoolCreated tracking**: builds a persistent pair→tokens
  mapping from DEX factory events. Survives restarts.
- **Deployer tracking**: per-chain factory activity for deployer reputation.
- **Daemon mode**: continuously index all chains in parallel with
  adaptive per-chain polling intervals.
- **Self-healing RPC pool**: per-provider token bucket rate limiting,
  exponential backoff, endpoint verification at startup.
- **Disk cap protection**: configurable hard limit on data directory size
  (default: 10 GB). Never run out of space.
- **Dual output**: NDJSON (JSONL) files organized by chain/date + ClickHouse
  native protocol inserts for analytical queries.
- **Endpoint verification**: automatically tests all endpoints at startup
  and only uses healthy ones. Range-aware fallback for archive-limited RPCs.
- **Per-provider rate limiting**: token bucket algorithm with per-provider
  limits. Prevents hitting free-tier caps.
- **Provider-aware block ranges**: respects endpoint-specific eth_getLogs
  range limits. Automatically adjusts batch sizes.
