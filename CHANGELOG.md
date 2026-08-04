# Changelog

All notable changes to Lite Chain Indexer will be documented in this file.

## [Unreleased]

### Features

- **NATS JetStream real-time streaming** (`lite_indexer/streaming/`) — the
  indexer fans events out to NATS immediately after ClickHouse writes:
  transfers → `chain.{chain}.transfer`, DEX swaps →
  `chain.{chain}.dex.swap`, deployer events → `chain.{chain}.deploy`, and
  whale-flagged swaps additionally → `alerts.whale.{chain}`. Publishing is
  best-effort by design (NATS is a fan-out layer, never the durable store):
  every failure path is logged and swallowed so NATS downtime can never
  block indexing. A circuit breaker opens after
  `NATS_CIRCUIT_FAILURE_THRESHOLD` consecutive failures and drops
  publishes for `NATS_CIRCUIT_COOLDOWN_SECONDS` before half-open probing.
  JetStream is used when available (auto-provisions a `CHAIN_EVENTS`
  stream capturing `chain.>` / `alerts.>`), degrading to core NATS publish
  otherwise. Gated by `NATS_ENABLED` (default off); server runs via
  `compose.nats.yml` (NATS 2.10-alpine, JetStream + WebSocket on 8080).
  New optional dependency: `pip install -e '.[streaming]'` (nats-py).

## [1.3.0] — 2026-08-03

### Features

- **Lazy per-chain DEX/deployer indexing** (`chains/demand.py`) — every chain
  starts with transfer events only; DEX swap and deployer event indexing is
  enabled by default for top chains (ethereum, bsc, polygon, arbitrum, base,
  solana) and only after the scanner signals demand for long-tail chains
  (scroll, mantle, linea, fantom, avalanche, gnosis). Demand is signaled via a
  lightweight file touch (`{INDEXER_DEMAND_DIR}/{chain}/dex|deployer`) — no
  cross-service imports — and once enabled stays enabled (persisted in the
  state file). Setting `LAZY_INDEXING_ENABLED=0` restores the legacy behavior.
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
- **Env placeholder fix** — `${ENV_VAR}` RPC endpoint templates are now
  expanded and filtered at import time: unexpanded placeholder URLs are
  dropped from the pool instead of being retried forever
  (1583 `verify_endpoint_failed`/30min → 0). Defense-in-depth in the
  health checker and discovery path.
- **Parquet cold-tier export** — per-chain/per-date rollup of old ClickHouse
  rows into verified Parquet files, uploaded to the B2 permanent tier via
  rclone, then deleted from ClickHouse only after the upload is verified
  (`--delete-after-upload`). Idempotent (state marker + `parquet_export_state`
  audit table). Script: `scripts/lite-indexer-parquet-daily.sh`
  (daily 03:30 UTC; the fleet copy runs 6-hourly).

### Storage

- **Enrichment stripping** (`storage/token_meta.py`) — `token_symbol` /
  `token_name` are no longer written to every transfer row (~60 bytes x 278M
  rows). Transfer rows now carry only the 10 hot-path columns; token metadata
  lives in the `wallet_memory.token_meta_ref` reference table, upserted once
  per unique token per batch (ReplacingMergeTree makes re-upserts
  idempotent). `TOKEN_META_ENABLED` is always on and non-configurable.
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

### RPC & Reliability

- **Bloom byte-order fix** (`strategies/evm/bloom.py`) — `_test_bit` now
  reads byte `255 - p // 8` (geth/MSB-first logsBloom layout). The previous
  little-endian read produced 100% false negatives on address queries,
  silently skipping blocks that actually contained the address. Now
  geth-conformant with 6 regression tests. Bloom fetch concurrency is
  bounded (`BLOOM_FETCH_CONCURRENCY`).
- **Circuit breaker close fix** (`rpc/client.py`) — `record_endpoint_success`
  is now wired into `RpcClient.call()`, so breakers actually close on
  recovery instead of oscillating open↔half-open forever. The
  chainid.network registry is cached (1 fetch/hour instead of per-chain).
- **Restore `pair_address` in deployer_events** — the F0 deployer EOA fix
  repurposed `token_address` to hold token0 and dropped the pair/pool
  address, breaking scanner joins on `pair_address`. `pair_address` is
  added back as its own column (`String DEFAULT '' AFTER token1`), the
  insert now writes all address columns (`deployer`, `token_address`,
  `token0`, `token1`, `pair_address`, `factory`, `deployer_eoa`), and
  `_ensure_deployer_columns()` migrates existing tables idempotently.

### Docs

- **README updated for v1.3.0** — version badge, quickstart with `pip
  install` + env vars, new feature list (lazy indexing, enrichment
  stripping, NDJSON gate, reconciliation, reorg staging), refreshed
  architecture diagram, and honest performance claims (278M+ transfers,
  ~30s freshness, free RPCs only).
- **`.env.example` updated** — all new flags documented (`NDJSON_ENABLED`,
  `LAZY_INDEXING_ENABLED`, `RECONCILIATION_*`, `REORG_PROTECTION_ENABLED`,
  `TOKEN_META_ENABLED`, `WALLET_PROJECTION_ENABLED`, `BALANCE_CACHE_ENABLED`,
  `HOLDER_BALANCES_MV_ENABLED`, `BLOOM_ENABLED`, `CH_TTL_ENABLED`,
  `PARQUET_*`).

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
