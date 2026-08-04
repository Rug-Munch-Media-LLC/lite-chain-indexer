# LiteChainIndexer 🪶

**The free, self-healing, multi-chain blockchain indexer.** Index transfers, DEX swaps, deployer activity, and internal-transfer traces across **16 chains (14 EVM + Solana + Tron)** using **only free public RPCs** — zero infrastructure cost, zero API keys required, ~2 GB RAM, and DuckDB-queryable parquet archives on B2. No node. No gateway. No bill. **359M+ rows in production on a $5 VPS.**

[![CI](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml/badge.svg?branch=main)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-v1.3.0-blue.svg)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/tags)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![16 chains](https://img.shields.io/badge/chains-16-success.svg)](#chains-covered)
[![359M+ rows](https://img.shields.io/badge/rows-359M%2B-success.svg)](#performance)

---

## What It Does

LiteChainIndexer is a lightweight, self-hosted **multi-chain blockchain indexer** that turns raw on-chain activity into queryable, analyzable data. It watches **ERC-20/TRC-20/SPL transfer events**, **DEX swap events** (Uniswap V2/V3 and forks), **pair/pool deployment events**, and **internal-transfer traces** (WETH unwraps, bridge deposits, fee flows) across **14 EVM chains plus Solana and Tron** — entirely through **free public RPC endpoints**. Events are written to ClickHouse in real time (address-first projections make wallet queries sub-second), then exported as **parquet snapshots to B2** so you can run **DuckDB SQL queries over historical data** with no node, no gateway, and no recurring bill.

It's the **free blockchain indexer** for anyone who wants **blockchain data without running a node** — analysts, wallets, dashboards, Telegram bots, and research pipelines.

---

## Why It's Different

- **$0/month forever** — runs entirely on free public RPC endpoints (61 curated endpoints across 16 chains; ~45 active by default, more when optional free-tier API keys are set). No paid APIs, no gateway fees, no infrastructure bill.
- **16 chains, one process** — Ethereum, Base, BSC, Polygon, Arbitrum, Optimism, Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo **+ Solana + Tron**. One Python process, `asyncio`-parallel daemon.
- **359M+ rows, address-first** — **359M+ rows** in the live hot store, served through address-first ClickHouse projections so "all transfers for wallet X" is a sub-second projection scan, not a full-table scan.
- **Self-healing by default** — backward-state guard (auto-recovers a clobbered state file from the ClickHouse max block), header-aware RPC throttling (respects `Retry-After` exactly, slows down *before* 429s), circuit breakers, chain-level health (a stalled chain auto-disables so the others keep indexing), and a `--recover` CLI that resets state from ClickHouse.
- **$5/mo to run** — ~2 GB RAM for all 16 chains concurrently. Runs on a $5 VPS.
- **DuckDB-queryable parquet archive** — ClickHouse for live data, **parquet snapshots exported to B2 on a scheduled cron** (daily in-repo script, 6-hourly on the fleet), queryable with DuckDB. No vendor lock-in, no proprietary formats.
- **Self-hosted** — your data, your server, your control. No centralized gateway, no paid API keys, no rate-limit anxiety.
- **Plugin system** — user-installed plugins (drop a `.py` file into `PLUGIN_DIR`) extend the indexer with custom event handlers without forking the codebase.
- **Production-proven** — **359M+ rows** in the live hot store indexed in production, ~3M events/hour sustained, ~30s freshness from chain tip on free RPCs.
- **Demand-aware** — lazy per-chain DEX/deployer indexing: long-tail chains only index extra event types when your scanner actually queries them (file-touch signaling, persisted in state).

> This is an **open source blockchain indexer** (MIT) — the community reference implementation. The RugMunch production instance layers proprietary analytics on top; the OSS code here is a complete, self-contained starting point.

---

## Quickstart

The fastest way to try LiteChainIndexer — install from source, then one chain, one command:

```bash
pip install "git+https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git"
export CH_HOST=localhost CH_PORT=9000 CH_DATABASE=wallet_memory
lite-indexer --chain ethereum --dex
```

This installs the package, points the ClickHouse writer at a local server
(optional — without ClickHouse, indexing still works and data is written
to the state file + cold tier), and indexes Ethereum transfer + DEX swap
events from the current head. See [Quick Start](#quick-start) below for
the full walkthrough and the [Configuration](#configuration) section for
all supported environment variables.

### Key environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `CH_HOST` (alias `CLICKHOUSE_HOST`) | `127.0.0.1` | ClickHouse host |
| `CH_PORT` (alias `CLICKHOUSE_PORT`) | `9000` | ClickHouse native port |
| `CH_USER` (alias `CLICKHOUSE_USER`) | `default` | ClickHouse user |
| `CH_DATABASE` | `wallet_memory` | ClickHouse database |
| `DATA_DIR` | `/var/data/chain-indexer` | Data directory for state + parquet |
| `NDJSON_ENABLED` | `0` | Write-ahead NDJSON log (default off) |
| `LAZY_INDEXING_ENABLED` | `1` | Lazy per-chain DEX/deployer indexing |
| `REORG_PROTECTION_ENABLED` | `1` | Reorg staging / fork rollback (EVM) |
| `RECONCILIATION_ENABLED` | `1` | Balance reconciliation cron |
| `TOKEN_META_ENABLED` | always on | Enrichment stripping (token_meta_ref) |
| `BOOTSTRAP_B2_BUCKET` | `b2-crypt:chain-data` | rclone remote root for bootstrap snapshots |
| `BOOTSTRAP_PARQUET_PATH` | `snapshots` | Snapshot path prefix inside the bucket |
| `BOOTSTRAP_LOCAL_DIR` | `{DATA_DIR}/bootstrap` | Local staging dir for snapshot downloads |
| `BOOTSTRAP_CH_FILE_ROOT` | `` | file() path root for containerized ClickHouse |
| `BOOTSTRAP_RCLONE_TIMEOUT` | `1200` | Max seconds for one rclone snapshot op |

See [Configuration](#configuration) for the complete table.

---

## Quick Start

Requirements: Python 3.11+, a little disk space, and internet access to free RPCs. **No ClickHouse required to start** — the indexer runs on free public RPCs and persists progress to its state file; ClickHouse, DuckDB, and Parquet are optional outputs.

```bash
# 1. Clone (primary mirror: Forgejo; also on Codeberg, GitLab, Radicle)
git clone https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git
cd lite-chain-indexer

# 2. Install
python -m venv .venv && source .venv/bin/activate
pip install -e .

# 3. Index one chain (single batch — great for testing)
lite-indexer --chain ethereum --from-block 25645000 --to-block 25646000

# 4. Index with DEX swap decoding
lite-indexer --chain base --dex

# 5. Daemon mode — continuously index multiple chains in parallel
lite-indexer --daemon --chains ethereum,base,bsc,polygon

# 6. Full daemon: DEX + deployer tracking, start from chain tip
lite-indexer --daemon --dex --deployers --start-from-head
```

Output lands in `DATA_DIR` (default `/var/data/chain-indexer`) as state + optional compressed NDJSON organized by chain/date, plus ClickHouse inserts if configured.

### Run as a service

```bash
sudo bash scripts/install.sh   # installs a systemd unit (lite-indexer.service)
journalctl -u lite-indexer -f  # watch logs
```

### Optional: ClickHouse + DuckDB

```bash
# ClickHouse (live analytics) — set env vars, see Configuration below
export CH_HOST=127.0.0.1 CH_PORT=9000 CH_DATABASE=wallet_memory

# DuckDB (historical parquet on B2) — optional extra
pip install -e ".[query]"
```

### Quick Start: Bootstrap from Snapshot

Don't re-index a chain from genesis over free RPCs — that takes days. Our
parquet export pipeline publishes **free public bootstrap snapshots** to B2
(`b2-crypt:chain-data/snapshots/{date}/{chain}/`), each with a `manifest.json`
describing its block range + row counts. `lite-indexer bootstrap` downloads
the snapshot and replays it into ClickHouse with zero-copy parquet imports
(`INSERT INTO ... SELECT * FROM file('*.parquet')`) — **minutes, not days**:

```bash
# Instead of waiting days for RPC backfill:
lite-indexer bootstrap --list              # what snapshots are available?
lite-indexer bootstrap --all               # every chain from its latest snapshot
lite-indexer bootstrap --chain ethereum    # one chain, latest snapshot
lite-indexer bootstrap --chain base --from-snapshot 2026-08-01  # specific date
lite-indexer daemon                        # stay live on free RPCs from the snapshot end block
```

After bootstrap, the chain's state file + monotonic high-water mark are set
to the snapshot's `end_block`, so `lite-indexer daemon` resumes live indexing
from exactly where the snapshot ends — no genesis backfill, no gap.

Publishing new snapshots is automatic: the daily export cron passes
`--publish-snapshot` (see `scripts/lite-indexer-parquet-daily.sh`), which
mirrors each exported `(chain, date)` to the snapshot location and writes
its manifest. Operators can also publish manually:

```bash
lite-indexer export-parquet --chain ethereum --date 2026-08-01 \
    --upload --publish-snapshot
```

Snapshot configuration (bucket, path prefix, download staging dir, ClickHouse
file root for containerized deployments) lives in the [Configuration](#configuration)
section under `BOOTSTRAP_*`.

---

## Chains Covered

16 chains, 61 raw RPC endpoints (51 EVM + 6 Solana + 4 Tron), all verified for the methods this indexer needs. **~45 endpoints are active by default** (37 public EVM + 4 public Solana + 4 public Tron); the other 16 are optional free-tier API-key placeholders (`${ALCHEMY_KEY}`, `${GETBLOCK_KEY}`, `${CHAINSTACK_KEY}`, `${QUICKNODE_ENDPOINT}`, `${INFURA_KEY}`, `${HELIUS_KEY}`) that activate when their env var is set — so the active count varies with configuration. Per-chain stats below are a **snapshot from the production ClickHouse store** (`wallet_memory`), captured 2026-08-03; `evm_transfers` totals **285M+** rows and `dex_events` hot storage is a **90-day TTL window** (older DEX rows live in the Parquet/B2 cold tier).

| Chain | Type | Block time | RPCs | Transfers indexed | DEX events indexed |
|-------|------|:----------:|:----:|------------------:|-------------------:|
| Polygon | EVM | 2s | 5 | 102,783,790 | 2,947,092 |
| Base | EVM | 2s | 4 | 57,894,669 | 4,903,140 |
| Ethereum | EVM | 12s | 7 | 42,451,053 | 2,088,845 |
| BSC | EVM | 3s | 5 | 27,279,812 | 1,147,162 |
| Avalanche | EVM | 2s | 3 | 17,707,142 | 437,695 |
| Optimism | EVM | 2s | 3 | 11,001,767 | 427,458 |
| Arbitrum | EVM | 0.25s | 4 | 10,718,201 | 294,638 |
| Tron | Tron | 3s | 4 | 9,034,358 | — |
| Gnosis | EVM | 5s | 2 | 2,596,248 | 472,437 |
| zkSync Era | EVM | 2s | 3 | 1,903,488 | 49,004 |
| Linea | EVM | 2s | 3 | 759,750 | 122,822 |
| Scroll | EVM | 3s | 3 | 618,665 | 127,388 |
| Mantle | EVM | 2s | 2 | 177,121 | 20,565 |
| Solana | Solana | 0.4s | 6 | 1,349,767 (SPL changes) | — |
| Fantom | EVM | 1s | 4 | *add via `--chains`* | *add via `--chains`* |
| Celo | EVM | 5s | 3 | *add via `--chains`* | *add via `--chains`* |
| **Total** | | | **61** | **345,000,000+** | **13,000,000+** |

*Totals don't include the ~0.9M deployer events, the 1.35M Solana SPL balance changes, or the internal-transfer trace rows — roughly **359M+ rows** in the live hot store. `dex_events` hot storage is a **90-day TTL window**; older DEX rows are exported to the Parquet/B2 cold tier, so the all-time cumulative DEX count is higher than the hot-store total. Fantom and Celo are fully supported in code; they're simply not in the default daemon chain list — add them with `--chains`.*

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       lite-indexer (CLI)                         │
│   --chain · --daemon · --dex · --deployers · --chains ·          │
│   reconcile · export-parquet                                     │
├──────────────────────────────────────────────────────────────────┤
│                  LiteIndexer (indexer.py)                        │
│      orchestrator: fetch → parse → enrich → persist              │
│      + demand-aware event gating (chains/demand.py)              │
├───────────────┬──────────────────┬───────────────────────────────┤
│  strategies/  │      rpc/        │         storage/              │
│  EVM (14)     │  endpoint pool   │  NDJSON writer (gated, .zst)  │
│  Solana       │  health checks   │  ClickHouse writer (live)     │
│  Tron         │  rate limiting   │  DuckDB query (parquet/B2)    │
│  bloom pre-   │  header-aware    │  token_meta_ref (stripping)   │
│  filtering    │  throttling      │  projections (address-first)  │
│  reorg stage  │  per-method      │  balance cache + MV           │
│  traces       │  routing         │  reconciliation               │
│               │  circuit breaker │  parquet cold tier + disk cap │
├───────────────┴──────────────────┴───────────────────────────────┤
│   chains/  (endpoints · limits · demand · polling · non-evm)     │
│   recovery/ (--recover · integrity-check · gap scan)             │
│            61 raw RPC endpoints · 16 chains · event topics       │
└──────────────────────────────────────────────────────────────────┘
```

### Data flow

```
Free public RPC (eth_getLogs · Solana JSON-RPC · TronGrid)
        │
        ▼
Strategy layer (EVM / Solana / Tron)  ── bloom pre-filter, chunked fetches
        │
        ▼
LiteIndexer: fetch → parse → enrich (block timestamps, token resolution)
        │   ▲
        │   └─ demand gating (chains/demand.py): lazy DEX/deployer per chain
        │
        ├───────────► NDJSON (optional) {data_dir}/{chain}/{date}/events.jsonl  (.zst)
        ├───────────► ClickHouse    wallet_memory.evm_transfers / dex_events
        │                             (live, hot, sub-second queries)
        │                             + token_meta_ref, projections, balances,
        │                               block_checkpoints (reorg), reconciliation
        ▼
Parquet export → B2  (daily 03:30 UTC script; fleet runs 6-hourly)
        │
        ▼
DuckDB queries (storage/duckdb_query.py)  — no node, no server, no bill
```

**Hot vs cold split:** ClickHouse serves live data with millisecond latency; DuckDB reads the static parquet snapshots from B2 for historical analytics. One is not a replacement for the other.

---

## Performance

Honest numbers from the production deployment (2-core VPS, free public RPCs):

| Metric | Value |
|--------|-------|
| Events indexed | **359M+ rows** in the live hot store — 345M+ transfer events + 13M+ DEX events (90-day hot window; older rows in Parquet/B2 cold tier) + ~0.9M deployer events + 1.35M Solana SPL changes + internal-transfer traces |
| Memory | **~2 GB** for all 16 chains in a single process |
| Throughput | **~3M+ events/hour** sustained (≈77M rows/day to ClickHouse) |
| Freshness | **~30s** from chain tip on free RPCs (adaptive per-chain polling, chunked fetches) |
| RPC cost | **$0** — free public endpoints only (61 curated; ~45 active by default, optional free-tier API keys), no paid API keys required |
| Disk | Configurable hard cap (default 10 GB); Parquet cold tier moves old rows to B2 |
| Infrastructure | **~$5/mo** — a single 2-core VPS runs all 16 chains |

Typical sustained catch-up (backfill) rates per chain on a 2-core VPS using free public RPCs — *actual rates vary with free-tier rate limits and network conditions*:

| Chain | Blocks/s (typical) | Chain | Blocks/s (typical) |
|-------|:------------------:|-------|:------------------:|
| Ethereum | 2–5 | Scroll | 3–10 |
| Base | 8–20 | zkSync Era | 3–10 |
| BSC | 8–20 | Mantle | 5–15 |
| Polygon | 8–20 | Gnosis | 2–8 |
| Arbitrum | 40–100 | Celo | 2–8 |
| Optimism | 5–15 | Solana | 5–15 (slots/s) |
| Avalanche | 3–10 | Tron | 3–10 |
| Fantom | 3–10 | Linea | 3–10 |

---

## Comparison

| Feature | **LiteChainIndexer** | Ponder | Subsquid | HyperIndex | The Graph |
|---------|:---:|:---:|:---:|:---:|:---:|
| Cost | **$0 + $5 VPS** | $0 + infra | $0 + infra | $0 + infra | $0 + infra/GRT |
| Chains | **16** (14 EVM + Solana + Tron) | Configurable (EVM) | Configurable (EVM) | Configurable (EVM) | Hosted (EVM) |
| Rows indexed in prod | **359M+** | 10M–100M typical | 100M+ | 10M–100M typical | billions (shared) |
| Free RPCs built in | ✅ **61 curated** | ❌ DIY | ❌ DIY | ❌ DIY | N/A |
| Setup time | **5 min** | ~30 min | ~1 hr | ~30 min | Days |
| Address-first wallet queries | ✅ **Projections** | ❌ | ❌ | ❌ | ❌ |
| Self-healing state | ✅ **Guard + --recover** | ❌ | Partial | Partial | ❌ |
| RPC throttling | ✅ **Header-aware** | ❌ | Partial | ❌ | N/A |
| Internal-transfer traces | ✅ | ❌ | ❌ | ❌ | ❌ |
| Deployer tracking | ✅ | ❌ | ❌ | ❌ | ❌ |
| DEX events | ✅ V2/V3 + forks | Configurable | Configurable | Configurable | ❌ |
| DuckDB queries | ✅ | ❌ | ❌ | ❌ | ❌ |
| Parquet export → B2 | ✅ | ❌ | Partial | ❌ | ❌ |
| Plugin system | ✅ | ✅ | ❌ | Partial | ❌ |
| Historical backfill | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python-native | ✅ | ❌ TS | ❌ TS | ❌ TS | ❌ GraphQL/AS |

**Why LiteChainIndexer wins for self-hosted free-tier indexing:**

- **Free from the ground up** — Ponder/Subsquid/HyperIndex assume you bring your own RPC (often $100s/mo for archive nodes); LiteChainIndexer ships with 61 curated free endpoints, live-verified, and throttles them intelligently (per-endpoint rate-limit header tracking, exact `Retry-After` honoring, pre-emptive slow-down before 429s, per-method budgets).
- **359M rows on a $5 VPS** — one Python process, ~2 GB RAM, address-first ClickHouse projections make wallet queries sub-second even at this scale.
- **Self-healing is the default** — the backward-state guard auto-recovers clobbered state from the ClickHouse max block; `--recover` fixes state files from the durable store; a stalled chain auto-disables while the others keep indexing.
- **Biggest blind spot covered** — internal-transfer traces (WETH unwraps, bridge deposits, fee flows) that ERC-20 `Transfer`-only indexers (and therefore the alternatives above) never see.

See [docs/COMPARISON.md](docs/COMPARISON.md) for the detailed breakdown, caveats, and a pricing walkthrough.

---

## Features

### 🪶 Real-time indexing
~30s freshness from chain tip. Adaptive polling intervals per chain (respects native block time — Base 2s, Arbitrum 0.25s, etc.). Daemon mode indexes all chains in parallel with `asyncio`, one independent task per chain so fast chains are never throttled by slow ones. Chunked fetches (50 EVM blocks / 500 Solana slots per `eth_getLogs`) cut RPC round trips and avoid "response too large" errors from restrictive free RPCs.

### 💱 DEX swap decoding
Automatically decodes Uniswap V2/V3 style swap events with token0/token1 resolution. Supports PancakeSwap (BSC), QuickSwap (Polygon), and all standard V2/V3 forks. Builds a persistent pair→tokens cache that survives restarts, with `eth_call` fallback resolution.

### 🏭 Deployer tracking
Tracks every PairCreated/PoolCreated event from known DEX factories. Records factory address, deployer EOA, token0/token1, and pair address (in a dedicated `pair_address` column) — perfect for building deployer reputation scores.

### 🌐 Three chain types
- **EVM (14)** — ERC-20 transfers, DEX swaps, pair/pool creation via `eth_getLogs`
- **Solana** — SPL token balance changes via `getSignaturesForAddress` + `getTransaction`. Each row captures the token account address plus its on-chain `owner` (the wallet that holds the token) in the `owner` column — so scanners can answer *who* holds each token, not just *what* changed.
- **Tron** — TRC-20 transfers via the TronGrid event API

### 🎯 Demand-aware indexing
Lazy per-chain event gating: every chain starts with transfer events only. Top chains (ethereum, bsc, polygon, arbitrum, base, solana) always index DEX + deployer events; long-tail chains (scroll, mantle, linea, fantom, avalanche, gnosis) enable them only after the scanner signals demand via a file touch (`{INDEXER_DEMAND_DIR}/{chain}/dex`). Enabled event types persist in the state file. Set `LAZY_INDEXING_ENABLED=0` for the legacy all-events behavior.

### 🧹 Enrichment stripping
Transfer rows carry only the 10 hot-path columns — `token_symbol` / `token_name` (~60 bytes × 285M rows) moved out of `evm_transfers` into a single `token_meta_ref` reference table. Queries `JOIN` the reference for metadata; the hot table stays lean.

### 🛡️ Reorg staging + reconciliation
- **Reorg staging** — the last N blocks per chain are held "unripe" in `wallet_memory.block_checkpoints`; on hash mismatch the indexer DELETEs `evm_transfers` + `dex_events` above the fork point and re-fetches from the fork (`REORG_PROTECTION_ENABLED`, EVM default on).
- **Balance reconciliation** — TrueBlocks-style completeness check: computed balances from `evm_transfers` deltas vs on-chain `eth_getBalance`, cached in `wallet_memory.reconciliation_results` (`reconcile` subcommand, default 100 sampled wallets/chain).

### 📊 Three data outputs
- **NDJSON** — optional write-ahead log (`NDJSON_ENABLED=1`), append-only, auto-compressed (`.zst`), organized by chain/date
- **ClickHouse** — native-protocol inserts for live analytical queries, queryable immediately
- **Parquet → B2 → DuckDB** — cold archive snapshots on a cron (daily script in-repo, 6-hourly on the fleet), queryable with plain SQL via the included `duckdb_query` module (`transfers`, `dex`, `spl_balance_changes` views)

### 🔒 Production hardening
- **Disk cap enforcement** — configurable hard limit on data directory (default 10 GB)
- **Self-healing RPC pool** — 61 endpoints with automatic health verification, tiered rotation, and exponential backoff
- **Intelligent RPC throttling** — per-endpoint rate-limit header tracking (`X-RateLimit-Remaining` / `Retry-After`), exact 429 backoff honoring, pre-emptive slow-down once >80% of an advertised budget is consumed, and per-method budgets (`eth_getLogs` paced at 3 req/s while `eth_blockNumber` runs at 20 req/s). All state persists to disk so a restart mid-429-window does not re-hammer the endpoint.
- **Per-method RPC routing** — heavy `eth_getLogs` goes to free public tiers; latency-sensitive calls (receipts, `eth_call`) prefer dedicated nodes
- **Circuit breakers** — per-endpoint open/half-open/close protection with success-based recovery
- **Per-provider token buckets** — fine-grained rate limiting that never trips free-tier caps
- **Backward-state guard** — a monotonic per-chain high-water mark + ClickHouse max query auto-recovers a clobbered state file (`last_block` reset to ~93K while CH held 25.6M → resumes at 25.6M instead of re-indexing from genesis)
- **`--recover` CLI** — reset state files from the ClickHouse durable store and backfill detected missing ranges
- **Chain-level health** — a chain whose state has not advanced for 2 hours auto-disables (and re-probes slowly) while the other chains keep indexing
- **Daily integrity check** — `lite-indexer integrity-check` audits per-chain ClickHouse row counts vs expected ranges (min/max/distinct blocks, contiguity %)
- **Provider-aware block ranges** — respects endpoint-specific `eth_getLogs` range limits, adjusts batch sizes automatically
- **Bloom pre-filtering** — address-indexed per-block logs-bloom cache skips blocks that can't contain the queried address (geth-conformant byte order)
- **Chain reorg protection** — staged block confirmation with fork rollback (deletes + re-fetch) instead of silently ingesting bad data
- **Column TTL** — heavy detail columns degrade to defaults after `RETENTION_DAYS` while summary columns stay queryable

### 🧪 Thread-safe state management
File-locked state files allow safe concurrent runs. Pair cache and per-chain progress persist across restarts.

### 🤖 AI-vector-ready chain indexing (LanceDB)

Chain data is vectorized into an embedded LanceDB store for semantic
queries — RAG, fraud detection, wallet similarity, contract clone
detection. LanceDB is embedded (no server process), columnar, disk-based,
and zero-cost; it complements ClickHouse (analytics) with ANN + full-text
search. Requires the `vectors` extra: `pip install -e ".[vectors]"`.

Pre-built collections:

| Collection | Contents | Use case |
|-----------|----------|----------|
| `wallet_embeddings` | Wallet behavior patterns | Semantic wallet search, similar-wallet discovery |
| `contract_embeddings` | Deployed contract/bytecode signatures | Clone & fork detection, rug screening |
| `transaction_embeddings` | Transaction patterns | Fraud / anomaly detection |
| `token_embeddings` | Token metadata + holder patterns | Token similarity, scam screening |

Query API endpoints (when `VECTOR_SEARCH_ENABLED=1`):

```
GET /api/v1/search/wallets?q=rug+pull+pattern
GET /api/v1/search/contracts?q=uniswap+v3+clone
GET /api/v1/similar/wallets/{address}
GET /api/v1/similar/contracts/{contract_address}
```

Demo: `python scripts/vectorize_demo.py 0x1234...` finds the top-10
wallets with behavior most similar to an address.

---

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/var/data/chain-indexer` | Data directory for state + parquet |
| `CH_HOST` (alias `CLICKHOUSE_HOST`) | `127.0.0.1` | ClickHouse host |
| `CH_PORT` (alias `CLICKHOUSE_PORT`) | `9000` | ClickHouse native port |
| `CH_USER` (alias `CLICKHOUSE_USER`) | `default` | ClickHouse user |
| `CH_PASSWORD` | `""` | ClickHouse password |
| `CH_DATABASE` | `wallet_memory` | ClickHouse database |
| `LOG_LEVEL` | `INFO` | Logging level |
| `RPC_TIMEOUT` | `30` | RPC call timeout (seconds) |
| `DISK_CAP_GB` | `10` | Max data directory size (GB) |
| `NDJSON_ENABLED` | `0` | Write-ahead NDJSON log (default off; redundant with ClickHouse) |
| `LAZY_INDEXING_ENABLED` | `1` | Lazy per-chain DEX/deployer indexing |
| `INDEXER_DEMAND_DIR` | `/tmp/indexer-demand` | Scanner demand signal files for lazy indexing |
| `TOKEN_META_ENABLED` | always on | Enrichment stripping (token_meta_ref table) |
| `WALLET_PROJECTION_ENABLED` | `1` | Address-first projections on evm_transfers |
| `BALANCE_CACHE_ENABLED` | `1` | Per-token incremental holder balance cache |
| `HOLDER_BALANCES_MV_ENABLED` | `1` | SummingMergeTree materialized holder balances |
| `REORG_PROTECTION_ENABLED` | `1` | Reorg staging + fork rollback (EVM) |
| `RECONCILIATION_ENABLED` | `1` | Balance reconciliation vs on-chain |
| `RECONCILIATION_SAMPLE_SIZE` | `100` | Sampled wallets per chain per reconcile run |
| `BLOOM_ENABLED` | `1` | logsBloom pre-filtering (geth byte order) |
| `REDIS_URL` | `""` | Optional Redis backing for the bloom cache |
| `CH_TTL_ENABLED` | `1` | Column-level ClickHouse TTL / retention |
| `PARQUET_COLD_DAYS` | `7` | Min age (days) of rows eligible for Parquet cold tier |
| `PARQUET_REMOTE` | `b2-crypt:lite-indexer-parquet` | rclone remote for Parquet uploads |
| `LANCE_DIR` | `/var/data/lance` | LanceDB vector database directory (embedded, no server) |
| `EMBEDDING_PROVIDER` | `infinity` | Embedding provider: `infinity` (local/free, default) or `openai` (compatible) |
| `EMBEDDING_MODEL` | `BAAI/bge-m3` | Embedding model name |
| `EMBEDDING_BASE_URL` | `http://localhost:8001` | Infinity / OpenAI-compatible base URL |
| `EMBEDDING_API_KEY` | `""` | API key for OpenAI-compatible providers |
| `EMBEDDING_BATCH_SIZE` | `32` | Texts per embedding request |
| `EMBEDDING_DIM` | `1024` | Embedding vector dimension (must match model) |
| `VECTOR_SEARCH_ENABLED` | `1` | Enable `/api/v1/search` + `/api/v1/similar` endpoints |
| `RPC_THROTTLE_FILE` | `<DATA_DIR>/rpc_throttle.json` | Persisted per-endpoint rate-limit tracker state |
| `RPC_PREEMPTIVE_THRESHOLD_PCT` | `80` | Slow down before 429 once >80% of an advertised budget is consumed |
| `RPC_METHOD_DEFAULT_RATE` | `10.0` | Default req/s budget for methods without an explicit entry |
| `RPC_THROTTLE_MAX_WAIT` | `30` | Cap (s) on pre-emptive/adaptive sleep (Retry-After is honored exactly) |
| `CHAIN_AUTO_DISABLE_ENABLED` | `1` | Auto-disable a chain stalled for `CHAIN_STALL_DISABLE_SECONDS` |
| `CHAIN_STALL_DISABLE_SECONDS` | `7200` | Stall window (s) before a chain is auto-disabled (2h) |
| `CHAIN_DISABLED_RECHECK_SECONDS` | `900` | Re-probe interval (s) for a disabled chain |

### RPC endpoints

Endpoints live in `lite_indexer/chains/` (`endpoints.py`, `non_evm.py`). Each EVM chain has tiers, all live-verified against a topic-only `eth_getLogs` probe:

- **Tier 1** — fast, reliable (drpc.org, tenderly, official RPCs)
- **Tier 2** — fallbacks (tatum, blxrbdn, nodereal, 48.club)
- **Tier 3** — last resort (range-limited publics)
- **Tier 4** — free API-key tiers (`${ALCHEMY_KEY}`, `${GETBLOCK_KEY}`, Chainstack, QuickNode, Infura) — placeholders are resolved from the environment at import time

The daemon also runs a periodic endpoint health monitor that logs up/down
status and feeds the adaptive pool scoring, so dead endpoints are
deprioritized between sync cycles.

---

## CLI Reference

```
usage: lite-indexer [-h] [--chain CHAIN] [--from-block FROM_BLOCK]
                    [--to-block TO_BLOCK] [--batch-size BATCH_SIZE]
                    [--disk-cap DISK_CAP] [--daemon] [--chains CHAINS]
                    [--dex] [--deployers] [--start-from-head] [--recover]
                    [--backfill] [--data-dir DATA_DIR]
                    [--log-level LOG_LEVEL]
                    {integrity-check,reconcile,export-parquet,backfill-whales,bootstrap,query}

LiteChainIndexer — lightweight multi-chain blockchain indexer using free RPCs

options:
  --chain CHAIN         Chain to index (default: ethereum)
  --from-block N        Override starting block (testing / backfill)
  --to-block N          Override ending block (testing / backfill)
  --batch-size N        Max blocks per batch (default: 1000)
  --disk-cap N          Disk cap in bytes (default: 10 GB)
  --daemon              Run in daemon mode — continuously index all chains
  --chains LIST         Comma-separated chains for daemon mode
  --dex                 Enable DEX swap event indexing (V2 + V3)
  --deployers           Enable deployer tracking via PairCreated events
  --start-from-head     Start from chain tip instead of backfilling
  --recover             Reset state files from the ClickHouse max block
                        (fixes state-file regressions; with --backfill also
                        rewinds to missing ranges for re-fetch)
  --backfill            With --recover: rewind state to the earliest
                        missing range so the next sync backfills it
  --data-dir PATH       Data directory for NDJSON + state
  --log-level LEVEL     Log level (DEBUG, INFO, WARNING, ERROR)

subcommands:
  integrity-check       Daily audit: per-chain ClickHouse row counts vs
                        expected ranges (min/max/distinct blocks, gaps)
  reconcile             TrueBlocks-style balance reconciliation vs on-chain
  export-parquet        Export cold rows to Parquet + upload to B2
  backfill-whales       Re-mark recent DEX rows with whale flags
  bootstrap             Replay a public B2 parquet snapshot into ClickHouse
  query                 On-demand historical RPC query — answer ANY
                        block/tx/address without full indexing
```

### Query examples (on-demand, no full indexing)

The `query` subcommand answers queries about any point in history by
routing to the best data source: ClickHouse (indexed, hot) → B2 parquet
(cold) → live RPC (on-demand). The system never says "data not
available" — the RPC leg is the last resort (bounded, cached, rate-limit
protected).

```bash
# A single transfer by tx hash (CH → cold → RPC)
lite-indexer query --chain ethereum --tx 0xabc...

# A wallet's transfer history (indexed rows, or the recent RPC window)
lite-indexer query --chain base --address 0xwallet... --limit 50

# A specific block range for an address (bounded to the RPC window)
lite-indexer query --chain ethereum --address 0xwallet... --from-block 21000000 --to-block 21001000

# A block with full transactions (always live RPC)
lite-indexer query --chain ethereum --block 21000000

# Who deployed a contract (indexed deployer_events, or on-chain lookup)
lite-indexer query --chain ethereum --deployer 0xpair...

# Indexed data only — no RPC fallback
lite-indexer query --chain ethereum --tx 0xabc... --no-rpc
```

The API's `/api/v1/transfers/{address}` and `/api/v1/deployers/{address}`
endpoints use the same `QueryRouter`: when the indexed stores return
nothing for a specific chain, they fall through to on-demand RPC. Set
`ON_DEMAND_RPC_ENABLED=0` to disable the fallback.

### Bootstrap examples

```bash
# List every available snapshot (chain, date, block range, size)
lite-indexer bootstrap --list

# One chain from its latest snapshot (minutes, not days of RPC backfill)
lite-indexer bootstrap --chain ethereum

# Every supported chain from the latest snapshot
lite-indexer bootstrap --all

# A specific snapshot date
lite-indexer bootstrap --chain base --from-snapshot 2026-08-01

# Preview the import plan without downloading or writing
lite-indexer bootstrap --chain ethereum --dry-run

# Publish today's export as a bootstrap snapshot (also in the daily cron)
lite-indexer export-parquet --chain ethereum --date 2026-08-01 --upload --publish-snapshot
```

### Recovery & integrity examples

```bash
# Fix a clobbered state file: reset to the ClickHouse max block
lite-indexer --chain ethereum --recover

# Same for every chain, and rewind to missing ranges so they re-fetch
lite-indexer --daemon --chains ethereum,base,bsc --recover --backfill

# Daily integrity audit (run from cron)
lite-indexer integrity-check --chains ethereum,base,bsc,solana,tron
```

---

## FAQ

Common questions — cost, supported chains, comparisons, historical data, adding chains, hardware, contributing — are answered in [docs/FAQ.md](docs/FAQ.md).

---

## Development

```bash
git clone https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git
cd lite-chain-indexer
pip install -e ".[dev]"
ruff check .
pytest -v
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full workflow.

---

## License & Contributing

MIT — free for any use, commercial or personal. See [LICENSE](LICENSE). Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

### Mirrors

| Remote | URL |
|--------|-----|
| Forgejo (primary) | https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer |
| Codeberg | https://codeberg.org/cryptorugmunch/lite-chain-indexer |
| GitLab | https://gitlab.com/cryptorugmuncher/lite-chain-indexer |
| Radicle | `rad://z3ca3ygazjcDWFXQJKrFmDBvprM6k` |

### Related projects

- [RugMunch RMI Backend](https://git.rugmunch.io/RugMunchMedia/rmi-backend) — crypto intelligence platform that uses this indexer
- [chain-indexer](https://git.rugmunch.io/RugMunchMedia/chain-indexer) — the full-featured TypeScript indexer (parquet, ClickHouse, deployer scoring)

---

*SEO: free blockchain indexer · EVM indexer · multi-chain indexer · blockchain data without node · open source blockchain indexer · Solana indexer · Tron indexer · DuckDB blockchain · parquet blockchain export*
