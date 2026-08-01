# Lite Chain Indexer 🪶

**The free, open-source, multi-chain blockchain indexer.** Index transfers, DEX swaps, and deployer activity across **16 chains (14 EVM + Solana + Tron)** using **only free public RPCs** — zero infrastructure cost, zero API keys required, and DuckDB-queryable parquet archives on B2. No node. No gateway. No bill.

[![CI](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml/badge.svg?branch=main)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-v1.1.0-blue.svg)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/tags)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![16 chains](https://img.shields.io/badge/chains-16-success.svg)](#chains-covered)

---

## What It Does

Lite Chain Indexer is a lightweight, self-hosted **multi-chain blockchain indexer** that turns raw on-chain activity into queryable, analyzable data. It watches **ERC-20/TRC-20/SPL transfer events**, **DEX swap events** (Uniswap V2/V3 and forks), and **pair/pool deployment events** across **14 EVM chains plus Solana and Tron** — entirely through **free public RPC endpoints**. Events are written to local NDJSON files and ClickHouse in real time, then exported as **parquet snapshots to B2** every six hours so you can run **DuckDB SQL queries over historical data** with no node, no gateway, and no recurring bill.

It's the **free blockchain indexer** for anyone who wants **blockchain data without running a node** — analysts, wallets, dashboards, Telegram bots, and research pipelines.

---

## Why It's Different

- **$0/month forever** — runs entirely on free public RPC endpoints (123 verified endpoints across 16 chains). No paid APIs, no gateway fees, no infrastructure bill.
- **16 chains, one process** — Ethereum, Base, BSC, Polygon, Arbitrum, Optimism, Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo **+ Solana + Tron**. One Python process, `asyncio`-parallel daemon.
- **DuckDB-queryable parquet archive** — ClickHouse for live data, **parquet snapshots exported to B2 every 6 hours**, queryable with DuckDB. No vendor lock-in, no proprietary formats.
- **Self-hosted** — your data, your server, your control. No centralized gateway, no API keys, no rate-limit anxiety.
- **Production-proven** — **231M+ transfer events** and **58M+ DEX events** indexed in production, ~3M events/hour sustained, sub-2-minute latency from chain tip.
- **Honest footprint** — ~2 GB RAM for all 16 chains concurrently. Runs on a $5 VPS.

> This is an **open source blockchain indexer** (MIT) — the community reference implementation. The RugMunch production instance layers proprietary analytics on top; the OSS code here is a complete, self-contained starting point.

---

## Quick Start

Requirements: Python 3.11+, a little disk space, and internet access to free RPCs. **No ClickHouse required to start** — events are always written to NDJSON; ClickHouse and DuckDB are optional outputs.

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

Output lands in `DATA_DIR` (default `/var/lib/chain-indexer`) as compressed NDJSON organized by chain/date, plus ClickHouse inserts if configured.

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

---

## Chains Covered

16 chains, 123 free RPC endpoints (113 EVM + 6 Solana + 4 Tron), all verified for the methods this indexer needs. Stats below are **live from the production ClickHouse store** (`wallet_memory`), captured 2026-08-01.

| Chain | Type | Block time | RPCs | Transfers indexed | DEX events indexed |
|-------|------|:----------:|:----:|------------------:|-------------------:|
| Polygon | EVM | 2s | 9 | 77,700,942 | 12,772,044 |
| Base | EVM | 2s | 4 | 49,920,147 | 20,787,390 |
| Ethereum | EVM | 12s | 16 | 39,702,256 | 6,250,437 |
| BSC | EVM | 3s | 11 | 24,467,187 | 8,148,525 |
| Avalanche | EVM | 2s | 7 | 12,102,565 | 306,176 |
| Arbitrum | EVM | 0.25s | 10 | 9,186,233 | 8,801,642 |
| Optimism | EVM | 2s | 10 | 6,951,275 | 304,540 |
| Tron | Tron | 3s | 4 | 6,311,669 | — |
| Gnosis | EVM | 5s | 7 | 2,242,706 | 393,227 |
| zkSync Era | EVM | 2s | 6 | 1,821,364 | 51,816 |
| Linea | EVM | 2s | 6 | 735,155 | 169,721 |
| Scroll | EVM | 3s | 7 | 599,266 | 133,230 |
| Mantle | EVM | 2s | 7 | 164,369 | 25,554 |
| Solana | Solana | 0.4s | 6 | 841,478 (SPL changes) | — |
| Fantom | EVM | 1s | 7 | *add via `--chains`* | *add via `--chains`* |
| Celo | EVM | 5s | 6 | *add via `--chains`* | *add via `--chains`* |
| **Total** | | | **123** | **231,902,240** | **58,143,706** |

*Totals don't include the ~0.9M deployer events and ~0.8M Solana SPL balance changes — roughly **291M+ events** indexed in total. Fantom and Celo are fully supported in code; they're simply not in the default daemon chain list — add them with `--chains`.*

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                       lite-indexer (CLI)                         │
│       --chain · --daemon · --dex · --deployers · --chains        │
├──────────────────────────────────────────────────────────────────┤
│                  LiteIndexer (indexer.py)                        │
│          orchestrator: fetch → parse → enrich → persist          │
├───────────────┬──────────────────┬───────────────────────────────┤
│  strategies/  │      rpc/        │         storage/              │
│  EVM (14)     │  endpoint pool   │  NDJSON writer (.zst)         │
│  Solana       │  health checks   │  ClickHouse writer (live)     │
│  Tron         │  rate limiting   │  DuckDB query (parquet/B2)    │
│               │  block ranges    │  disk cap + rotation          │
├───────────────┴──────────────────┴───────────────────────────────┤
│   chains/  (endpoints · limits · polling · non-evm) + config.py  │
│            123 free RPC endpoints · 16 chains · event topics     │
└──────────────────────────────────────────────────────────────────┘
```

### Data flow

```
Free public RPC (eth_getLogs · Solana JSON-RPC · TronGrid)
        │
        ▼
Strategy layer (EVM / Solana / Tron)
        │
        ▼
LiteIndexer: fetch → parse → enrich (block timestamps, token resolution)
        │
        ├───────────► NDJSON        {data_dir}/{chain}/{date}/events.jsonl  (local, zstd)
        ├───────────► ClickHouse    wallet_memory.evm_transfers / dex_events
        │                             (live, hot, sub-second queries)
        ▼
Parquet export → B2  (every 6 hours, cron: lite-indexer-parquet-daily.sh)
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
| Memory | **~2 GB** for all 16 chains in a single process |
| Throughput | **~3M+ events/hour** sustained (≈77M rows/day to ClickHouse) |
| Latency | **< 2 minutes** from chain tip (adaptive per-chain polling) |
| Disk | NDJSON auto-compressed to `.zst` after 15 min; configurable hard cap (default 10 GB) |

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

| Feature | **Lite Chain Indexer** | Ponder | Subsquid | Helius | The Graph |
|---------|:---:|:---:|:---:|:---:|:---:|
| Cost | **$0** | $0 + infra | $0 + infra | $49+/mo | $0 + infra/GRT |
| Chains | **16** (14 EVM + Solana + Tron) | Configurable (EVM) | Configurable | Solana only | Hosted (EVM) |
| Setup time | **5 min** | ~30 min | ~1 hr | Instant (API) | Days |
| Free RPCs | ✅ **Built-in** | ❌ DIY | ❌ DIY | N/A | N/A |
| DuckDB queries | ✅ | ❌ | ❌ | ❌ | ❌ |
| Parquet export | ✅ | ❌ | Partial | ❌ | ❌ |
| Deployer tracking | ✅ | ❌ | ❌ | ❌ | ❌ |
| DEX events | ✅ | Configurable | Configurable | Limited | ❌ |
| Historical backfill | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python-native | ✅ | ❌ TS | ❌ TS | ❌ | ❌ AS |

See [docs/COMPARISON.md](docs/COMPARISON.md) for the detailed breakdown, caveats, and a pricing walkthrough.

---

## Features

### 🪶 Real-time indexing
Sub-2-minute latency from chain tip. Adaptive polling intervals per chain (respects native block time — Base 2s, Arbitrum 0.25s, etc.). Daemon mode indexes all chains in parallel with `asyncio`, one independent task per chain so fast chains are never throttled by slow ones.

### 💱 DEX swap decoding
Automatically decodes Uniswap V2/V3 style swap events with token0/token1 resolution. Supports PancakeSwap (BSC), QuickSwap (Polygon), and all standard V2/V3 forks. Builds a persistent pair→tokens cache that survives restarts, with `eth_call` fallback resolution.

### 🏭 Deployer tracking
Tracks every PairCreated/PoolCreated event from known DEX factories. Records factory address, deployer, and pair address — perfect for building deployer reputation scores.

### 🌐 Three chain types
- **EVM (14)** — ERC-20 transfers, DEX swaps, pair/pool creation via `eth_getLogs`
- **Solana** — SPL token balance changes via `getSignaturesForAddress` + `getTransaction`
- **Tron** — TRC-20 transfers via the TronGrid event API

### 📊 Three data outputs
- **NDJSON** — portable, append-only, auto-compressed (`.zst`) event logs organized by chain/date
- **ClickHouse** — native-protocol inserts for live analytical queries, queryable immediately
- **Parquet → B2 → DuckDB** — cold archive snapshots every 6 hours, queryable with plain SQL via the included `duckdb_query` module (`transfers`, `dex`, `spl_balance_changes` views)

### 🔒 Production hardening
- **Disk cap enforcement** — configurable hard limit on data directory (default 10 GB)
- **Self-healing RPC pool** — 123 endpoints with automatic health verification, tiered rotation, and exponential backoff
- **Per-provider token buckets** — fine-grained rate limiting that never trips free-tier caps
- **Provider-aware block ranges** — respects endpoint-specific `eth_getLogs` range limits, adjusts batch sizes automatically
- **Chain reorg detection** — detects and flags reorganizations instead of silently ingesting bad data

### 🧪 Thread-safe state management
File-locked state files allow safe concurrent runs. Pair cache and per-chain progress persist across restarts.

---

## Configuration

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/var/lib/chain-indexer` | Data directory for NDJSON + state |
| `CH_HOST` | `127.0.0.1` | ClickHouse host |
| `CH_PORT` | `9000` | ClickHouse native port |
| `CH_USER` | `default` | ClickHouse user |
| `CH_PASSWORD` | `""` | ClickHouse password |
| `CH_DATABASE` | `wallet_memory` | ClickHouse database |
| `LOG_LEVEL` | `INFO` | Logging level |
| `RPC_TIMEOUT` | `30` | RPC call timeout (seconds) |
| `DISK_CAP_GB` | `10` | Max data directory size (GB) |

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
                    [--dex] [--deployers] [--start-from-head]
                    [--data-dir DATA_DIR] [--log-level LOG_LEVEL]

Lite Chain Indexer — lightweight multi-chain blockchain indexer using free RPCs

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
  --data-dir PATH       Data directory for NDJSON + state
  --log-level LEVEL     Log level (DEBUG, INFO, WARNING, ERROR)
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
