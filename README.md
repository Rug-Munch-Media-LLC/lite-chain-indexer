# LiteChainIndexer 🪶

**The free, open-source multi-chain blockchain indexer.**

Index ERC-20 transfers, DEX swaps, and pair creation events across 14 EVM chains using **only free public RPCs**. No paid APIs. No infrastructure costs. Just data.

[![CI](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml/badge.svg)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

---

## Why LiteChainIndexer?

- **$0/month** — runs entirely on free public RPC endpoints. 111 endpoints across 14 chains, all verified for `eth_getLogs`.
- **Lightweight** — single Python process, <200MB RAM for 12+ chains. No Docker required (but works great in Docker).
- **Multi-chain** — Ethereum, Base, BSC, Polygon, Arbitrum, Optimism, Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo.
- **Self-hosted** — your data, your server, your control. No centralized gateways, no API keys, no rate limit anxiety.
- **Production-proven** — indexes 3M+ events/hour, 79M+ events stored in production. Self-healing, crash-resistant, disk-safe.

### Better than the alternatives

| Feature | LiteChainIndexer | Subsquid | The Graph | Ponder | Alchemy |
|---------|:---:|:---:|:---:|:---:|:---:|
| Free | ✅ | ⚠️ limited | ⚠️ limited | ✅ | ❌ |
| Self-hosted | ✅ | ❌ gateway | ❌ hosted | ✅ | ❌ |
| Multi-chain (14) | ✅ | ✅ | ✅ | ⚠️ EVM only | ✅ |
| No gateway dependency | ✅ | ❌ | ❌ | ✅ | ❌ |
| Python-native | ✅ | ❌ TS | ❌ AS | ❌ TS | ❌ |
| DEX swap decoding | ✅ | ✅ | ❌ | ❌ | ❌ |
| ClickHouse output | ✅ | ❌ | ❌ | ❌ | ✅ ($$$) |
| Disk cap enforcement | ✅ | ❌ | ❌ | ❌ | ❌ |
| 111 free RPC endpoints | ✅ | ❌ | ❌ | ❌ | ❌ |

---

## Quick Start

```bash
# Install
pip install lite-chain-indexer

# Index one chain (single batch)
lite-indexer --chain ethereum

# Index with DEX swap decoding
lite-indexer --chain base --dex

# Daemon mode — continuously index all chains
lite-indexer --daemon --chains ethereum,base,bsc,polygon

# Full daemon with DEX + deployer tracking
lite-indexer --daemon --dex --deployers --start-from-head

# Run as module
python -m lite_indexer --chain ethereum --from-block 25645000 --to-block 25646000
```

### With Docker

```bash
docker pull rugmunch/lite-chain-indexer:latest
docker run -v /srv/data:/srv/data rugmunch/lite-chain-indexer --daemon
```

---

## Supported Chains

| Chain | Type | Block Time | RPC Endpoints | Free Tier |
|-------|------|:----------:|:-------------:|:---------:|
| Ethereum | EVM | 12s | 12 | ✅ |
| Base | EVM | 2s | 10 | ✅ |
| BSC | EVM | 3s | 10 | ✅ |
| Polygon | EVM | 2s | 8 | ✅ |
| Arbitrum | EVM | 0.25s | 9 | ✅ |
| Optimism | EVM | 2s | 9 | ✅ |
| Avalanche | EVM | 2s | 7 | ✅ |
| Fantom | EVM | 1s | 7 | ✅ |
| Linea | EVM | 12s | 6 | ✅ |
| Scroll | EVM | 3s | 7 | ✅ |
| zkSync Era | EVM | 1s | 6 | ✅ |
| Mantle | EVM | 2s | 7 | ✅ |
| Gnosis | EVM | 5s | 7 | ✅ |
| Celo | EVM | 5s | 6 | ✅ |
| **Total** | | | **111** | **All free** |

---

## Features

### 🔄 Real-time Indexing
Sub-2-minute latency from chain tip. Adaptive polling intervals per chain (respects native block time). Daemon mode indexes all chains in parallel using `asyncio`.

### 💱 DEX Swap Decoding
Automatically decodes Uniswap V2/V3 style swap events with token0/token1 resolution. Supports PancakeSwap (BSC), QuickSwap (Polygon), and all standard V2/V3 forks. Builds a persistent pair→tokens cache that survives restarts.

### 🏭 Deployer Tracking
Tracks every PairCreated/PoolCreated event from known DEX factories. Records factory address, deployer, and pair address. Perfect for building deployer reputation scores.

### 📊 Dual Output
- **NDJSON (JSONL)** — portable, append-only event logs organized by chain/date. Ready for streaming into your own pipeline.
- **ClickHouse** — native protocol inserts for high-performance analytical queries. Inserted in real-time, queryable immediately.

### 🔒 Production Hardening
- **Disk cap enforcement** — configurable hard limit on data directory size (default: 10 GB). Never run out of space.
- **Self-healing RPC pool** — 111 endpoints across 14 chains with automatic health verification, tiered rotation, and exponential backoff.
- **Per-provider token bucket** — fine-grained rate limiting prevents hitting free-tier caps.
- **Provider-aware block ranges** — respects endpoint-specific `eth_getLogs` range limits. Automatically adjusts batch sizes.
- **Endpoint verification** — tests all endpoints at startup and only uses healthy ones. Range-aware fallback for archive-limited RPCs.

### 🧪 Thread-Safe State Management
File-locked state files allow safe concurrent runs. Pair cache persists across restarts — token resolution is never lost.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     LiteChainIndexer                        │
├─────────────┬──────────────┬──────────────┬─────────────────┤
│   rpc.py    │   dex.py     │  storage.py  │    state.py     │
│  Endpoint   │  V2/V3 Swap  │  NDJSON +    │  State file +   │
│  pool +     │  Decoder +   │  ClickHouse  │  Pair cache     │
│  health     │  PairCreated │  Writer      │  persistence    │
│  tracking   │  Parser      │              │                 │
├─────────────┴──────────────┴──────────────┴─────────────────┤
│                     indexer.py (LiteIndexer)                 │
│           Orchestrator: fetch → parse → enrich → persist    │
├─────────────────────────────────────────────────────────────┤
│                     chains.py + config.py                    │
│           111 endpoints · 14 chains · event topics          │
├─────────────────────────────────────────────────────────────┤
│                     cli.py (entry point)                     │
│          --daemon · --dex · --deployers · --chains          │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
RPC (eth_getLogs) → LiteIndexer.fetch_transfers()
                  → _parse_logs() → parse_transfer_logs()
                  → _enrich_timestamps()
                  → write_ndjson()        {chain}/{date}/events.jsonl
                  → write_clickhouse()    wallet_memory.evm_transfers

DEX (eth_getLogs) → LiteIndexer.fetch_swap_events()
                  → fetch_pair_created_events()  (build token mapping)
                  → _parse_swap_logs()
                  → _batch_resolve_pool_tokens() (eth_call fallback)
                  → write_dex_ndjson()   {chain}/dex/{date}/events.jsonl
                  → write_dex_clickhouse()  wallet_memory.dex_events
```

---

## Configuration

### Environment Variables

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

### RPC Endpoints

Endpoints are defined in `lite_indexer/chains/endpoints.py` (EVM chains) and
`lite_indexer/chains/non_evm.py` (Solana/Tron) — that is the source of truth.
Each chain has 3 tiers:

- **Tier 1** — Fast, reliable (drpc.org, official RPCs)
- **Tier 2** — Fallbacks (1rpc.io, publicnode.com)
- **Tier 3** — Last resort (ankr.com, cloudflare-eth.com)

---

## CLI Reference

```
usage: lite-indexer [-h] [--chain CHAIN] [--from-block FROM_BLOCK]
                    [--to-block TO_BLOCK] [--batch-size BATCH_SIZE]
                    [--disk-cap DISK_CAP] [--daemon] [--chains CHAINS]
                    [--dex] [--deployers] [--start-from-head]
                    [--data-dir DATA_DIR] [--log-level LOG_LEVEL]

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
  --data-dir PATH       Data directory for NDJSON + state
  --log-level LEVEL     Log level (DEBUG, INFO, WARNING, ERROR)
```

---

## Development

```bash
git clone https://github.com/RugMunchMedia/lite-chain-indexer
cd lite-chain-indexer
pip install -e ".[dev]"
ruff check .
pytest -v
```

---

## License

MIT — free for any use, commercial or personal. See [LICENSE](LICENSE).

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Related Projects

- [RugMunch RMI Backend](https://github.com/RugMunchMedia/rmi-backend) — Crypto intelligence platform using LiteChainIndexer
- [chain-indexer](https://github.com/getlago/lago) — Full blockchain indexer (reference)
