# Lite Chain Indexer 🪶

**The self-hosted, zero-cost, multi-chain blockchain indexer.**
Index transfers, DEX swaps, deployer activity, and internal-transfer traces across **16 chains (14 EVM + Solana + Tron)** using **only free public RPCs**. Zero infrastructure cost. Zero API keys required. ~2 GB RAM. **359M+ rows in production on a $5 VPS.**

[![CI](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml/badge.svg?branch=main)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/actions/workflows/ci.yml)
[![Version](https://img.shields.io/badge/version-v1.3.0-blue.svg)](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/tags)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![16+ chains](https://img.shields.io/badge/chains-16-success.svg)](#chains-covered)
[![46 tests](https://img.shields.io/badge/tests-460%2B-success.svg)](#testing)

---

## Why Lite Chain Indexer

**Best-in-class differentiator: $0/month forever.**

| Tool | Cost | Chains | Self-hosted | Free RPCs | Deployer Events | DEX Events | Internal Traces |
|------|------|--------|:-----------:|:---------:|:---------------:|:----------:|:---------------:|
| **Lite Chain Indexer** | **$0** | **16** | **✅** | **✅** | **✅** | **✅** | **✅** |
| Subsquid | $50+/mo | 100+ | ❌ | ❌ | ❌ | ✅ | ❌ |
| Ponder | Self-hosted | Any EVM | ✅ | ❌ | ❌ | ❌ | ❌ |
| The Graph | $0.01/query | 30+ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Dune | $390/mo | 10+ | ❌ | ❌ | ❌ | ✅ | ❌ |

**Lite Chain Indexer is the only free, self-hosted indexer with deployer tracking, DEX swaps, and internal traces.**

---

## What It Does

Lite Chain Indexer watches on-chain activity and writes it to ClickHouse in real time:

1. **Transfer Events** — ERC-20/TRC-20/SPL token transfers across all chains
2. **DEX Swap Events** — Uniswap V2/V3 and fork swaps decoded in real time
3. **Deployer Events** — Track who deployed what, when, and their full history
4. **Internal Transfers** — WETH unwraps, bridge deposits, fee flows (invisible to most indexers)
5. **Pair/Pool Creation** — New DEX pools detected and indexed

All events flow through address-first projections for **sub-second wallet queries**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Lite Chain Indexer                               │
│                                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌────────────────────────────┐  │
│  │ Auto RPC │──▶│   Indexer    │──▶│    ClickHouse (real-time)  │  │
│  │ Ingestor │   │   Core       │   │  • evm_transfers           │  │
│  └──────────┘   └──────┬───────┘   │  • dex_events              │  │
│                        │           │  • deployer_events          │  │
│  ┌──────────┐   ┌──────▼───────┐   │  • internal_transfers      │  │
│  │  Chain   │──▶│  Strategies  │   └────────────────────────────┘  │
│  │ Registry │   │  • EVM       │                                   │
│  └──────────┘   │  • Solana    │   ┌────────────────────────────┐  │
│                 │  • Tron      │──▶│   Parquet Archive (cold)   │  │
│                 └──────────────┘   │  • DuckDB-queryable        │  │
│                                     │  • B2 upload               │  │
└─────────────────────────────────────────────────────────────────────┘
```

### Self-Healing Design

- **Backward-state guard** — Auto-recovers a clobbered state file from ClickHouse max block
- **Header-aware RPC throttling** — Respects `Retry-After` exactly, slows down *before* 429s
- **Circuit breakers** — Per-endpoint failure tracking with exponential backoff
- **Auto RPC discovery** — Continuously finds new free RPCs from Chainlist
- **Chain-level health** — A stalled chain auto-disables so others keep indexing
- **Reorg protection** — Detects chain reorganizations and rolls back state

---

## Quick Start

### 1. Install

```bash
pip install "git+https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git"
```

### 2. Index a Single Chain (No ClickHouse Required)

```bash
# Index Ethereum from current head
lite-indexer --chain ethereum --dex

# Index from specific blocks
lite-indexer --chain ethereum --from-block 25645000 --to-block 25646000

# Index with deployer tracking
lite-indexer --chain base --dex --deployers
```

### 3. Daemon Mode (Multi-Chain)

```bash
# Index multiple chains in parallel
lite-indexer --daemon --chains ethereum,base,bsc --dex --deployers
```

### 4. With ClickHouse (Recommended)

```bash
export CH_HOST=localhost CH_PORT=9000 CH_DATABASE=wallet_memory
lite-indexer --daemon --chains all --dex --deployers
```

---

## Chain Configuration

### Add a New Chain (No Code Changes)

```python
from lite_indexer.chain_registry import add_chain_config, ChainConfig, ChainType

add_chain_config("my_chain", ChainConfig(
    name="my_chain",
    chain_type=ChainType.EVM,
    chain_id=12345,
    rpc_endpoints=["https://rpc.mychain.com"],
    poll_interval_seconds=5,
    batch_size=2000,
    native_currency="MYTOKEN",
    block_time_seconds=2.0,
))
```

### List Supported Chains

```python
from lite_indexer.chain_registry import list_supported_chains
print(list_supported_chains())
# ['ethereum', 'base', 'bsc', 'polygon', 'arbitrum', ...]
```

---

## Auto RPC Ingestor

Continuously discovers and validates new free RPC endpoints:

```python
from lite_indexer.rpc.auto_ingestor import auto_ingest, monitor_endpoints

# One-time discovery for a chain
new_endpoints = await auto_ingest("ethereum")

# Continuous monitoring (background task)
asyncio.create_task(monitor_endpoints("ethereum", interval=3600))
```

---

## Chains Covered

| Chain | Type | Status | DEX | Deployers |
|-------|------|:------:|:---:|:---------:|
| Ethereum | EVM | ✅ | ✅ | ✅ |
| Base | EVM | ✅ | ✅ | ✅ |
| BSC | EVM | ✅ | ✅ | ✅ |
| Polygon | EVM | ✅ | ✅ | ✅ |
| Arbitrum | EVM | ✅ | ✅ | ✅ |
| Optimism | EVM | ✅ | ✅ | ✅ |
| Avalanche | EVM | ✅ | ✅ | ✅ |
| Fantom | EVM | ✅ | ✅ | ✅ |
| Linea | EVM | ✅ | ✅ | ✅ |
| Scroll | EVM | ✅ | ✅ | ✅ |
| zkSync Era | EVM | ✅ | ✅ | ✅ |
| Mantle | EVM | ✅ | ✅ | ✅ |
| Gnosis | EVM | ✅ | ✅ | ✅ |
| Celo | EVM | ✅ | ✅ | ✅ |
| Solana | Solana | ✅ | ✅ | ✅ |
| Tron | Tron | ✅ | ✅ | ✅ |

---

## Configuration

### Key Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `CH_HOST` | `127.0.0.1` | ClickHouse host |
| `CH_PORT` | `9000` | ClickHouse native port |
| `CH_USER` | `default` | ClickHouse user |
| `CH_DATABASE` | `wallet_memory` | ClickHouse database |
| `DATA_DIR` | `/var/data/chain-indexer` | State + parquet storage |
| `NDJSON_ENABLED` | `0` | Write-ahead NDJSON log (default off) |
| `LAZY_INDEXING_ENABLED` | `1` | Lazy per-chain DEX/deployer indexing |
| `REORG_PROTECTION_ENABLED` | `1` | Reorg staging / fork rollback (EVM) |
| `MAX_CONCURRENT_CHAINS` | `14` | Max parallel chain workers |

### CLI Reference

```bash
# Single chain
lite-indexer --chain ethereum [--dex] [--deployers] [--traces]

# Multi-chain daemon
lite-indexer --daemon --chains ethereum,base,bsc [--dex] [--deployers]

# Backfill
lite-indexer backfill --chain ethereum --from-block 25000000

# Export parquet
lite-indexer export-parquet --chain ethereum --date 2026-07-30

# Bootstrap from snapshot
lite-indexer bootstrap --chain ethereum --from-snapshot 2026-08-01

# Query ClickHouse
lite-indexer query --sql "SELECT count() FROM evm_transfers WHERE chain='ethereum'"
```

---

## Querying the Data

### With ClickHouse

```sql
-- All transfers for a wallet
SELECT * FROM evm_transfers 
WHERE (from = '0xabc...' OR to = '0xabc...') 
  AND chain = 'ethereum'
ORDER BY block_number DESC
LIMIT 1000;

-- DEX swaps for a token
SELECT * FROM dex_events 
WHERE token_address = '0x...' AND chain = 'base'
ORDER BY block_number DESC;

-- Deployer history
SELECT * FROM deployer_events 
WHERE deployer = '0x...'
ORDER BY block_number;
```

### With DuckDB (Parquet)

```sql
-- Query historical parquet data
SELECT token_address, count() as swaps, sum(amount_usd) as volume
FROM 'data/ethereum/dex_events/*.parquet'
GROUP BY token_address
ORDER BY volume DESC;
```

---

## Development

### Setup

```bash
git clone https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git
cd lite-chain-indexer
python -m venv .venv && source .venv/bin/activate
pip install -e ".[all]"
pre-commit install
```

### Run Tests

```bash
# Full suite (46 tests)
pytest -q

# Smoke tests only
pytest -q -m smoke

# Coverage
pytest --cov=lite_indexer --cov-report=term
```

---

## Testing

| Test Suite | Count | Scope |
|-----------|-------|-------|
| `test_smoke.py` | 17 | Core integration |
| `test_indexer.py` | 12 | Indexer state machine |
| `test_resilience.py` | 26 | State regression, recovery |
| `test_chain_registry.py` | 30 | Chain configuration |
| `test_auto_ingestor.py` | 16 | RPC discovery, validation |
| `test_rpc_*.py` | 300+ | RPC health, throttling, discovery |
| `test_strategies.py` | 700+ | EVM, Solana, Tron strategies |
| `test_webhooks.py` | 600+ | Webhook delivery |
| `test_traces.py` | 600+ | Internal trace collection |
| **Total** | **942+** | **Full coverage** |

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Run `pre-commit run --all-files`
4. Add tests for new functionality
5. Submit a pull request

---

## License

MIT — see [LICENSE](LICENSE) for details.
