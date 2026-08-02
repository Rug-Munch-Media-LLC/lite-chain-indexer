# FAQ — Lite Chain Indexer

Answers to the questions we hear most often. Still stuck? Open an issue on [Forgejo](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer/issues) or email admin@rugmunch.io.

---

## How is this free? How do you afford RPCs?

We don't pay for any RPCs — and neither do you. The indexer uses **free public RPC endpoints** that blockchain networks and infrastructure providers publish for exactly this purpose. The project ships with **123 curated, verified endpoints** across 16 chains (113 EVM + 6 Solana + 4 Tron), organized in tiers:

- **Tier 1** — fast, reliable public endpoints (drpc.org, official RPCs, onfinality)
- **Tier 2** — fallbacks (1rpc.io, publicnode.com, blastapi)
- **Tier 3** — last resort (ankr.com, cloudflare-eth.com, tatum)
- **Tier 4** — optional free API-key tiers (Alchemy, GetBlock, Chainstack, QuickNode, Helius) — only if you want the headroom

The RPC pool self-heals: endpoints are health-checked at startup, rate-limited per provider with token buckets, and rotated on failure. Because the indexer respects each provider's `eth_getLogs` range limits and rate caps, it stays inside free tiers while sustaining millions of events per hour.

**Bottom line:** the only cost is the hardware you already have (a $5 VPS is plenty). There is no API key required, no gateway subscription, and no per-query fee. If a free endpoint disappears, the pool rotates to the next healthy one — no manual intervention.

## What chains are supported?

**16 chains** — 14 EVM chains plus Solana and Tron:

| EVM (14) | Non-EVM (2) |
|----------|-------------|
| Ethereum, Base, BSC, Polygon, Arbitrum, Optimism, Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo | Solana, Tron |

Per-chain event coverage:

- **EVM** — ERC-20 `Transfer` events, DEX `Swap` events (Uniswap V2/V3 and forks like PancakeSwap, QuickSwap), and `PairCreated`/`PoolCreated` deployer events
- **Solana** — SPL token balance changes (`getSignaturesForAddress` + `getTransaction`)
- **Tron** — TRC-20 transfers via the TronGrid event API

Live per-chain statistics are in the [README chain table](../README.md#chains-covered) — currently **231M+ transfers and 58M+ DEX events** indexed in production.

## How does it compare to Ponder/Subsquid?

Short version: **same job, a fraction of the cost and complexity.**

- **Ponder** — a TypeScript framework. It's free and self-hosted, but *you* source and manage RPC endpoints, you write handlers in TS, and you pay for the infrastructure to run it at scale. No built-in multi-chain endpoint pool, no deployer tracking, no parquet/DuckDB archive.
- **Subsquid** — powerful, but its hosted/gateway tiers cost money at scale, its free tier is limited, and the self-hosted setup is a full TypeScript project with a learning curve. No built-in free RPC pool; parquet support is partial and requires your own pipeline.
- **Lite Chain Indexer** — a single Python process, zero config for RPCs, zero infra cost, and it ships with deployer tracking and a DuckDB-queryable parquet archive out of the box. If you want the fastest possible setup from a cold clone to live indexing, this is it.

See the [detailed comparison](COMPARISON.md) for a row-by-row breakdown.

## Can I query historical data?

**Yes — three ways:**

1. **NDJSON files** — every event is written to `{data_dir}/{chain}/{date}/events.jsonl` (auto-compressed to `.zst` after 15 minutes). Portable, append-only, ready for any pipeline.
2. **ClickHouse** — live inserts into `wallet_memory.*` tables (`evm_transfers`, `dex_events`, `deployer_events`, `solana_spl_balance_changes`). Millisecond queries over billions of rows. Ideal for hot, real-time analytics.
3. **Parquet on B2 + DuckDB** — parquet snapshots are exported to B2 daily at 03:30 UTC (`lite-indexer-parquet-daily.sh`). Query them with plain SQL using the included `duckdb_query` module — no node, no server, no ClickHouse round-trip:

```python
from lite_indexer.storage.duckdb_query import query_parquet

# Count ETH transfers yesterday
rows = query_parquet(
    "SELECT count(*) AS n FROM transfers WHERE toDate(block_timestamp) = '2026-07-31'",
    chain="ethereum",
)
```

Historical **backfill** is also supported — pass `--from-block`/`--to-block` for a specific range, or run without `--start-from-head` to index from genesis.

## How do I add a new chain?

Adding a new EVM chain is a small config change — no new code required for standard chains:

1. Add RPC endpoints to `lite_indexer/chains/endpoints.py` (`GET_LOGS_ENDPOINTS`)
2. Register the chain in `SUPPORTED_CHAINS` (`lite_indexer/strategies/__init__.py`)
3. Add a poll interval (`lite_indexer/chains/polling.py`) and block time (`lite_indexer/config.py`)
4. Add block-range caps and rate limits (`lite_indexer/chains/limits.py`)
5. Optionally add DEX factory addresses (`lite_indexer/config.py`)
6. Add it to your daemon list: `lite-indexer --daemon --chains ethereum,base,...,<newchain>`

For a brand-new **non-EVM** chain (e.g., a different ecosystem), implement a strategy following the `solana/` or `tron/` pattern in `lite_indexer/strategies/` and wire it into `create_strategy()`.

Full steps with file paths: [CONTRIBUTING.md](../CONTRIBUTING.md#adding-a-new-chain). PRs that add chains are always welcome!

## What hardware do I need?

Very little:

| Resource | Minimum | Notes |
|----------|---------|-------|
| CPU | 1–2 cores | A $5–10 VPS is the sweet spot |
| RAM | **~2 GB** | For all 16 chains in a single process |
| Disk | 10 GB default cap | Configurable via `DISK_CAP_GB`; NDJSON auto-compresses |
| ClickHouse | Optional | Only needed for live analytical queries |

It's a single Python process (`asyncio`), so it runs on a VPS, a Raspberry Pi-class box, a container, or inside Docker. The production deployment runs on a 2-core VPS with free public RPCs and sustains ~3M events/hour.

## How do I contribute?

Read [CONTRIBUTING.md](../CONTRIBUTING.md). In short:

1. Clone the repo from [Forgejo](https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer) (mirrors: Codeberg, GitLab, Radicle)
2. `pip install -e ".[dev]"`
3. Write tests first, run `ruff check .` and `pytest -v`
4. Open a pull request with a clear, conventional commit message

All contributions are welcome: bug reports, new chains, new RPC endpoints, docs, and features. By contributing you agree to the MIT license.

---

*Still have a question? Open an issue or email admin@rugmunch.io.*
