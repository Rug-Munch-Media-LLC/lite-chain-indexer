# Comparison — Lite Chain Indexer vs. the alternatives

A detailed, honest look at how Lite Chain Indexer stacks up against the most common
indexing options: **Ponder**, **Subsquid**, **a hosted Solana data platform**, and (for context) **The Graph**.

> **TL;DR** — If you want the same multi-chain event data without paying for
> infrastructure, managing RPC endpoints, or learning a TypeScript framework, Lite
> Chain Indexer is the only option that is free *and* self-contained *and*
> DuckDB-queryable out of the box.

## The short table

| Feature | Lite Chain Indexer | Ponder | Subsquid | a hosted Solana data platform |
|---------|:---:|:---:|:---:|:---:|
| Cost | **$0** | $0 + infra | $0 + infra | $49+/mo |
| Chains | **13+** | Configurable | Configurable | Solana only |
| Setup time | **5 min** | 30 min | 1 hour | Instant (API) |
| Free RPCs | ✅ **Built-in** | ❌ DIY | ❌ DIY | N/A |
| DuckDB queries | ✅ | ❌ | ❌ | ❌ |
| Parquet export | ✅ | ❌ | Partial | ❌ |
| Deployer tracking | ✅ | ❌ | ❌ | ❌ |
| DEX events | ✅ | Configurable | Configurable | Limited |
| Historical backfill | ✅ | ✅ | ✅ | ✅ |

## Row-by-row

### Cost

- **Lite Chain Indexer: $0.** Runs on free public RPCs (123 built-in, verified endpoints across 16 chains). No subscription, no API key, no gateway fee. Hardware is whatever you already have — a $5 VPS runs all 16 chains.
- **Ponder: $0 + infra.** The software is free, but you pay for RPC access at scale (free tiers throttle quickly on `eth_getLogs`) and for the server/container running your indexers. Multiple chains multiply your RPC costs.
- **Subsquid: $0 + infra.** Free self-hosted tier exists, but the hosted/gateway tiers (where most people end up at scale) cost real money. Self-hosting means sourcing and paying for your own data feeds.
- **a hosted Solana data platform: $49+/mo.** A hosted Solana data platform. Predictable pricing, but it starts at $49/month for meaningful limits and it only covers Solana. There is no self-hosted option.

### Chains

- **Lite Chain Indexer:** 16 chains out of the box — 14 EVM (Ethereum, Base, BSC, Polygon, Arbitrum, Optimism, Avalanche, Fantom, Linea, Scroll, zkSync Era, Mantle, Gnosis, Celo) + Solana + Tron. Adding a new EVM chain is a config change, not a rewrite.
- **Ponder / Subsquid:** "Configurable" is doing heavy lifting — you can index any EVM chain, but you configure each one yourself (RPCs, handlers, deploy pipelines). Solana/Tron support is not a first-class citizen.
- **a hosted Solana data platform:** Solana only.

### Setup time

- **Lite Chain Indexer:** clone → `pip install` → `lite-indexer --daemon --chains ...`. Five minutes from cold start to live indexing. No code generation, no subgraph manifest, no Docker Compose required.
- **Ponder:** ~30 minutes for a first indexer (scaffold, configure RPCs, write handlers).
- **Subsquid:** ~1 hour (squid scaffold, schema, mappings, deploy).
- **a hosted Solana data platform:** instant if you only need their hosted APIs — but you're locked to their endpoints and pricing.

### Free RPCs

- **Lite Chain Indexer:** 123 endpoints are built in, tiered, health-checked at startup, rate-limited per provider, and rotated on failure. You literally don't think about RPCs.
- **Ponder / Subsquid:** you bring your own endpoints. Free tiers are rate-limited, many free endpoints drop `eth_getLogs` support, and managing a pool is your problem.

### DuckDB queries & Parquet export

- **Lite Chain Indexer:** live ClickHouse writes (hot) **and** parquet snapshots exported to B2 every 6 hours (cold). Query the archive with plain SQL via the included `duckdb_query` module — `transfers`, `dex`, and `spl_balance_changes` views, zero infrastructure.
- **Ponder:** Postgres/SQLite only. No parquet, no DuckDB path.
- **Subsquid:** partial parquet support via its archive format, but you build the export pipeline yourself.
- **a hosted Solana data platform:** hosted APIs only. You export what they let you export.

### Deployer tracking & DEX events

- **Lite Chain Indexer:** `PairCreated`/`PoolCreated` tracking across known DEX factories (Uniswap V2/V3 forks, PancakeSwap, QuickSwap) plus decoded V2/V3 swap events — both **built in**, not "you can build it."
- **Ponder / Subsquid:** you write the mappings yourself. Possible, but it's your code to maintain.
- **a hosted Solana data platform:** limited DEX data (their own enrichment), no generic deployer tracking.

### Historical backfill

All four can reach back into history: Lite Chain Indexer via `--from-block`/`--to-block`
ranges or genesis backfill; Ponder/Subsquid via their sync mechanisms; a hosted Solana data platform via
historical endpoints (with platform limits).

## What about The Graph?

The Graph is a decentralized indexing protocol — hosted subgraphs over a public
network (with GRT fees) or self-hosted infrastructure you operate yourself.

| Feature | Lite Chain Indexer | The Graph |
|---------|:---:|:---:|
| Cost | **$0** | $0 self-hosted + infra / GRT fees hosted |
| Setup time | **5 min** | Days (subgraph manifest + deploy) |
| Language | **Python** | AssemblyScript/GraphQL subgraphs |
| Chains | **16** (incl. Solana, Tron) | ~70 hosted (EVM-heavy, mostly L1s) |
| Free RPCs | ✅ **Built-in** | ❌ (indexers bring their own) |
| DuckDB / parquet | ✅ | ❌ |
| Deployer tracking | ✅ | ❌ |
| DEX swap decoding | ✅ | ❌ (raw events only) |

The Graph shines when you want a *public, decentralized* query layer that many
dApps share. Lite Chain Indexer shines when you want **your own data**, fast, free,
and queryable in SQL — without a subgraph manifesto.

## When to pick what

| You want… | Pick |
|-----------|------|
| Zero-cost multi-chain data, self-hosted, now | **Lite Chain Indexer** |
| A TypeScript event-driven framework you control | Ponder |
| A managed/squid ecosystem with enterprise support | Subsquid |
| Production Solana data with managed APIs (budget: $49+/mo) | a hosted Solana data platform |
| A public decentralized query layer for dApps | The Graph |

## Methodology

- Cost figures are list prices as of 2026-08-01 and exclude labor. "Infra" means
  the server/container you must run yourself.
- Lite Chain Indexer stats are live from the production ClickHouse store
  (`wallet_memory`): 231M+ transfer events, 58M+ DEX events, ~0.9M deployer events.
- Ponder/Subsquid/a hosted Solana data platform/The Graph comparisons are based on public documentation
  and typical community deployments; feature availability can change.

---

*Last updated: 2026-08-01 · See also [FAQ](FAQ.md) and the [README](../README.md).*
