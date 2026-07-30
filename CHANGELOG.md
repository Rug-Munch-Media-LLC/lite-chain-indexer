# Changelog

All notable changes to LiteChainIndexer will be documented in this file.

## [0.1.0] — 2026-07-30

### Added

- **Initial open source release** of LiteChainIndexer — a free, lightweight,
  multi-chain blockchain indexer using only public RPC endpoints.
- **14 chains supported**: Ethereum, Base, BSC, Polygon, Arbitrum, Optimism,
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

### Technical Highlights

- Single Python process, no external dependencies beyond httpx and structlog.
- <200MB RAM for 12+ concurrent chains.
- Thread-safe state management with file locking.
- 3M+ events/hour sustained throughput on free RPCs.
- 79M+ events indexed across production deployment.
