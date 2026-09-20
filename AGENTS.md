# Lite Chain Indexer — Dev Guide

> Lightweight multi-chain blockchain indexer using only free RPCs.

## House Standard

Module <=200 lines hard (new), one primary function per module, zero monkeypatch in production, shrink-only ratchets — all CI-enforced. See **[docs/STANDARDS.md](docs/STANDARDS.md)**.

## What This Repo Is

Lite Chain Indexer indexes ERC-20 Transfer events, DEX Swap events (V2+V3),
and PairCreated/PoolCreated events across 14 EVM chains — using
**eth_getLogs** on curated free RPC endpoints. No Subsquid, no Reth,
no infrastructure bill.

## Architecture

```
lite_indexer/
  __init__.py    # Package exports
  __main__.py    # python -m lite_indexer
  cli.py         # CLI + daemon entry point
  indexer.py     # LiteIndexer orchestrator + sync loop
  rpc.py         # RPC endpoint pool, health tracking, HTTP client
  dex.py         # DEX event decoders (V2/V3 Swap, PairCreated)
  storage.py     # NDJSON + ClickHouse writers
  state.py       # State file + pair cache persistence
  chains.py      # Chain configs, RPC endpoints, poll intervals
  config.py      # Constants, event topics, factory addresses
  logging.py     # structlog-based structured logging
```

## Standards

- **Python**: 3.11+, ruff (line-length=120), strict mypy
- **Testing**: pytest with asyncio mode
- **Commits**: Conventional commits (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`)
- **Secrets**: None in git — gitleaks enforced
- **Deps**: Minimal — httpx + structlog + optional clickhouse-driver

## Owner

Community maintained. PRs welcome.
