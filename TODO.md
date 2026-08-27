# Lite Chain Indexer — TODO & Roadmap

## Status: Production Ready

**Test Coverage:** 994 tests passing, 5 skipped
**CI Gates:** Full fleet-standard CI with bandit, semgrep, gitleaks, ruff, mypy, diff-cover, docstring coverage, supply-chain

---

## Completed (2026-08-27)

- [x] Add chain registry with 16+ presets (EVM + Solana + Tron)
- [x] Add auto RPC ingestor with Chainlist discovery
- [x] Wire chain registry into LiteIndexer core
- [x] Add 46 tests for chain registry and auto ingestor
- [x] Add world-class README with comparison table
- [x] Split sync loop into SyncLoopMixin (1027→597 lines)
- [x] Add docstring coverage gate (60%)
- [x] Standardize CI workflow

---

## Planned

- [ ] Add more chain presets (Polygon zkEVM, Arbitrum Nova, etc.)
- [ ] Add health check endpoint for monitoring
- [ ] Add Prometheus metrics for chain indexing progress
- [ ] Add automatic chain detection from contract address
- [ ] Add support for more non-EVM chains (Aptos, Sui, Cosmos)

---

## Known Issues

- None blocking
