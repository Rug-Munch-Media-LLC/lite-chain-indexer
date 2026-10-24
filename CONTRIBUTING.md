# Contributing to Lite Chain Indexer

Thanks for your interest in contributing! Lite Chain Indexer is a community-driven,
open-source blockchain indexer. All contributions — bug reports, feature requests,
documentation improvements, and code — are welcome.

## Quick Start

```bash
# Clone (primary mirror: Forgejo — also on Codeberg, GitLab, Radicle)
git clone https://git.rugmunch.io/RugMunchMedia/lite-chain-indexer.git
cd lite-chain-indexer

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run lint + tests before committing
ruff check .
pytest -v
```

## Development Workflow

1. **Fork the repo on Forgejo** and create a feature branch from `main`.
   Mirrors: [Codeberg](https://codeberg.org/cryptorugmunch/lite-chain-indexer) ·
   [GitLab](https://gitlab.com/cryptorugmuncher/lite-chain-indexer) · Radicle (`rad://z3ca3ygazjcDWFXQJKrFmDBvprM6k`).
2. **Write tests first** for any new functionality (we use pytest).
3. **Run the linter** — we use ruff with line-length=120. Config is in `pyproject.toml`.
4. **Run type checking** — we use mypy in strict mode.
5. **Run all tests** and ensure they pass.
6. **Submit a pull request** with a clear description of the change.

## Code Standards

| Requirement | Standard |
|-------------|----------|
| Python version | 3.11+ |
| Code style | ruff (E, F, I, N, W, UP, B, SIM, T20) |
| Line length | 120 |
| Type annotations | Required for all public functions |
| Docstrings | Google-style for public APIs |
| Commits | [Conventional Commits](https://www.conventionalcommits.org/) |
| Testing | pytest with asyncio mode |

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(chains): add Solana RPC support
fix(rpc): handle rate limit retry correctly
docs: add Docker deployment guide
refactor(storage): consolidate NDJSON writers
```

## Project Structure

```
lite_indexer/
  __init__.py    # Package exports
  __main__.py    # python -m lite_indexer entry point
  cli.py         # CLI argument parser and entry points
  indexer.py     # LiteIndexer orchestrator + sync loop
  config.py      # Constants, event topics, factory addresses, block times
  metrics.py     # Consolidated metrics counters
  logging.py     # structlog-based structured logging
  health.py      # Endpoint health-check helpers
  state.py       # State file + pair cache persistence
  chains/
    endpoints.py # EVM chain RPC endpoint lists (tiered)
    non_evm.py   # Solana + Tron RPC endpoint lists
    limits.py    # Provider rate limits + block range caps
    polling.py   # Per-chain poll intervals + health config
    helpers.py   # Endpoint selection helpers
  rpc/
    pool.py      # Endpoint pool, health tracking, rotation
    client.py    # HTTP RPC client
    ratelimit.py # Per-provider token buckets
    health.py    # Endpoint verification
    verify.py    # Verification logic
  dex/
    v2.py, v3.py # DEX V2/V3 swap decoders
    parsers.py   # Event parser dispatch
    transfers.py # Transfer decoding helpers
    common.py    # Shared DEX utilities
  storage/
    ndjson.py    # NDJSON writer (auto .zst compression)
    clickhouse.py# ClickHouse native-protocol writer
    disk.py      # Disk cap + free-space checks
    duckdb_query.py # DuckDB query layer for B2 parquet archives
  strategies/
    base.py      # AbstractChainStrategy
    evm/         # EVMChainStrategy (transfers, DEX, deployers)
    solana/      # SolanaStrategy (SPL balance changes)
    tron/        # TronStrategy (TRC-20 transfers)
tests/
scripts/
config/
```

## Adding a New Chain

For an **EVM** chain:

1. Add RPC endpoints to `lite_indexer/chains/endpoints.py` (in `GET_LOGS_ENDPOINTS`).
2. Add the chain to `SUPPORTED_CHAINS` in `lite_indexer/strategies/__init__.py`.
3. Add a poll interval to `CHAIN_POLL_INTERVALS` in `lite_indexer/chains/polling.py`.
4. Add per-chain block time to `CHAIN_BLOCK_TIMES` in `lite_indexer/config.py`.
5. Add block-range safety caps to `CHAIN_BLOCK_RANGE_LIMITS` in `lite_indexer/chains/limits.py`.
6. Add provider rate limits if you use a new RPC provider.
7. Add DEX factory addresses if the chain has known DEXes (`FACTORY_ADDRESSES` / `V3_FACTORY_ADDRESSES` in `lite_indexer/config.py`).
8. Update the README chain table and `docs/FAQ.md`.
9. Write tests for the new chain config.

For a **non-EVM** chain (e.g., a new Solana or Tron-like network):

1. Add RPC endpoints to `lite_indexer/chains/non_evm.py`.
2. Implement a strategy in `lite_indexer/strategies/` (see `solana/` and `tron/` for the pattern).
3. Wire the strategy into `create_strategy()` in `lite_indexer/strategies/__init__.py`.

## Testing

We use pytest with `asyncio_mode = auto`, so async tests work out of the box.

Run tests with the system interpreter — `python3 -m pytest` — not a local
`.venv` (see `docs/TEST-ENV.md`: `.venv/` is a stale 3.11 venv with no `pip`,
while runtime and the canonical test interpreter are `python3` 3.12).

```bash
# Run all tests
pytest -v

# Run with coverage
pytest --cov=lite_indexer --cov-report=term-missing

# Run a specific test class
pytest -v -k "TestParseTransferLogs"
```

## Reporting Issues

Include:
- Python version and OS
- Chain you were indexing
- RPC endpoints used (if custom)
- Full error output and logs
- Steps to reproduce

## License

By contributing, you agree that your contributions will be licensed under the
MIT License (see [LICENSE](LICENSE)).
