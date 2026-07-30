# Contributing to LiteChainIndexer

Thanks for your interest in contributing! LiteChainIndexer is a community-driven,
open-source blockchain indexer. All contributions — bug reports, feature requests,
documentation improvements, and code — are welcome.

## Quick Start

```bash
# Clone
git clone https://github.com/RugMunchMedia/lite-chain-indexer
cd lite-chain-indexer

# Install in editable mode with dev dependencies
pip install -e ".[dev]"

# Run lint + tests before committing
ruff check .
pytest -v
```

## Development Workflow

1. **Fork the repo** and create a feature branch from `main`.
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
  rpc.py         # RPC endpoint pool, health tracking, client
  dex.py         # DEX event decoders (V2/V3 swap, pair creation)
  storage.py     # NDJSON + ClickHouse writers
  state.py       # State file + pair cache persistence
  chains.py      # Chain configs, RPC endpoints, poll intervals
  config.py      # Constants, event topics, factory addresses
  logging.py     # structlog-based structured logging
tests/
scripts/
config/
```

## Adding a New Chain

1. Add RPC endpoints to `lite_indexer/chains.py` (in `GET_LOGS_ENDPOINTS`).
2. Add poll interval to `CHAIN_POLL_INTERVALS`.
3. Add provider rate limits if using new RPC providers.
4. Add DEX factory addresses if the chain has known DEXes.
5. Update the README chain table.
6. Write tests for the new chain config.

## Testing

We use pytest with `asyncio_mode = auto`, so async tests work out of the box.

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
