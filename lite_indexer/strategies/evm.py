"""
EVM chain strategy — facade module.

This module is now a re-export facade. The implementation has been
split into the ``evm/`` package:

- ``evm/strategy.py`` — EVMChainStrategy orchestrator
- ``evm/transfer_handler.py`` — ERC-20 Transfer event fetching
- ``evm/dex_handler.py`` — DEX Swap event fetching (V2+V3)
- ``evm/deployer_handler.py`` — PairCreated/PoolCreated event fetching
"""
# ruff: noqa: F401 — re-exports

from lite_indexer.strategies.evm.strategy import EVMChainStrategy

__all__ = [
    "EVMChainStrategy",
]
