"""
EVM chain strategy — facade module.

This module is now a re-export facade. The implementation has been
split into the ``evm/`` package:

- ``evm/strategy.py`` — EVMChainStrategy orchestrator
- ``evm/transfer_handler.py`` — ERC-20 Transfer event fetching
  (with chunk-based parallel fetching + TrueBlocks bloom pre-filtering)
- ``evm/dex_handler.py`` — DEX Swap event fetching (V2+V3)
- ``evm/deployer_handler.py`` — PairCreated/PoolCreated event fetching
- ``evm/bloom.py`` — Ethereum logsBloom pre-filtering + cache
"""
# ruff: noqa: F401 — re-exports

from lite_indexer.strategies.evm.bloom import (
    BloomCache,
    BloomFilter,
    candidate_blocks_for_address,
    candidate_blocks_for_topic,
    fetch_block_blooms,
)
from lite_indexer.strategies.evm.strategy import EVMChainStrategy

__all__ = [
    "EVMChainStrategy",
    "BloomCache",
    "BloomFilter",
    "candidate_blocks_for_address",
    "candidate_blocks_for_topic",
    "fetch_block_blooms",
]
