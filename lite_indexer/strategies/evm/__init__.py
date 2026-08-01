"""
EVM chain strategy package.

Provides the EVMChainStrategy orchestrator class and individual
handler modules for transfer events, DEX swaps, and pair creation.
"""

from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.deployer_handler import fetch_pair_created
from lite_indexer.strategies.evm.dex_handler import fetch_swap_events
from lite_indexer.strategies.evm.strategy import EVMChainStrategy
from lite_indexer.strategies.evm.transfer_handler import fetch_transfers

__all__ = [
    "EVMChainStrategy",
    "fetch_transfers",
    "fetch_swap_events",
    "fetch_pair_created",
    "enforce_block_range",
    "detect_reorg",
]
