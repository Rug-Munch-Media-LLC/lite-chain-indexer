"""
LiteChainIndexer — Lightweight multi-chain blockchain indexer using free RPCs.

Indexes blockchain transfer events across multiple chain types:
  - EVM chains (14): ERC-20 Transfer events, DEX Swap events (V2+V3),
    and PairCreated/PoolCreated events
  - Solana: SPL token balance changes via getSignaturesForAddress
  - Tron: TRC20 Transfer events via TronGrid event API

Architecture uses a strategy pattern:
  - AbstractChainStrategy (base)
  - EVMChainStrategy (eth_getLogs for 14 EVM chains)
  - SolanaStrategy (getSignaturesForAddress + getTransaction)
  - TronStrategy (TronGrid event API)
"""

from lite_indexer.chains import (
    CHAIN_POLL_INTERVALS,
    GET_LOGS_ENDPOINTS,
    PROVIDER_RATE_LIMITS,
    get_provider_rate_limit,
)
from lite_indexer.cli import async_main, main, main_daemon, parse_args
from lite_indexer.config import BATCH_SIZE, DISK_CAP_BYTES
from lite_indexer.indexer import LiteIndexer
from lite_indexer.storage import ClickHouseWriter, NDJSONWriter, ParquetExporter
from lite_indexer.strategies import (
    SUPPORTED_CHAINS,
    AbstractChainStrategy,
    EVMChainStrategy,
    SolanaStrategy,
    TronStrategy,
    create_strategy,
)

__all__ = [
    "LiteIndexer",
    "main",
    "async_main",
    "main_daemon",
    "parse_args",
    "GET_LOGS_ENDPOINTS",
    "CHAIN_POLL_INTERVALS",
    "BATCH_SIZE",
    "DISK_CAP_BYTES",
    "PROVIDER_RATE_LIMITS",
    "get_provider_rate_limit",
    # Storage exports
    "ClickHouseWriter",
    "NDJSONWriter",
    "ParquetExporter",
    # Strategy exports
    "SUPPORTED_CHAINS",
    "AbstractChainStrategy",
    "EVMChainStrategy",
    "SolanaStrategy",
    "TronStrategy",
    "create_strategy",
]
