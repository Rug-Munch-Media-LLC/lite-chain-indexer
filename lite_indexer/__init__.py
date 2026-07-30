"""
LiteChainIndexer — Lightweight multi-chain blockchain indexer using free RPCs.

Indexes ERC-20 Transfer events, DEX Swap events (V2+V3), and
PairCreated/PoolCreated events across 14 EVM chains with 111+ RPC endpoints.

Features:
  - 111+ verified endpoints across 14 chains (6-12 per chain)
  - Per-provider token bucket rate limiting
  - Automatic endpoint health verification at startup
  - Exponential backoff for rate limits and failures
  - Provider-aware block range limits
"""

from lite_indexer.chains import (
    CHAIN_POLL_INTERVALS,
    GET_LOGS_ENDPOINTS,
    PROVIDER_RATE_LIMITS,
    get_endpoints_for_range,
    get_max_block_range,
    get_provider_rate_limit,
)
from lite_indexer.cli import async_main, main, main_daemon, parse_args
from lite_indexer.config import BATCH_SIZE, DISK_CAP_BYTES
from lite_indexer.indexer import LiteIndexer

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
    "get_endpoints_for_range",
    "get_max_block_range",
    "get_provider_rate_limit",
]
