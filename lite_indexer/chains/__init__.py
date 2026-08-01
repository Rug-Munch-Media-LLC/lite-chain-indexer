"""
Chain configuration package — RPC endpoints, rate limits, and poll intervals.

Split by concern:
  - endpoints: EVM chain eth_getLogs RPC endpoint lists
  - non_evm:   Solana and Tron RPC endpoint lists
  - limits:    provider rate limits and chain block-range caps
  - polling:   chain poll intervals and health-check config
  - helpers:   endpoint selection helper functions

All public names are re-exported here so `from lite_indexer.chains import X`
keeps working after the split.
"""

from .endpoints import GET_LOGS_ENDPOINTS
from .helpers import get_provider_rate_limit
from .limits import CHAIN_BLOCK_RANGE_LIMITS, DEFAULT_RATE_LIMIT, PROVIDER_RATE_LIMITS
from .non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS
from .polling import (
    CHAIN_POLL_INTERVALS,
    ENDPOINT_VERIFY_INTERVAL,
    ENDPOINT_VERIFY_TIMEOUT,
    VERIFY_BLOCK_DELTA,
)

__all__ = [
    "GET_LOGS_ENDPOINTS",
    "SOLANA_ENDPOINTS",
    "TRON_ENDPOINTS",
    "PROVIDER_RATE_LIMITS",
    "DEFAULT_RATE_LIMIT",
    "CHAIN_BLOCK_RANGE_LIMITS",
    "CHAIN_POLL_INTERVALS",
    "ENDPOINT_VERIFY_INTERVAL",
    "ENDPOINT_VERIFY_TIMEOUT",
    "VERIFY_BLOCK_DELTA",
    "get_provider_rate_limit",
]
