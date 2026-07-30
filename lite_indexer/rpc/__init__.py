"""
RPC endpoint pool — health tracking, backoff, and direct HTTP client.

Provides the RPC layer for LiteIndexer with:
  - Tracks endpoint health with exponential backoff
  - Rotates across multiple endpoints for load distribution
  - Provides eth_getLogs, eth_blockNumber, eth_getBlockByNumber,
    and eth_call methods
  - Verifies endpoint health before adding to rotation
  - Per-provider token bucket rate limiting
  - Multi-tier RPC support (FREE → FREEMIUM → PAID fallback)

Split into sub-modules:
  - client.py: RpcClient class + fetch_block_timestamps
  - health.py: Endpoint verification + health state
  - ratelimit.py: Token-bucket rate limiting per provider
  - pool.py: Endpoint selection, failure tracking, backoff
  - tiers.py: Tiered RPC system (FREE/FREEMIUM/PAID)
"""

from __future__ import annotations

from lite_indexer.rpc.client import RpcClient, fetch_block_timestamps
from lite_indexer.rpc.health import is_endpoint_healthy, verify_endpoint, verify_endpoints_for_chain
from lite_indexer.rpc.pool import apply_endpoint_backoff, get_best_endpoint, handle_rate_limit, record_endpoint_failure
from lite_indexer.rpc.tiers import (
    RPC_ENDPOINTS_BY_TIER,
    RpcTier,
    get_best_chain_endpoint,
    get_chain_endpoints,
)

__all__ = [
    "RpcClient",
    "apply_endpoint_backoff",
    "fetch_block_timestamps",
    "get_best_endpoint",
    "get_best_chain_endpoint",
    "get_chain_endpoints",
    "handle_rate_limit",
    "is_endpoint_healthy",
    "RPC_ENDPOINTS_BY_TIER",
    "RpcTier",
    "record_endpoint_failure",
    "verify_endpoint",
    "verify_endpoints_for_chain",
]
