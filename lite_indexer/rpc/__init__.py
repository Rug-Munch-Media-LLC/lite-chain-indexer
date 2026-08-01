"""
RPC endpoint pool — health tracking, backoff, and direct HTTP client.

Provides the RPC layer for LiteIndexer with:
  - Tracks endpoint health with exponential backoff
  - Rotates across multiple endpoints for load distribution
  - Provides eth_getLogs, eth_blockNumber, eth_getBlockByNumber,
    and eth_call methods
  - Verifies endpoint health before adding to rotation
  - Per-provider token bucket rate limiting

Split into sub-modules:
  - client.py: RpcClient class + fetch_block_timestamps
  - health.py: Endpoint verification + health state
  - ratelimit.py: Token-bucket rate limiting per provider
  - pool.py: Endpoint selection, failure tracking, backoff
"""

from __future__ import annotations

from lite_indexer.rpc.client import RpcClient, fetch_block_timestamps
from lite_indexer.rpc.health import is_endpoint_healthy, verify_endpoint, verify_endpoints_for_chain
from lite_indexer.rpc.pool import apply_endpoint_backoff, get_best_endpoint, handle_rate_limit, record_endpoint_failure

__all__ = [
    "RpcClient",
    "apply_endpoint_backoff",
    "fetch_block_timestamps",
    "get_best_endpoint",
    "handle_rate_limit",
    "is_endpoint_healthy",
    "record_endpoint_failure",
    "verify_endpoint",
    "verify_endpoints_for_chain",
]
