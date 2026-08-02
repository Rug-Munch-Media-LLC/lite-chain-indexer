"""
RPC endpoint pool — health tracking, backoff, and direct HTTP client.

Provides the RPC layer for LiteIndexer with:
  - Tracks endpoint health with exponential backoff
  - Rotates across multiple endpoints for load distribution
  - Provides eth_getLogs, eth_blockNumber, eth_getBlockByNumber,
    eth_getTransactionReceipt, and eth_call methods
  - Verifies endpoint health before adding to rotation
  - Per-provider token bucket rate limiting
  - Per-method routing, cost tracking, and circuit breakers

Split into sub-modules:
  - client.py: RpcClient class + fetch_block_timestamps
  - health.py: Endpoint verification + health state
  - ratelimit.py: Token-bucket rate limiting per provider
  - pool.py: Endpoint selection, failure tracking, backoff
  - routing.py: Per-method routing, usage tracking, circuit breakers
  - discovery.py: chainlist.org free-RPC auto-discovery
"""

from __future__ import annotations

from lite_indexer.rpc.client import RpcClient, fetch_block_timestamps
from lite_indexer.rpc.health import (
    is_endpoint_healthy,
    monitor_endpoint_health,
    verify_endpoint,
    verify_endpoints_for_chain,
)
from lite_indexer.rpc.pool import (
    apply_endpoint_backoff,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
    record_endpoint_success,
)
from lite_indexer.rpc.routing import (
    CIRCUIT_BREAKER,
    USAGE_TRACKER,
    RpcCircuitBreaker,
    RpcUsageTracker,
    endpoint_tier,
    is_circuit_open,
    log_rpc_usage,
    preferred_tier_for_method,
    record_rpc_usage,
    select_endpoint_for_method,
)


class RpcFetchFailedError(RuntimeError):
    """Raised when all RPC endpoints fail to return data for a range.

    Acts as a data-loss guard: the indexer must not advance its state
    past a block range that was never successfully queried. Callers
    treat this as "retry the range on the next cycle" instead of
    "no events in range".
    """


__all__ = [
    "RpcClient",
    "RpcFetchFailedError",
    "apply_endpoint_backoff",
    "fetch_block_timestamps",
    "get_best_endpoint",
    "handle_rate_limit",
    "is_endpoint_healthy",
    "monitor_endpoint_health",
    "record_endpoint_failure",
    "record_endpoint_success",
    "verify_endpoint",
    "verify_endpoints_for_chain",
    # routing
    "RpcUsageTracker",
    "RpcCircuitBreaker",
    "USAGE_TRACKER",
    "CIRCUIT_BREAKER",
    "endpoint_tier",
    "preferred_tier_for_method",
    "select_endpoint_for_method",
    "record_rpc_usage",
    "log_rpc_usage",
    "is_circuit_open",
]
