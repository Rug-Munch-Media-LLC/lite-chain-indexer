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
  - throttle.py: Header-aware rate-limit tracking + pre-emptive throttling
  - pool.py: Endpoint selection, failure tracking, backoff
  - routing.py: Per-method routing, usage tracking, circuit breakers
  - discovery.py: chainlist.org free-RPC auto-discovery
"""

from __future__ import annotations

from lite_indexer.rpc.budgets import (
    BUDGET_TRACKER,
    ProviderBudgetTracker,
    budget_available,
    budget_slot,
    budget_wait_seconds,
    record_budget_dispatch,
    record_budget_response,
    record_budget_success,
)
from lite_indexer.rpc.capabilities import (
    ArchiveRoutingError,
    NoArchiveEndpointError,
    archive_capable_endpoints,
    is_archive_capable,
    method_requires_archive,
)
from lite_indexer.rpc.client import RpcClient, clear_timestamp_cache, fetch_block_timestamps
from lite_indexer.rpc.health import (
    is_endpoint_healthy,
    monitor_endpoint_health,
    verify_endpoint,
    verify_endpoints_for_chain,
)
from lite_indexer.rpc.pool import (
    apply_endpoint_backoff,
    ban_endpoint,
    desperation_probe_due,
    force_probe_endpoint,
    get_best_endpoint,
    handle_rate_limit,
    is_endpoint_banned,
    is_endpoint_hard_banned,
    is_endpoint_soft_banned,
    least_banned_endpoint,
    mark_desperation_probe,
    pool_fully_down,
    pool_rate_limited,
    record_endpoint_failure,
    record_endpoint_success,
    unban_endpoint,
)
from lite_indexer.rpc.provider_caps import (
    PROVIDER_LIMITER,
    ProviderRateLimiter,
    provider_slot,
    record_provider_response,
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
    select_archive_endpoint,
    select_endpoint_for_method,
)
from lite_indexer.rpc.throttle import (
    RATE_TRACKER,
    RPCRateTracker,
    record_rpc_response,
    throttle_before,
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
    "clear_timestamp_cache",
    "RpcFetchFailedError",
    "apply_endpoint_backoff",
    "ban_endpoint",
    "fetch_block_timestamps",
    "get_best_endpoint",
    "handle_rate_limit",
    "is_endpoint_banned",
    "is_endpoint_healthy",
    "monitor_endpoint_health",
    "record_endpoint_failure",
    "record_endpoint_success",
    "unban_endpoint",
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
    # pool resilience helpers
    "desperation_probe_due",
    "force_probe_endpoint",
    "is_endpoint_hard_banned",
    "is_endpoint_soft_banned",
    "least_banned_endpoint",
    "mark_desperation_probe",
    "pool_fully_down",
    "pool_rate_limited",
    # throttle
    "RATE_TRACKER",
    "RPCRateTracker",
    "record_rpc_response",
    "throttle_before",
    # provider caps
    "PROVIDER_LIMITER",
    "ProviderRateLimiter",
    "provider_slot",
    "record_provider_response",
    # per-provider / per-endpoint budgets
    "BUDGET_TRACKER",
    "ProviderBudgetTracker",
    "budget_available",
    "budget_slot",
    "budget_wait_seconds",
    "record_budget_dispatch",
    "record_budget_response",
    "record_budget_success",
    # archive-class routing
    "ArchiveRoutingError",
    "NoArchiveEndpointError",
    "archive_capable_endpoints",
    "is_archive_capable",
    "method_requires_archive",
    "select_archive_endpoint",
]
