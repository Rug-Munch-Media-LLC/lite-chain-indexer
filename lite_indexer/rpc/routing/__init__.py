"""Intelligent RPC routing — per-method routing, cost tracking, circuit breakers.

Three capabilities layered on top of the existing endpoint pool:

1. **Per-method routing** — heavy, throughput-oriented calls
   (``eth_getLogs``) prefer free public tiers; small, latency-sensitive
   calls (``eth_getTransactionReceipt``, ``eth_call``) prefer dedicated
   (API-key) nodes. :func:`select_endpoint_for_method` re-ranks the
   pool accordingly.

2. **Cost tracking** — an in-memory ledger records which endpoints are
   actually serving data vs. returning errors/rate-limits, keyed by
   method, so operators can see where free-tier budget is being spent.
   :func:`log_rpc_usage` emits a periodic summary.

3. **Per-endpoint circuit breaker** — unlike the existing failure
   backoff (which only penalizes), this breaker *opens* an endpoint
   after a burst of failures (skipping it entirely), half-opens after
   a cooldown to probe, and closes again on success. Integrates with
   :func:`lite_indexer.rpc.pool.get_best_endpoint` scoring.

All state is in-process and safe under asyncio (single-threaded event
loop, no locks required).

This is a package facade over ``selector`` / ``health`` / ``balancer``
/ ``helpers`` sub-modules; every public name is re-exported here so
``from lite_indexer.rpc.routing import X`` keeps working.

Patch/reload compatibility notes (tests rely on these):
- ``ERROR_RATE_DAMPING`` is computed HERE (not in ``balancer.py``) so
  that ``importlib.reload(lite_indexer.rpc.routing)`` re-reads the
  ``RPC_ERROR_RATE_DAMPING`` env override, and so monkeypatching
  ``routing.ERROR_RATE_DAMPING`` affects ``EndpointLatency.damped_score``
  (which reads it through this package at call time).
- ``time`` and ``random`` are imported here so ``routing.time`` /
  ``routing.random`` remain valid monkeypatch targets.
"""

from __future__ import annotations

import os
import random  # noqa: F401  (patch target: routing.random)
import time  # noqa: F401  (patch target: routing.time)

from lite_indexer.rpc.routing.balancer import (
    select_p2c,
    select_p2c_healthy,
)
from lite_indexer.rpc.routing.health import (
    CIRCUIT_BREAKER,
    ERROR_WINDOW_SECONDS,
    EWMA_ALPHA,
    EWMA_INITIAL_MS,
    STALE_ENDPOINT_PENALTY,
    STALENESS_MAX_BLOCKS,
    EndpointLatency,
    RpcCircuitBreaker,
    _stats,  # noqa: F401  (test patch target: routing._stats)
    decr_in_flight,
    endpoint_block_height,
    endpoint_error_rate,
    endpoint_ewma_ms,
    in_flight_count,
    incr_in_flight,
    is_circuit_open,
    is_endpoint_stale,
    latency_snapshot,
    record_endpoint_block_height,
    record_rpc_call,
    record_rpc_latency,
    record_rpc_outcome,
    reset_latency_state,
    stale_endpoints,
    staleness_blocks,
    track_in_flight,
)
from lite_indexer.rpc.routing.helpers import (
    USAGE_TRACKER,
    EndpointUsage,
    RpcUsageTracker,
    log_rpc_usage,
    record_rpc_usage,
)
from lite_indexer.rpc.routing.selector import (
    MIN_ARCHIVE_ENDPOINTS,
    endpoint_tier,
    method_class,
    preferred_tier_for_method,
    select_archive_endpoint,
    select_endpoint_for_method,
)

# Success-rate damping for the P2C score: an endpoint's damped latency is
# multiplied by ``(1 + ERROR_RATE_DAMPING * error_rate)`` so endpoints with
# poor recent success ratios lose selections even between hard failures.
# At the default 2.0: 30% errors -> 1.6x score, 100% errors -> 3x. Bounded,
# smooth, and derived from the already-tracked windowed outcomes.
# Computed in this package (see module docstring) — consumed by
# ``health.EndpointLatency.damped_score`` via a call-time package lookup.
ERROR_RATE_DAMPING = float(os.getenv("RPC_ERROR_RATE_DAMPING", "2.0"))

__all__ = [
    "RpcUsageTracker",
    "RpcCircuitBreaker",
    "USAGE_TRACKER",
    "CIRCUIT_BREAKER",
    "EndpointLatency",
    "EndpointUsage",
    "EWMA_ALPHA",
    "EWMA_INITIAL_MS",
    "ERROR_WINDOW_SECONDS",
    "STALENESS_MAX_BLOCKS",
    "STALE_ENDPOINT_PENALTY",
    "ERROR_RATE_DAMPING",
    "MIN_ARCHIVE_ENDPOINTS",
    "endpoint_tier",
    "method_class",
    "preferred_tier_for_method",
    "select_archive_endpoint",
    "select_endpoint_for_method",
    "record_rpc_usage",
    "log_rpc_usage",
    "is_circuit_open",
    "record_rpc_latency",
    "record_rpc_outcome",
    "record_rpc_call",
    "endpoint_ewma_ms",
    "endpoint_error_rate",
    "incr_in_flight",
    "decr_in_flight",
    "in_flight_count",
    "track_in_flight",
    "record_endpoint_block_height",
    "endpoint_block_height",
    "staleness_blocks",
    "is_endpoint_stale",
    "stale_endpoints",
    "select_p2c",
    "select_p2c_healthy",
    "latency_snapshot",
    "reset_latency_state",
]
