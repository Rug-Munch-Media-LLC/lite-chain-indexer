"""
RPC client — direct JSON-RPC HTTP calls for blockchain data.

Provides the RpcClient class for making eth_getLogs, eth_blockNumber,
eth_getBlockByNumber, and eth_call requests. Also includes the
fetch_block_timestamps utility function.

Split into cohesive modules (2026-09-22, shrink-only ratchet); this
module is the public facade — every historically importable name is
re-exported here, and the implementation resolves patchable
collaborators through this facade so monkeypatched attributes keep
working.
"""

from __future__ import annotations

import asyncio  # noqa: F401  (module attribute used by tests)
import contextlib  # noqa: F401  (module attribute preserved)
import json  # noqa: F401  (module attribute preserved)
import time  # noqa: F401  (module attribute preserved)
from typing import Any, cast  # noqa: F401  (module attributes preserved)

import httpx  # noqa: F401  (module attribute used by tests)

from lite_indexer.config import (
    CHAIN_BLOCK_TIMES,  # noqa: F401  (re-exported)
    DEFAULT_BLOCK_TIME,  # noqa: F401  (re-exported)
    GET_LOGS_SEMAPHORE_LIMIT,  # noqa: F401  (re-exported)
    RATE_LIMIT_WAIT_CAP,  # noqa: F401  (re-exported)
    RPC_BATCH_ENABLED,  # noqa: F401  (re-exported; tests patch it here)
    RPC_BATCH_MAX,  # noqa: F401  (re-exported; tests patch it here)
    RPC_CALL_CLIENTS_MAX,  # noqa: F401  (re-exported; rpc.client_pool reads it at runtime)
    RPC_CALL_TIMEOUT,  # noqa: F401  (re-exported)
    RPC_RETRY_ATTEMPTS,  # noqa: F401  (re-exported)
    RPC_RETRY_BASE_DELAY_S,  # noqa: F401  (re-exported)
    RPC_RETRY_MAX_DELAY_S,  # noqa: F401  (re-exported)
    RPC_TIMEOUT,  # noqa: F401  (re-exported)
    TIMESTAMP_FETCH_TIMEOUT,  # noqa: F401  (re-exported)
    TIMESTAMP_PHASE_DEADLINE_S,  # noqa: F401  (re-exported)
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr  # noqa: F401  (re-exported)
from lite_indexer.rpc import proxy as rpc_proxy  # noqa: F401  (re-exported)
from lite_indexer.rpc.budgets import (  # noqa: F401  (re-exported)
    budget_slot,
    budget_wait_seconds,
    record_budget_dispatch,
    record_budget_response,
    record_budget_success,
)
from lite_indexer.rpc.client_batch import BatchMixin
from lite_indexer.rpc.client_blocks import BlocksMixin
from lite_indexer.rpc.client_call import CallMixin
from lite_indexer.rpc.client_logrange import LogRangeMixin
from lite_indexer.rpc.client_logs import LogsMixin
from lite_indexer.rpc.client_pool import (  # noqa: F401  (re-export for import stability)
    _call_clients,
    _drop_call_client,
    _get_call_client,
    _is_closed_client_error,
    aclose_call_clients,
)
from lite_indexer.rpc.client_timestamps import (
    _TS_SAMPLE_MAX_FETCHES,  # noqa: F401  (re-exported)
    fetch_block_timestamps,
)
from lite_indexer.rpc.client_transport import (  # noqa: F401  (re-exported)
    TransportMixin,
    _budget_before,
    _post_direct,
    _post_json,
    _safe_record,
)
from lite_indexer.rpc.defer_gate import _defer_reason_for, _warn_deferred  # noqa: F401  (re-exported)
from lite_indexer.rpc.jsonparse import decode_response_async  # noqa: F401  (re-exported)
from lite_indexer.rpc.ledger import record_endpoint_call  # noqa: F401  (re-exported)
from lite_indexer.rpc.pool import (  # noqa: F401  (re-exported)
    ban_endpoint,
    get_best_endpoint,
    record_endpoint_failure,
    record_endpoint_success,
    record_rate_limit,
)
from lite_indexer.rpc.provider_caps import (  # noqa: F401  (re-exported)
    account_key_for_endpoint,
    provider_concurrency_slot,
    provider_throttle_before,
    record_provider_response,
)
from lite_indexer.rpc.ratelimit import apply_provider_rate_limit  # noqa: F401  (re-exported)
from lite_indexer.rpc.routing import (  # noqa: F401  (re-exported)
    decr_in_flight,
    incr_in_flight,
    record_endpoint_block_height,
    record_endpoint_tip_failure,
    record_rpc_latency,
    record_rpc_outcome,
    record_rpc_usage,
)
from lite_indexer.rpc.ts_cache import (  # noqa: F401  (public re-export)
    _ts_cache_get,
    _ts_cache_put,
    clear_timestamp_cache,
)
from lite_indexer.util.http import http_limits  # noqa: F401  (re-exported)

logger = get_logger(__name__)


class RpcClient(
    TransportMixin,
    CallMixin,
    BatchMixin,
    BlocksMixin,
    LogsMixin,
    LogRangeMixin,
):
    """Direct RPC client for blockchain calls.

    Replaces the backend RpcRouter with a standalone implementation
    that makes direct HTTP POST calls to RPC endpoints. The method
    bodies live in the ``client_*`` sibling modules (transport, call,
    batch, blocks, logs, log-range, timestamps); this class composes
    them and preserves the historical import surface.
    """


__all__ = [
    "RpcClient",
    "fetch_block_timestamps",
    "clear_timestamp_cache",
    "_TS_SAMPLE_MAX_FETCHES",
    "_safe_record",
    "_budget_before",
    "_post_direct",
    "_post_json",
]
