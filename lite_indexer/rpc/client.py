"""
RPC client — direct JSON-RPC HTTP calls for blockchain data.

Provides the RpcClient class for making eth_getLogs, eth_blockNumber,
eth_getBlockByNumber, and eth_call requests. Also includes the
fetch_block_timestamps utility function.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import OrderedDict
from typing import Any, cast

import httpx

from lite_indexer.config import (
    CHAIN_BLOCK_TIMES,
    DEFAULT_BLOCK_TIME,
    GET_LOGS_SEMAPHORE_LIMIT,
    RATE_LIMIT_WAIT_CAP,
    RPC_CALL_CLIENTS_MAX,
    RPC_CALL_TIMEOUT,
    RPC_RETRY_ATTEMPTS,
    RPC_RETRY_BASE_DELAY_S,
    RPC_TIMEOUT,
    TIMESTAMP_FETCH_TIMEOUT,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.ledger import (
    LEDGER,
    defer_reason,
    key_id_for_endpoint,
    provider_id_for_endpoint,
    record_endpoint_call,
)
from lite_indexer.rpc.pool import (
    ban_endpoint,
    get_best_endpoint,
    record_endpoint_failure,
    record_endpoint_success,
    record_rate_limit,
)
from lite_indexer.rpc.provider_caps import (
    account_key_for_endpoint,
    provider_concurrency_slot,
    provider_throttle_before,
    record_provider_response,
)
from lite_indexer.rpc.ratelimit import apply_provider_rate_limit
from lite_indexer.rpc.routing import (
    decr_in_flight,
    incr_in_flight,
    record_endpoint_block_height,
    record_rpc_latency,
    record_rpc_outcome,
)
from lite_indexer.util.http import http_limits

logger = get_logger(__name__)

# Max eth_getBlockByNumber header fetches per timestamp-enrichment call.
# Larger sets are sampled and interpolated (see fetch_block_timestamps).
_TS_SAMPLE_MAX_FETCHES = 64


def _safe_record(recorder: Any, *args: Any, **kwargs: Any) -> None:
    """Run an additive ledger/routing telemetry call without breaking RPC.

    The quota ledger (``rpc/ledger.py``) and EWMA/staleness trackers
    (``rpc/routing.py``) are best-effort observability layers: if a
    recorder ever raises (e.g. a corrupt ledger file), the error is
    logged and the RPC result is returned untouched.
    """
    try:
        recorder(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 — telemetry must never raise into the RPC path
        logger.debug("rpc_telemetry_skipped", error=str(exc)[:80])


# (provider_id, key_id, level) tuples already warned about — the defer gate
# logs at warning once per provider+level per process, never per call.
_defer_warned: set[tuple[str, str, int]] = set()


def _defer_reason_for(
    endpoint: str,
    method: str,
    *,
    range_blocks: int | None = None,
) -> str | None:
    """Guarded quota-degrade gate for the RPC fetch path.

    Derives the account-level provider/key id from the endpoint URL and asks
    the ledger whether this call should be deferred. Never raises: on any
    ledger error it degrades to ``None`` (allow the call) so enforcement can
    never break the RPC path.
    """
    try:
        return defer_reason(
            provider_id_for_endpoint(endpoint),
            method,
            key_id_for_endpoint(endpoint),
            range_blocks=range_blocks,
        )
    except Exception as exc:  # noqa: BLE001 — enforcement must never break RPC
        logger.debug("rpc_defer_gate_skipped", endpoint=endpoint[:50], error=str(exc)[:80])
        return None


def _warn_deferred(endpoint: str, method: str, reason: str) -> None:
    """Log a deferral at warning, once per provider+level (not spammy)."""
    provider_id = ""
    key_id = ""
    level = 0
    try:
        provider_id = provider_id_for_endpoint(endpoint)
        key_id = key_id_for_endpoint(endpoint)
        level = LEDGER.degrade_level_for(provider_id, key_id)
    except Exception as exc:  # noqa: BLE001 — dedup bookkeeping must never raise
        logger.debug("rpc_defer_warn_skipped", endpoint=endpoint[:50], error=str(exc)[:80])
    dedup_key = (provider_id, key_id, level)
    if dedup_key in _defer_warned:
        return
    _defer_warned.add(dedup_key)
    logger.warning(
        "rpc_call_deferred",
        provider=provider_id,
        key_id=key_id,
        level=level,
        method=method,
        reason=reason,
    )


# ── Pooled HTTP Clients ────────────────────────────────────────────────
# Reuse a small set of AsyncClients across all RpcClient instances instead
# of creating a brand-new httpx client per call. Clients are keyed by
# timeout so callers with different timeouts don't fight over one value.
# Safe under asyncio: creation is synchronous, so there is no await point
# between the lookup and the assignment.
#
# BOUNDED (2026-09-13): this map previously grew one client per distinct
# timeout, never evicted and never closed — the primary socket leak (the
# live process held 150 open sockets). It is now capped at
# RPC_CALL_CLIENTS_MAX with LRU eviction, and an evicted client is closed.
_call_clients: OrderedDict[float, httpx.AsyncClient] = OrderedDict()


async def _get_call_client(timeout: float) -> httpx.AsyncClient:
    """Get a pooled module-level httpx client for the given timeout.

    The pool is capped at :data:`RPC_CALL_CLIENTS_MAX`; the least-recently
    used client is evicted and closed when the cap is exceeded. Every
    client gets explicit connection limits so sockets stay bounded.
    """
    client = _call_clients.get(timeout)
    if client is not None and not client.is_closed:
        _call_clients.move_to_end(timeout)
        return client
    client = httpx.AsyncClient(timeout=httpx.Timeout(timeout), limits=http_limits())
    _call_clients[timeout] = client
    _call_clients.move_to_end(timeout)
    while len(_call_clients) > RPC_CALL_CLIENTS_MAX:
        _, evicted = _call_clients.popitem(last=False)
        with contextlib.suppress(Exception):
            await evicted.aclose()
    return client


async def aclose_call_clients() -> None:
    """Close and clear every pooled call client (shutdown / tests)."""
    clients = list(_call_clients.values())
    _call_clients.clear()
    for client in clients:
        if not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()


# ── RPC Client ─────────────────────────────────────────────────────────


class RpcClient:
    """Direct RPC client for blockchain calls.

    Replaces the backend RpcRouter with a standalone implementation
    that makes direct HTTP POST calls to RPC endpoints.
    """

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=http_limits(),
            )
        return self._client

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent)."""
        client, self._client = self._client, None
        if client is not None and not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = RPC_CALL_TIMEOUT,
    ) -> dict[str, Any] | None:
        """Make a JSON-RPC call to an endpoint.

        Records per-endpoint usage/cost in the shared RPC usage ledger
        (which endpoints actually serve data vs. error).

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method (e.g., 'eth_blockNumber').
            params: Method parameters.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        from lite_indexer.rpc.autotune import autotune_wait
        from lite_indexer.rpc.autotune import record_call as autotune_record
        from lite_indexer.rpc.capabilities import archive_guard_reason
        from lite_indexer.rpc.circuit import circuit_open, record_circuit_failure, record_circuit_success
        from lite_indexer.rpc.routing import record_rpc_usage
        from lite_indexer.rpc.throttle import record_rpc_response, throttle_before

        # Quota-degrade gate: before dispatching, ask the ledger whether
        # this call should be deferred (level 3 parks everything, level 2
        # blocks heavy methods). Guarded — a ledger error never blocks RPC.
        # Returns None (the existing failure shape) so callers skip
        # gracefully; a deferred call is never dispatched and records no
        # quota consumption.
        reason = _defer_reason_for(endpoint, method)
        if reason:
            _warn_deferred(endpoint, method, reason)
            return None

        # Archive-class fail-closed gate: a known non-archive endpoint must
        # never serve eth_getLogs/eth_getProof/debug_*/trace_* — it silently
        # returns wrong/empty data. Returns None (the existing failure shape).
        archive_reason = archive_guard_reason(endpoint, method)
        if archive_reason:
            metrics_incr("rpc_archive_fail_closed")
            logger.warning(
                "rpc_archive_fail_closed",
                endpoint=endpoint[:60],
                method=method,
                reason=archive_reason,
            )
            return None

        # Circuit breaker gate: an endpoint that tripped (5 consecutive
        # transient failures) is skipped for the open-TTL window; after
        # expiry one probe passes (half-open) and success resets it.
        if await circuit_open(endpoint):
            logger.debug("rpc_circuit_skip", endpoint=endpoint[:50], method=method)
            return None

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        # Per-account (provider) cap: queue when the provider's concurrency /
        # requests-per-second budget is exhausted, and honor the account-level
        # Retry-After window — providers ban the ACCOUNT, not one URL.
        account_key = account_key_for_endpoint(endpoint)
        # Intelligent throttle: honor Retry-After / advertised budget /
        # per-method rate BEFORE spending another request on this endpoint.
        await throttle_before(endpoint, method)
        # Provider (account) gate: honor the account-level Retry-After /
        # requests-per-second budget ONCE before the retry loop. Concurrency
        # is enforced per dispatch below; pacing once avoids re-pacing the
        # provider budget on every transient retry.
        await provider_throttle_before(endpoint)
        # Auto-tune soft spacing: a straining endpoint gets progressively
        # wider gaps instead of a hard ban (NodeCore-style adaptation).
        await autotune_wait(endpoint)
        started = time.perf_counter()

        # Exponential backoff retry for TRANSIENT failures only (timeouts,
        # connection errors, 5xx, empty responses): 0.1s → 0.2s → 0.4s,
        # max RPC_RETRY_ATTEMPTS attempts. Non-transient outcomes (429,
        # 403, malformed JSON, JSON-RPC errors) return immediately. When
        # all attempts fail, we return None — the existing failure shape —
        # and the hedging layer above keeps doing its job.
        max_attempts = RPC_RETRY_ATTEMPTS
        delay = RPC_RETRY_BASE_DELAY_S
        for attempt in range(1, max_attempts + 1):
            transient = False
            try:
                # Ledger + in-flight telemetry around the actual request.
                # record_endpoint_call feeds the quota ledger; incr/decr
                # in-flight is the guarded equivalent of track_in_flight() so
                # the P2C damped score sees concurrent load. All best-effort.
                _safe_record(record_endpoint_call, endpoint, method)
                _safe_record(incr_in_flight, endpoint)
                try:
                    async with provider_concurrency_slot(account_key):
                        client = await _get_call_client(timeout)
                        resp = await client.post(endpoint, json=payload)
                finally:
                    _safe_record(decr_in_flight, endpoint)
                _safe_record(record_rpc_latency, endpoint, (time.perf_counter() - started) * 1000.0)
                bytes_received = len(resp.content or b"")
                record_rpc_response(endpoint, method, resp.status_code, resp.headers)
                _safe_record(record_provider_response, endpoint, resp.status_code, resp.headers)

                # Empty response check — often a transient gateway glitch.
                if not resp.text or len(resp.text) < 5:
                    logger.debug("rpc_empty_response", endpoint=endpoint[:50], method=method)
                    record_rpc_usage(endpoint, method, ok=False, error="empty_response")
                    _safe_record(record_rpc_outcome, endpoint, ok=False)
                    transient = True
                else:
                    resp.raise_for_status()

                    # Malformed JSON protection
                    try:
                        data = resp.json()
                    except (json.JSONDecodeError, ValueError) as exc:
                        logger.debug(
                            "rpc_malformed_json",
                            endpoint=endpoint[:50],
                            method=method,
                            error=str(exc)[:60],
                        )
                        record_rpc_usage(
                            endpoint, method, ok=False, error="malformed_json", bytes_received=bytes_received
                        )
                        _safe_record(record_rpc_outcome, endpoint, ok=False)
                        return None

                    ok = bool("error" not in data or data.get("error") is None)
                    _safe_record(autotune_record, endpoint, ok=ok)
                    if ok:
                        record_rpc_usage(endpoint, method, ok=True, bytes_received=bytes_received)
                        # Actual successful response — close the circuit breaker for
                        # this endpoint so it doesn't stay open forever (it only ever
                        # opens via record_endpoint_failure; without this it would
                        # oscillate open↔half-open via cooldown and never re-close).
                        record_endpoint_success(endpoint)
                        await record_circuit_success(endpoint)
                    else:
                        record_rpc_usage(
                            endpoint,
                            method,
                            ok=False,
                            error=str(data.get("error", ""))[:120],
                            bytes_received=bytes_received,
                        )
                    _safe_record(record_rpc_outcome, endpoint, ok=ok)
                    return cast(dict[str, Any], data)

            except httpx.TimeoutException:
                logger.debug("rpc_timeout", endpoint=endpoint[:50], method=method)
                record_rpc_usage(endpoint, method, ok=False, error="timeout")
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                transient = True
            except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError) as exc:
                logger.debug("rpc_connection_error", endpoint=endpoint[:50], method=method, error=str(exc)[:60])
                record_rpc_usage(endpoint, method, ok=False, error="connection_error")
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                transient = True
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                logger.debug("rpc_http_error", endpoint=endpoint[:50], method=method, status=status)
                record_rpc_usage(endpoint, method, ok=False, error=f"http_{status}")
                # 429 = provider rate-limit: backoff only (recent-failure
                # penalty + exponential sleep) — do NOT trip the circuit
                # breaker or hard-ban, the throttle lifts in seconds.
                if status == 429:
                    record_rate_limit(endpoint)
                # 403 = provider blacklist: hard-ban the endpoint for an
                # exponential window so we stop hammering it.
                elif status == 403:
                    ban_endpoint(endpoint, reason="http_403")
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                # 5xx = upstream hiccup: transient, worth a retry.
                transient = status >= 500
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
                logger.warning("rpc_call_failed", endpoint=endpoint[:50], method=method, error=str(exc)[:80])
                record_rpc_usage(endpoint, method, ok=False, error=str(exc)[:120])
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                transient = True

            if transient:
                await record_circuit_failure(endpoint)
                if attempt < max_attempts:
                    logger.debug(
                        "rpc_retry",
                        endpoint=endpoint[:50],
                        method=method,
                        attempt=attempt,
                        next_delay_s=delay,
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 1.0)
        return None

    async def eth_block_number(self, endpoint: str) -> int | None:
        """Get latest block number.

        Handles:
        - Empty/malformed responses (returns None)
        - Connection errors (returns None)
        - Non-hex results (returns None)
        """
        result = await self.call(endpoint, "eth_blockNumber")
        if not result or "result" not in result:
            return None
        try:
            height = int(result["result"], 16)
        except (ValueError, TypeError) as exc:
            logger.debug("eth_block_number_non_hex", endpoint=endpoint[:50], error=str(exc)[:40])
            return None
        _safe_record(record_endpoint_block_height, endpoint, height)
        return height

    async def eth_get_block_by_number(
        self,
        endpoint: str,
        block_number: int,
        full_tx: bool = False,
    ) -> dict[str, Any] | None:
        """Get a block by its number."""
        result = await self.call(
            endpoint,
            "eth_getBlockByNumber",
            [hex(block_number), full_tx],
            timeout=TIMESTAMP_FETCH_TIMEOUT,
        )
        if result and "result" in result:
            block = cast(dict[str, Any], result["result"])
            if block:
                # Feed the staleness gate (skip null blocks — a missing
                # block would skew the median toward 0).
                _safe_record(record_endpoint_block_height, endpoint, block_number)
            return block
        return None

    async def eth_get_transaction_receipt(self, endpoint: str, tx_hash: str) -> dict[str, Any] | None:
        """Get a transaction receipt.

        Routed to dedicated (API-key) nodes by default — receipts are
        small, latency-sensitive calls better served by a dedicated node
        than by a heavily-loaded public tier. See
        :func:`lite_indexer.rpc.routing.select_endpoint_for_method`.

        Args:
            endpoint: RPC endpoint URL.
            tx_hash: 0x-prefixed transaction hash.

        Returns:
            Receipt dict, or None on failure.
        """
        result = await self.call(
            endpoint,
            "eth_getTransactionReceipt",
            [tx_hash],
            timeout=RPC_CALL_TIMEOUT,
        )
        if result and "result" in result:
            return cast(dict[str, Any], result["result"])
        return None

    async def eth_call(self, endpoint: str, to: str, data: str) -> str | None:
        """Execute an eth_call against a contract.

        Args:
            endpoint: RPC endpoint URL.
            to: Contract address.
            data: Hex-encoded call data (selector + args).

        Returns:
            Hex result string, or None on failure.
        """
        result = await self.call(
            endpoint,
            "eth_call",
            [{"to": to, "data": data}, "latest"],
        )
        if result and "result" in result:
            return str(result["result"])
        return None

    async def get_logs(
        self,
        endpoint: str,
        from_block: int,
        to_block: int,
        topic: str,
        address: str | None = None,
        max_retries: int = 3,
    ) -> list[dict[str, Any]] | None:
        """Fetch event logs via eth_getLogs with retry.

        Applies per-provider token bucket rate limiting before each request.
        Automatically falls back to smaller ranges if the endpoint complains
        about range size.

        Args:
            endpoint: RPC endpoint URL.
            from_block: Start block.
            to_block: End block.
            topic: Event topic hash.
            address: Optional contract address filter.
            max_retries: Max retry attempts on rate limits.

        Returns:
            List of raw log dicts, or None on failure.
        """
        # Quota-degrade gate for eth_getLogs: level 1 defers wide backfill
        # scans only (range > 1000 blocks), level 2 defers all getLogs,
        # level 3 (parked) defers everything. Empty list is the existing
        # "no logs in range" shape, so callers skip gracefully without
        # raising or breaking the result contract.
        reason = _defer_reason_for(endpoint, "eth_getLogs", range_blocks=to_block - from_block)
        if reason:
            _warn_deferred(endpoint, "eth_getLogs", reason)
            return []

        # Archive-class fail-closed gate: eth_getLogs is ARCHIVE-class — a
        # known non-archive endpoint silently returns wrong/empty logs, so
        # fail closed (None = failure shape) rather than query it.
        from lite_indexer.rpc.capabilities import archive_guard_reason

        archive_reason = archive_guard_reason(endpoint, "eth_getLogs")
        if archive_reason:
            metrics_incr("rpc_archive_fail_closed")
            logger.warning(
                "rpc_archive_fail_closed",
                endpoint=endpoint[:60],
                method="eth_getLogs",
                reason=archive_reason,
            )
            return None

        filter_params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": [topic],
        }
        if address:
            filter_params["address"] = address

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter_params],
        }

        from lite_indexer.rpc.throttle import record_rpc_response, throttle_before

        account_key = account_key_for_endpoint(endpoint)
        for attempt in range(max_retries):
            # Apply provider rate limiting before request
            await apply_provider_rate_limit(endpoint)
            # Intelligent throttle: Retry-After / budget / per-method pacing.
            await throttle_before(endpoint, "eth_getLogs")
            # Provider (account) gate: account-level Retry-After / rps budget.
            await provider_throttle_before(endpoint)

            try:
                # Ledger + in-flight telemetry around the request (best-effort).
                _safe_record(record_endpoint_call, endpoint, "eth_getLogs")
                started = time.perf_counter()
                _safe_record(incr_in_flight, endpoint)
                try:
                    async with provider_concurrency_slot(account_key):
                        client = await self._get_client()
                        resp = await client.post(endpoint, json=payload, timeout=RPC_TIMEOUT)
                finally:
                    _safe_record(decr_in_flight, endpoint)
                _safe_record(record_rpc_latency, endpoint, (time.perf_counter() - started) * 1000.0)
                record_rpc_response(endpoint, "eth_getLogs", resp.status_code, resp.headers)
                _safe_record(record_provider_response, endpoint, resp.status_code, resp.headers)

                if resp.status_code == 429:
                    # Respect Retry-After exactly when the endpoint tells
                    # us how long to wait; otherwise exponential backoff.
                    # Rate limits trigger backoff ONLY — they must not
                    # trip the circuit breaker or hard-ban the endpoint
                    # (the throttle lifts in seconds).
                    from lite_indexer.rpc.throttle import RATE_TRACKER

                    state = RATE_TRACKER.state_for(endpoint)
                    backoff_until = state.get("backoff_until", 0.0)
                    if backoff_until:
                        wait = max(0.0, backoff_until - time.time())
                    else:
                        wait = min(2**attempt, RATE_LIMIT_WAIT_CAP)
                    logger.debug(
                        "rate_limited_retry",
                        endpoint=endpoint[:40],
                        attempt=attempt,
                        wait=round(wait, 1),
                    )
                    record_rate_limit(endpoint)
                    _safe_record(record_rpc_outcome, endpoint, ok=False)
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict) and "result" in data:
                    result = data["result"]
                    if isinstance(result, list):
                        _safe_record(record_rpc_outcome, endpoint, ok=True)
                        return result
                    # result is null — endpoint works but no logs in range
                    if result is None:
                        _safe_record(record_rpc_outcome, endpoint, ok=True)
                        return []
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                return None

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError) as exc:
                logger.debug(
                    "rpc_connection_error",
                    endpoint=endpoint[:40],
                    error=str(exc)[:60],
                )
                record_endpoint_failure(endpoint)
                metrics_incr("rpc_errors")
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "rpc_unexpected_error",
                    endpoint=endpoint[:40],
                    error=str(exc)[:60],
                )
                _safe_record(record_rpc_outcome, endpoint, ok=False)
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None

        return None


# ── Block Timestamp Fetching ───────────────────────────────────────────


async def fetch_block_timestamps(
    client: RpcClient,
    endpoints: list[str],
    block_numbers: set[int],
    chain: str = "ethereum",
    bloom_cache: Any = None,
) -> dict[int, str]:
    """Fetch timestamps for a set of block numbers in parallel.

    Falls back to approximate timestamps if RPC fails, using the
    chain's average block time (from CHAIN_BLOCK_TIMES) instead of a
    hardcoded 12s value — fast chains like Base (2s) and Arbitrum
    (0.25s) would otherwise get badly skewed estimates.

    When a ``bloom_cache`` is provided, the ``logsBloom`` from each
    fetched block header is stored in it — this warms the TrueBlocks
    pre-filter cache for free (headers are already being fetched).

    Args:
        client: RpcClient instance.
        endpoints: List of RPC endpoints for the chain.
        block_numbers: Set of block numbers to resolve.
        chain: Chain name used to look up the average block time.
        bloom_cache: Optional :class:`BloomCache` to populate with
            header logsBloom values.

    Returns:
        Dict of {block_number: ISO-8601 timestamp string}.
    """
    if not block_numbers:
        return {}

    # 2026-09-02 catch-up fix: fetching one header PER BLOCK made deep
    # catch-up glacial — a 1000-block cycle issued 1000 eth_getBlockByNumber
    # calls just for timestamps (the dominant RPC cost, ~10x the getLogs
    # work), which is why bsc/polygon/base crawled at ~95 blocks/min and
    # dex_events sat 4-9 days behind. When the set is large, fetch a bounded
    # SAMPLE of anchor headers and interpolate the rest linearly between
    # anchors (real timestamps, accurate to block-time variance) — the same
    # approximation the avg-block-time fallback already accepted.
    batch = sorted(block_numbers)
    if len(batch) > _TS_SAMPLE_MAX_FETCHES:
        stride = -(-len(batch) // _TS_SAMPLE_MAX_FETCHES)  # ceil division
        anchors = set(batch[::stride])
        anchors.update((batch[0], batch[-1]))
        fetch_list = sorted(anchors)
    else:
        fetch_list = batch

    timestamps: dict[int, str] = {}
    semaphore = asyncio.Semaphore(GET_LOGS_SEMAPHORE_LIMIT)
    endpoint = get_best_endpoint(endpoints)

    async def _get_ts(block_num: int) -> tuple[int, str | None]:
        async with semaphore:
            try:
                block = await client.eth_get_block_by_number(endpoint, block_num)
                if block and "timestamp" in block:
                    ts_int = int(block["timestamp"], 16)
                    from datetime import UTC, datetime

                    # Warm the bloom cache from the header we already have.
                    if bloom_cache is not None and block.get("logsBloom"):
                        with contextlib.suppress(Exception):
                            await bloom_cache.set(chain, block_num, str(block["logsBloom"]))
                    return block_num, datetime.fromtimestamp(ts_int, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                logger.warning("block_ts_fetch_failed", block=block_num, error=str(exc)[:80])
            return block_num, None

    tasks = [_get_ts(bn) for bn in fetch_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, tuple) and res[1] is not None:
            timestamps[res[0]] = res[1]

    # Interpolate missing timestamps between fetched anchors (linear in
    # block number — real anchor timestamps, so accuracy tracks actual
    # block-time variance), and extrapolate outside the anchor range with
    # the chain's average block time.
    if len(timestamps) < len(block_numbers):
        from datetime import datetime, timedelta

        avg_block_time = CHAIN_BLOCK_TIMES.get(chain, DEFAULT_BLOCK_TIME)
        anchor_items = sorted((bn, ts) for bn, ts in timestamps.items())
        if anchor_items:
            parsed: list[tuple[int, datetime]] = []
            for bn, ts_str in anchor_items:
                try:
                    parsed.append((bn, datetime.fromisoformat(ts_str.replace("Z", "+00:00"))))
                except ValueError:
                    continue
            for bn in block_numbers:
                if bn in timestamps:
                    continue
                # Find surrounding anchors.
                prev = next((p for p in reversed(parsed) if p[0] <= bn), None)
                nxt = next((p for p in parsed if p[0] >= bn), None)
                if prev and nxt and nxt[0] > prev[0]:
                    frac = (bn - prev[0]) / (nxt[0] - prev[0])
                    est_ts = prev[1] + (nxt[1] - prev[1]) * frac
                elif prev:
                    offset = avg_block_time * (bn - prev[0])
                    est_ts = prev[1] + timedelta(seconds=offset)
                elif nxt:
                    offset = avg_block_time * (nxt[0] - bn)
                    est_ts = nxt[1] - timedelta(seconds=offset)
                else:
                    continue
                timestamps[bn] = est_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    return timestamps
