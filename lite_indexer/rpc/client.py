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
from typing import Any, cast

import httpx

from lite_indexer.config import (
    CHAIN_BLOCK_TIMES,
    DEFAULT_BLOCK_TIME,
    GET_LOGS_SEMAPHORE_LIMIT,
    RPC_CALL_TIMEOUT,
    RPC_TIMEOUT,
    TIMESTAMP_FETCH_TIMEOUT,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.pool import (
    ban_endpoint,
    get_best_endpoint,
    record_endpoint_failure,
    record_endpoint_success,
)
from lite_indexer.rpc.ratelimit import apply_provider_rate_limit

logger = get_logger(__name__)

# ── Pooled HTTP Clients ────────────────────────────────────────────────
# Reuse a small set of AsyncClients across all RpcClient instances instead
# of creating a brand-new httpx client per call. Clients are keyed by
# timeout so callers with different timeouts don't fight over one value.
# Safe under asyncio: creation is synchronous, so there is no await point
# between the lookup and the assignment.
_call_clients: dict[float, httpx.AsyncClient] = {}


async def _get_call_client(timeout: float) -> httpx.AsyncClient:
    """Get a pooled module-level httpx client for the given timeout."""
    client = _call_clients.get(timeout)
    if client is None or client.is_closed:
        client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))
        _call_clients[timeout] = client
    return client


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
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=limits,
            )
        return self._client

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
        from lite_indexer.rpc.routing import record_rpc_usage
        from lite_indexer.rpc.throttle import record_rpc_response, throttle_before

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        # Intelligent throttle: honor Retry-After / advertised budget /
        # per-method rate BEFORE spending another request on this endpoint.
        await throttle_before(endpoint, method)
        try:
            client = await _get_call_client(timeout)
            resp = await client.post(endpoint, json=payload)
            bytes_received = len(resp.content or b"")
            record_rpc_response(endpoint, method, resp.status_code, resp.headers)

            # Empty response check
            if not resp.text or len(resp.text) < 5:
                logger.debug("rpc_empty_response", endpoint=endpoint[:50], method=method)
                record_rpc_usage(endpoint, method, ok=False, error="empty_response")
                return None

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
                record_rpc_usage(endpoint, method, ok=False, error="malformed_json", bytes_received=bytes_received)
                return None

            ok = bool("error" not in data or data.get("error") is None)
            if ok:
                record_rpc_usage(endpoint, method, ok=True, bytes_received=bytes_received)
                # Actual successful response — close the circuit breaker for
                # this endpoint so it doesn't stay open forever (it only ever
                # opens via record_endpoint_failure; without this it would
                # oscillate open↔half-open via cooldown and never re-close).
                record_endpoint_success(endpoint)
            else:
                record_rpc_usage(
                    endpoint,
                    method,
                    ok=False,
                    error=str(data.get("error", ""))[:120],
                    bytes_received=bytes_received,
                )
            return cast(dict[str, Any], data)

        except httpx.TimeoutException:
            logger.debug("rpc_timeout", endpoint=endpoint[:50], method=method)
            record_rpc_usage(endpoint, method, ok=False, error="timeout")
            return None
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError) as exc:
            logger.debug("rpc_connection_error", endpoint=endpoint[:50], method=method, error=str(exc)[:60])
            record_rpc_usage(endpoint, method, ok=False, error="connection_error")
            return None
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            logger.debug("rpc_http_error", endpoint=endpoint[:50], method=method, status=status)
            record_rpc_usage(endpoint, method, ok=False, error=f"http_{status}")
            # 403/429 = provider rate-limit / blacklist: hard-ban the
            # endpoint for an exponential window so we stop hammering it.
            if status in (403, 429):
                ban_endpoint(endpoint, reason=f"http_{status}")
            return None
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
            logger.warning("rpc_call_failed", endpoint=endpoint[:50], method=method, error=str(exc)[:80])
            record_rpc_usage(endpoint, method, ok=False, error=str(exc)[:120])
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
            return int(result["result"], 16)
        except (ValueError, TypeError) as exc:
            logger.debug("eth_block_number_non_hex", endpoint=endpoint[:50], error=str(exc)[:40])
            return None

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
            return cast(dict[str, Any], result["result"])
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

        for attempt in range(max_retries):
            # Apply provider rate limiting before request
            await apply_provider_rate_limit(endpoint)
            # Intelligent throttle: Retry-After / budget / per-method pacing.
            await throttle_before(endpoint, "eth_getLogs")

            try:
                client = await self._get_client()
                resp = await client.post(endpoint, json=payload, timeout=RPC_TIMEOUT)
                record_rpc_response(endpoint, "eth_getLogs", resp.status_code, resp.headers)

                if resp.status_code == 429:
                    # Respect Retry-After exactly when the endpoint tells
                    # us how long to wait; otherwise exponential backoff.
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
                    record_endpoint_failure(endpoint)
                    ban_endpoint(endpoint, reason="http_429")
                    await asyncio.sleep(wait)
                    continue

                resp.raise_for_status()
                data = resp.json()
                if isinstance(data, dict) and "result" in data:
                    result = data["result"]
                    if isinstance(result, list):
                        return result
                    # result is null — endpoint works but no logs in range
                    if result is None:
                        return []
                return None

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError) as exc:
                logger.debug(
                    "rpc_connection_error",
                    endpoint=endpoint[:40],
                    error=str(exc)[:60],
                )
                record_endpoint_failure(endpoint)
                metrics_incr("rpc_errors")
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

    timestamps: dict[int, str] = {}
    batch = list(block_numbers)
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

    tasks = [_get_ts(bn) for bn in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, tuple) and res[1] is not None:
            timestamps[res[0]] = res[1]

    # Fallback for missing timestamps
    if len(timestamps) < len(block_numbers):
        from datetime import datetime, timedelta

        avg_block_time = CHAIN_BLOCK_TIMES.get(chain, DEFAULT_BLOCK_TIME)
        latest_block = max(block_numbers)
        latest_ts_str = timestamps.get(latest_block)
        if latest_ts_str:
            latest_ts = datetime.fromisoformat(latest_ts_str.replace("Z", "+00:00"))
            for bn in block_numbers:
                if bn not in timestamps:
                    offset_seconds = avg_block_time * (latest_block - bn)
                    est_ts = latest_ts - timedelta(seconds=offset_seconds)
                    timestamps[bn] = est_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    return timestamps
