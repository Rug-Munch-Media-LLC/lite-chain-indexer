"""
RPC client — direct JSON-RPC HTTP calls for blockchain data.

Provides the RpcClient class for making eth_getLogs, eth_blockNumber,
eth_getBlockByNumber, and eth_call requests. Also includes the
fetch_block_timestamps utility function.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx

from lite_indexer.config import (
    GET_LOGS_SEMAPHORE_LIMIT,
    RPC_CALL_TIMEOUT,
    RPC_TIMEOUT,
    TIMESTAMP_FETCH_TIMEOUT,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc.pool import get_best_endpoint, record_endpoint_failure
from lite_indexer.rpc.ratelimit import apply_provider_rate_limit

logger = get_logger(__name__)

# ── Metrics ────────────────────────────────────────────────────────────
_metrics: dict[str, int] = {
    "rpc_errors": 0,
}


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

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method (e.g., 'eth_blockNumber').
            params: Method parameters.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(endpoint, json=payload)

                # Empty response check
                if not resp.text or len(resp.text) < 5:
                    logger.debug("rpc_empty_response", endpoint=endpoint[:50], method=method)
                    return None

                resp.raise_for_status()

                # Malformed JSON protection
                try:
                    return resp.json()
                except (json.JSONDecodeError, ValueError) as exc:
                    logger.debug(
                        "rpc_malformed_json",
                        endpoint=endpoint[:50],
                        method=method,
                        error=str(exc)[:60],
                    )
                    return None

        except httpx.TimeoutException:
            logger.debug("rpc_timeout", endpoint=endpoint[:50], method=method)
            return None
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError) as exc:
            logger.debug("rpc_connection_error", endpoint=endpoint[:50], method=method, error=str(exc)[:60])
            return None
        except httpx.HTTPStatusError as exc:
            logger.debug("rpc_http_error", endpoint=endpoint[:50], method=method, status=exc.response.status_code)
            return None
        except Exception as exc:
            logger.debug("rpc_call_failed", endpoint=endpoint[:50], method=method, error=str(exc)[:80])
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
            return result["result"]
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

        for attempt in range(max_retries):
            # Apply provider rate limiting before request
            await apply_provider_rate_limit(endpoint)

            try:
                client = await self._get_client()
                resp = await client.post(endpoint, json=payload, timeout=RPC_TIMEOUT)

                if resp.status_code == 429:
                    wait = 2**attempt
                    logger.debug(
                        "rate_limited_retry",
                        endpoint=endpoint[:40],
                        attempt=attempt,
                        wait=wait,
                    )
                    record_endpoint_failure(endpoint)
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
                _metrics["rpc_errors"] += 1
                if attempt < max_retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
            except Exception as exc:
                logger.debug(
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
) -> dict[int, str]:
    """Fetch timestamps for a set of block numbers in parallel.

    Falls back to approximate timestamps if RPC fails.

    Args:
        client: RpcClient instance.
        endpoints: List of RPC endpoints for the chain.
        block_numbers: Set of block numbers to resolve.

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

                    return block_num, datetime.fromtimestamp(ts_int, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                pass
            return block_num, None

    tasks = [_get_ts(bn) for bn in batch]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for res in results:
        if isinstance(res, tuple) and res[1] is not None:
            timestamps[res[0]] = res[1]

    # Fallback for missing timestamps
    if len(timestamps) < len(block_numbers):
        from datetime import datetime, timedelta

        avg_block_time = 12
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
