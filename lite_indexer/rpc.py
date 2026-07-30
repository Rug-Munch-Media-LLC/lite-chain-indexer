"""
RPC endpoint pool — health tracking, backoff, and direct HTTP client.

Provides the RPC layer for LiteIndexer. Replaces the backend-specific
rpc_router with a standalone implementation that:
  - Tracks endpoint health with exponential backoff
  - Rotates across multiple endpoints for load distribution
  - Provides eth_getLogs, eth_blockNumber, eth_getBlockByNumber,
    and eth_call methods
  - Verifies endpoint health before adding to rotation
  - Per-provider token bucket rate limiting
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import Any

import httpx

from lite_indexer.chains import (
    ENDPOINT_VERIFY_INTERVAL,
    ENDPOINT_VERIFY_TIMEOUT,
    PROVIDER_RATE_LIMITS,
    VERIFY_BLOCK_DELTA,
    get_provider_rate_limit,
)
from lite_indexer.config import (
    BACKOFF_CAP,
    ENDPOINT_COOLDOWN,
    GET_LOGS_SEMAPHORE_LIMIT,
    MAX_FAILURES,
    RATE_LIMIT_WAIT_CAP,
    RPC_CALL_TIMEOUT,
    RPC_TIMEOUT,
    TIMESTAMP_FETCH_TIMEOUT,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# ── Endpoint Health Tracking ──────────────────────────────────────────
_endpoint_failures: dict[str, list[float]] = {}
_endpoint_last_verified: dict[str, float] = {}  # url -> last verify timestamp
_endpoint_verified_ok: dict[str, bool] = {}  # url -> last verify result
_metrics: dict[str, int] = {
    "rpc_errors": 0,
    "rpc_rate_limits": 0,
    "endpoints_verified": 0,
    "endpoints_failed_verify": 0,
}

# ── Token Bucket Rate Limiters (per provider domain pattern) ─────────
_token_buckets: dict[str, dict[str, float]] = {}  # pattern -> {tokens, last_refill}


def _get_token_bucket(endpoint: str) -> dict[str, float]:
    """Get or create a token bucket for this endpoint's provider."""
    from urllib.parse import urlparse

    domain = urlparse(endpoint).hostname or endpoint
    # Find matching provider pattern
    provider = domain
    for pattern in PROVIDER_RATE_LIMITS:
        if pattern in domain:
            provider = pattern
            break

    if provider not in _token_buckets:
        limits = get_provider_rate_limit(endpoint)
        _token_buckets[provider] = {
            "tokens": float(limits["burst"]),
            "last_refill": time.time(),
            "max_tokens": float(limits["burst"]),
            "refill_rate": float(limits["req_per_sec"]),
        }
    return _token_buckets[provider]


def _check_rate_limit(endpoint: str) -> float:
    """Check rate limit and return wait time in seconds.

    Implements a token bucket algorithm per provider.
    Returns 0 if request can proceed immediately.
    Returns >0 seconds to wait if rate limited.
    """
    bucket = _get_token_bucket(endpoint)
    now = time.time()

    # Refill tokens based on elapsed time
    elapsed = now - bucket["last_refill"]
    bucket["tokens"] = min(
        bucket["max_tokens"],
        bucket["tokens"] + elapsed * bucket["refill_rate"],
    )
    bucket["last_refill"] = now

    if bucket["tokens"] >= 1.0:
        bucket["tokens"] -= 1.0
        return 0.0

    # Need to wait for refill
    wait = (1.0 - bucket["tokens"]) / bucket["refill_rate"]
    return wait


# ── Endpoint Verification ────────────────────────────────────────────

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


async def verify_endpoint(
    endpoint: str,
    chain: str,
    latest_block: int | None = None,
) -> bool:
    """Verify that an endpoint can serve eth_getLogs before adding to rotation.

    Makes a small-range eth_getLogs call (100 blocks back from tip, or
    blocks 1-100 for chains without a tip). Returns True if the endpoint
    responds with a valid result (list or null — both are acceptable).

    Args:
        endpoint: RPC endpoint URL.
        chain: Chain name (for logging).
        latest_block: Optional latest block number. If provided, checks
            the most recent 100 blocks. Otherwise checks blocks 1-100.

    Returns:
        True if endpoint responded to eth_getLogs without error.
    """
    try:
        if latest_block:
            from_block = max(0, latest_block - VERIFY_BLOCK_DELTA)
            to_block = latest_block
        else:
            from_block = 1
            to_block = 100

        # Use a fresh client for verification
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(ENDPOINT_VERIFY_TIMEOUT),
        ) as client:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getLogs",
                "params": [
                    {
                        "fromBlock": hex(from_block),
                        "toBlock": hex(to_block),
                        "topics": [TRANSFER_TOPIC],
                    }
                ],
            }
            resp = await client.post(endpoint, json=payload)
            data = resp.json()

            if not isinstance(data, dict):
                _endpoint_verified_ok[endpoint] = False
                _metrics["endpoints_failed_verify"] += 1
                return False

            # Accept any valid result (list or null — both mean endpoint works)
            if "result" in data and not ("error" in data and data.get("error")):
                _endpoint_verified_ok[endpoint] = True
                _endpoint_last_verified[endpoint] = time.time()
                _metrics["endpoints_verified"] += 1
                return True

            if "error" in data:
                err_msg = data["error"].get("message", str(data["error"]))
                # Some endpoints restrict archive data but work for recent blocks
                if "archive" in err_msg.lower() or "range" in err_msg.lower() or "limit" in err_msg.lower():
                    logger.debug(
                        "verify_endpoint_range_limited",
                        endpoint=endpoint[:50],
                        chain=chain,
                        error=err_msg[:80],
                    )
                    # Still accept — it may work with appropriate ranges
                    _endpoint_verified_ok[endpoint] = True
                    _endpoint_last_verified[endpoint] = time.time()
                    _metrics["endpoints_verified"] += 1
                    return True

            _endpoint_verified_ok[endpoint] = False
            _metrics["endpoints_failed_verify"] += 1
            return False

    except httpx.TimeoutException:
        logger.debug("verify_endpoint_timeout", endpoint=endpoint[:50], chain=chain)
        _endpoint_verified_ok[endpoint] = False
        _metrics["endpoints_failed_verify"] += 1
        return False
    except Exception as exc:
        logger.debug(
            "verify_endpoint_failed",
            endpoint=endpoint[:50],
            chain=chain,
            error=str(exc)[:80],
        )
        _endpoint_verified_ok[endpoint] = False
        _metrics["endpoints_failed_verify"] += 1
        return False


async def verify_endpoints_for_chain(
    endpoints: list[str],
    chain: str,
    latest_block: int | None = None,
    force: bool = False,
) -> list[str]:
    """Verify all endpoints for a chain, returning only healthy ones.

    Skips endpoints that have been verified within ENDPOINT_VERIFY_INTERVAL
    unless force=True. Falls through to all endpoints if no healthy ones exist.

    Args:
        endpoints: List of RPC URLs for this chain.
        chain: Chain name.
        latest_block: Optional latest block for range-based verification.
        force: If True, re-verify even recently-verified endpoints.

    Returns:
        List of verified healthy endpoints (maintains original order).
    """
    now = time.time()
    verified: list[str] = []

    for ep in endpoints:
        # Skip if recently verified and not forced
        last_v = _endpoint_last_verified.get(ep, 0)
        if not force and (now - last_v) < ENDPOINT_VERIFY_INTERVAL:
            if _endpoint_verified_ok.get(ep, False):
                verified.append(ep)
            continue

        ok = await verify_endpoint(ep, chain, latest_block)
        if ok:
            verified.append(ep)

    # Fall through: if no endpoints passed, return all originals
    if not verified:
        logger.warning(
            "all_endpoints_failed_verify",
            chain=chain,
            count=len(endpoints),
        )
        return list(endpoints)

    return verified


def is_endpoint_healthy(endpoint: str) -> bool:
    """Check if an endpoint passed its last verification."""
    result = _endpoint_verified_ok.get(endpoint)
    if result is None:
        return True  # Not yet verified — assume healthy
    return result


# ── Best Endpoint Selection ──────────────────────────────────────────


def get_best_endpoint(endpoints: list[str]) -> str:
    """Return the healthiest endpoint with load-balanced rotation.

    Scoring:
      - Failed verification: +10000 penalty (worst)
      - Each recent failure adds 1000 penalty
      - Original list position adds 1..N penalty (prefers earlier tiers)
      - Bottom 30% of healthy list is ignored for rotation diversity
      - Top 2 healthy endpoints are chosen randomly for load distribution

    Fallthrough: if all endpoints are in cooldown, clear and retry the first.
    """
    now = time.time()

    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        _endpoint_failures[ep] = failures

        penalty = len(failures) * 1000 + i

        # Add verification penalty (unverified endpoints deprioritized)
        if not is_endpoint_healthy(ep):
            penalty += 10000

        scored.append((penalty, ep))

    scored.sort(key=lambda x: x[0])

    healthy = [ep for penalty, ep in scored if penalty < MAX_FAILURES * 1000]

    if not healthy:
        for ep in endpoints:
            _endpoint_failures[ep] = []
        logger.warning("all_endpoints_cooldown_reset", count=len(endpoints))
        return endpoints[0]

    pool_size = max(2, len(healthy) // 2)
    return random.choice(healthy[:pool_size])  # noqa: S311


def record_endpoint_failure(endpoint: str) -> None:
    """Record a failure for this endpoint."""
    _endpoint_failures.setdefault(endpoint, []).append(time.time())


async def apply_endpoint_backoff(url: str) -> None:
    """Apply exponential backoff if this endpoint has recent failures."""
    recent_failures = [t for t in _endpoint_failures.get(url, []) if time.time() - t < ENDPOINT_COOLDOWN]
    if recent_failures:
        backoff = 2 ** len(recent_failures)
        await asyncio.sleep(min(backoff, BACKOFF_CAP))


async def handle_rate_limit(url: str) -> None:
    """Record a rate-limit failure and sleep with exponential backoff."""
    record_endpoint_failure(url)
    _metrics["rpc_rate_limits"] += 1
    wait = min(2 ** len(_endpoint_failures.get(url, [])), RATE_LIMIT_WAIT_CAP)
    logger.debug("rpc_rate_limited", url=url[:50], backoff=wait)
    await asyncio.sleep(wait)


async def apply_provider_rate_limit(endpoint: str) -> None:
    """Apply token-bucket rate limiting for this endpoint's provider.

    Waits if the provider's token bucket is empty. Uses per-provider
    limits from PROVIDER_RATE_LIMITS configuration.

    Args:
        endpoint: RPC URL to rate-limit.
    """
    wait = _check_rate_limit(endpoint)
    if wait > 0:
        logger.debug(
            "provider_rate_limited",
            endpoint=endpoint[:50],
            wait_seconds=round(wait, 2),
        )
        await asyncio.sleep(min(wait, RATE_LIMIT_WAIT_CAP))


# ── RPC Client ────────────────────────────────────────────────────────


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
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:
            logger.debug("rpc_call_failed", method=method, error=str(exc)[:80])
            return None

    async def eth_block_number(self, endpoint: str) -> int | None:
        """Get latest block number."""
        result = await self.call(endpoint, "eth_blockNumber")
        if result and "result" in result:
            return int(result["result"], 16)
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
