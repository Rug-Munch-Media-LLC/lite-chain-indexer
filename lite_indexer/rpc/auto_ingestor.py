"""
Auto RPC Ingestor — continuously discovers and validates new free RPC endpoints.

Features:
1. Periodic discovery from Chainlist (chainid.network/chains.json)
2. Validation with eth_blockNumber + eth_getLogs probes
3. Auto-add healthy endpoints to the in-memory pool
4. Health monitoring and automatic removal of dead endpoints
5. Metrics and logging for discovered endpoints

Runs as a background task in the daemon or as a standalone CLI command.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc.gates import provider_gates

logger = get_logger(__name__)

# Default discovery interval in seconds
DEFAULT_DISCOVERY_INTERVAL = 3600  # 1 hour

# Max endpoints to validate per cycle
MAX_VALIDATION_PER_CYCLE = 50


async def discover_chain_rpcs(
    chain_id: int,
    *,
    timeout: float = 10.0,
) -> list[str]:
    """Discover free RPC URLs from Chainlist for a specific chain.

    Args:
        chain_id: EVM chain ID.
        timeout: Request timeout.
    Returns:
        List of discovered free RPC URLs.
    """
    registry = await _fetch_registry(timeout=timeout)
    candidates = _extract_candidates(chain_id, registry)
    return candidates
async def _fetch_registry(
    url: str = "https://chainid.network/chains.json",
    timeout: float = 15.0,
) -> list[dict[str, Any]]:
    """Fetch and parse the chain registry."""
    import json

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, list):
            return data
    except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("auto_ingest_fetch_failed", error=str(exc)[:80])
    return []


def _extract_candidates(chain_id: int, registry: list[Any]) -> list[str]:
    """Extract free, keyless RPC URLs for a chain from the registry."""
    from urllib.parse import urlparse

    NON_FREE_MARKERS = ("${", "api-key", "api_key", "apikey", "key=", "token=")  # noqa: N806
    candidates: list[str] = []
    seen: set[str] = set()

    for entry in registry:
        if not isinstance(entry, dict):
            continue
        if entry.get("chainId") != chain_id:
            continue
        rpc_list = entry.get("rpc")
        if not isinstance(rpc_list, list):
            continue

        for rpc_url in rpc_list:
            if not isinstance(rpc_url, str) or not rpc_url:
                continue
            lowered = rpc_url.lower()
            if any(marker in lowered for marker in NON_FREE_MARKERS):
                continue
            parsed = urlparse(rpc_url)
            if parsed.scheme not in ("https", "http"):
                continue
            if rpc_url in seen:
                continue
            seen.add(rpc_url)
            candidates.append(rpc_url)

    return candidates


async def validate_endpoint(
    url: str,
    *,
    timeout: float = 10.0,
) -> bool:
    """Validate an RPC endpoint with eth_blockNumber and eth_getLogs probes.

    Args:
        url: RPC endpoint URL.
        timeout: Probe timeout.

    Returns:
        True if the endpoint is healthy and supports eth_getLogs.
    """
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            # Gate 1: eth_blockNumber (provider gates first).
            await provider_gates(url, "eth_blockNumber")
            resp = await client.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
            )
            if resp.status_code != 200:
                return False
            data = resp.json()
            if not (isinstance(data, dict) and bool(data.get("result"))):
                return False

            # Gate 2: eth_getLogs capability
            tip_hex = data.get("result", "0x0")
            try:
                tip = int(tip_hex, 16)
            except (TypeError, ValueError):
                return False
            from_block = hex(max(0, tip - 2))
            await provider_gates(url, "eth_getLogs")
            gl = await client.post(
                url,
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "eth_getLogs",
                    "params": [{"fromBlock": from_block, "toBlock": "latest"}],
                },
            )
            if gl.status_code != 200:
                return False
            gl_data = gl.json()
            return isinstance(gl_data, dict) and isinstance(gl_data.get("result"), list)

    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError):
        return False


async def validate_endpoints(
    urls: list[str],
    *,
    timeout: float = 10.0,
    concurrency: int = 5,
) -> list[str]:
    """Validate multiple endpoints in parallel.

    Args:
        urls: List of RPC URLs to validate.
        timeout: Per-endpoint timeout.
        concurrency: Max parallel validations.

    Returns:
        List of healthy endpoint URLs.
    """
    semaphore = asyncio.Semaphore(concurrency)

    async def _validate(url: str) -> tuple[str, bool]:
        async with semaphore:
            is_healthy = await validate_endpoint(url, timeout=timeout)
            return url, is_healthy

    tasks = [_validate(url) for url in urls[:MAX_VALIDATION_PER_CYCLE]]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    healthy: list[str] = []
    for result in results:
        if isinstance(result, tuple) and result[1] is True:
            healthy.append(result[0])

    return healthy


async def auto_ingest(
    chain: str,
    *,
    add_to_pool: bool = True,
) -> list[str]:
    """Discover and ingest new free RPC endpoints for a chain.

    Args:
        chain: Chain name (e.g., "ethereum", "base").
        add_to_pool: Whether to add healthy endpoints to the pool.

    Returns:
        List of newly discovered and validated endpoints.
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS

    # Map chain name to chain ID
    chain_id_map = {
        "ethereum": 1,
        "base": 8453,
        "bsc": 56,
        "polygon": 137,
        "arbitrum": 42161,
        "optimism": 10,
        "avalanche": 43114,
        "fantom": 250,
        "linea": 59144,
        "scroll": 534352,
        "zksync": 324,
        "mantle": 5000,
        "gnosis": 100,
        "celo": 42220,
        "hyperliquid": 999,
    }

    chain_id = chain_id_map.get(chain.lower())
    if chain_id is None:
        logger.warning("auto_ingest_unknown_chain", chain=chain)
        return []

    # Discover candidates
    candidates = await discover_chain_rpcs(chain_id)
    if not candidates:
        logger.info("auto_ingest_no_candidates", chain=chain)
        return []

    # Validate candidates
    healthy = await validate_endpoints(candidates)
    if not healthy:
        logger.info("auto_ingest_no_healthy", chain=chain, candidates=len(candidates))
        return []

    # Add to pool
    if add_to_pool:
        existing = set(GET_LOGS_ENDPOINTS.get(chain, []))
        new_endpoints = [ep for ep in healthy if ep not in existing]
        if new_endpoints:
            GET_LOGS_ENDPOINTS.setdefault(chain, []).extend(new_endpoints)
            logger.info(
                "auto_ingest_added",
                chain=chain,
                added=len(new_endpoints),
                total=len(GET_LOGS_ENDPOINTS[chain]),
            )
        return new_endpoints

    return healthy


async def monitor_endpoints(
    chain: str,
    *,
    interval: int = DEFAULT_DISCOVERY_INTERVAL,
) -> None:
    """Continuously monitor and discover new RPC endpoints.

    Runs as a long-lived background task.

    Args:
        chain: Chain to monitor.
        interval: Discovery interval in seconds.
    """
    while True:
        try:
            new_endpoints = await auto_ingest(chain)
            if new_endpoints:
                logger.info(
                    "monitor_discovered_new",
                    chain=chain,
                    new=len(new_endpoints),
                )
        except Exception as exc:
            logger.warning("monitor_error", chain=chain, error=str(exc)[:100])

        await asyncio.sleep(interval)


class RpcIngestorStats:
    """Statistics for the RPC auto-ingestor."""

    def __init__(self) -> None:
        self.total_discovered: int = 0
        self.total_validated: int = 0
        self.total_added: int = 0
        self.last_discovery_timestamp: float = 0.0
        self.last_discovery_count: int = 0

    def record_discovery(self, discovered: int, validated: int, added: int) -> None:
        """Record a discovery cycle."""
        self.total_discovered += discovered
        self.total_validated += validated
        self.total_added += added
        self.last_discovery_timestamp = time.monotonic()
        self.last_discovery_count = added

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_discovered": self.total_discovered,
            "total_validated": self.total_validated,
            "total_added": self.total_added,
            "last_discovery_timestamp": self.last_discovery_timestamp,
            "last_discovery_count": self.last_discovery_count,
        }


__all__ = [
    "discover_chain_rpcs",
    "validate_endpoint",
    "validate_endpoints",
    "auto_ingest",
    "monitor_endpoints",
    "RpcIngestorStats",
]
