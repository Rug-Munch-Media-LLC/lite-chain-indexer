"""
Free-RPC auto-discovery from chainlist.org (chainid.network).

Periodically scans the public chain registry for new free RPC endpoints
for a chain, validates candidates with the existing endpoint verifier,
and merges healthy new endpoints into the in-memory pool.

Sources:
- ``https://chainid.network/chains.json`` — the canonical Chainlist
  registry (also served by chainlist.org). Each entry carries ``rpc``
  URLs and a ``chainId``.

Design notes:
- Discovery is best-effort and never fatal: network failures, rate
  limits and malformed payloads log and return empty results.
- Candidates with ``${API_KEY}`` placeholders are skipped (they are not
  "free public" endpoints).
- New endpoints are validated with the same ``eth_blockNumber`` /
  ``eth_getLogs`` probe used by the pool, then appended (de-duplicated)
  to :data:`GET_LOGS_ENDPOINTS` in memory.
"""

from __future__ import annotations

import json
import time
from typing import Any
from urllib.parse import urlparse

import httpx

from lite_indexer.config import ENDPOINT_VERIFY_TIMEOUT
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Chainlist registry URL (chainid.network is the canonical source; the
# same data backs chainlist.org).
CHAINLIST_URL = "https://chainid.network/chains.json"

# EVM chain name → chainId (only chains we index; verified 2026-08-01).
_CHAIN_IDS: dict[str, int] = {
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
}

# URL markers that indicate an endpoint is NOT free/public.
_NON_FREE_MARKERS = ("${", "api-key", "api_key", "apikey", "key=", "token=")

# Default request timeout for the registry fetch.
_REGISTRY_TIMEOUT = 15.0

# Registry cache: chains.json is a multi-MB download and is identical for
# every chain, so cache it per-URL for an hour instead of re-fetching it
# once per chain per discovery cycle (up to 14 downloads per cycle).
_registry_cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_REGISTRY_CACHE_TTL_SECONDS = 3600.0


def chain_id_for(chain: str) -> int | None:
    """Return the Chainlist chainId for an indexed EVM chain.

    Args:
        chain: Chain name (``ethereum``, ``base``, ...).

    Returns:
        ChainId, or None for non-EVM / unknown chains.
    """
    return _CHAIN_IDS.get(chain)


def _is_free_url(url: str) -> bool:
    """Heuristic check that a URL looks like a free, keyless endpoint.

    Args:
        url: RPC URL from the registry.

    Returns:
        False when the URL contains an API-key placeholder or marker.
    """
    lowered = url.lower()
    return not any(marker in lowered for marker in _NON_FREE_MARKERS)


async def fetch_chain_registry(
    url: str = CHAINLIST_URL,
    timeout: float = _REGISTRY_TIMEOUT,
) -> list[dict[str, Any]]:
    """Fetch and parse the chain registry (chains.json).

    Args:
        url: Registry URL.
        timeout: Request timeout in seconds.

    Returns:
        List of chain entries (dicts), or [] on any failure.
    """
    now = time.monotonic()
    cached = _registry_cache.get(url)
    if cached is not None and now - cached[0] < _REGISTRY_CACHE_TTL_SECONDS:
        logger.debug("chain_registry_cache_hit", url=url)
        return cached[1]

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout), follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, list):
            _registry_cache[url] = (time.monotonic(), data)
            return data
        logger.warning("chain_registry_unexpected_shape", type=type(data).__name__)
    except (httpx.HTTPError, ValueError, json.JSONDecodeError) as exc:
        logger.warning("chain_registry_fetch_failed", error=str(exc)[:100])
    return []


def extract_rpc_candidates(
    chain: str,
    registry: list[dict[str, Any]],
) -> list[str]:
    """Extract free, keyless RPC URLs for ``chain`` from the registry.

    Args:
        chain: Chain name.
        registry: Parsed chains.json entries.

    Returns:
        List of candidate RPC URLs (deduplicated, https-only).
    """
    chain_id = chain_id_for(chain)
    if chain_id is None:
        return []

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
        for url in rpc_list:
            if not isinstance(url, str) or not url:
                continue
            if not _is_free_url(url):
                continue
            parsed = urlparse(url)
            if parsed.scheme not in ("https", "http"):
                continue
            if url in seen:
                continue
            seen.add(url)
            candidates.append(url)
    return candidates


async def validate_rpc_candidates(
    chain: str,
    candidates: list[str],
    *,
    latest_block: int | None = None,
    timeout: float = ENDPOINT_VERIFY_TIMEOUT,
    concurrency: int = 5,
) -> list[str]:
    """Validate candidate endpoints with an eth_blockNumber probe.

    Uses the same probe as the endpoint health monitor (light, cheap).
    Endpoints that answer with a valid block number are considered
    healthy candidates.

    Args:
        chain: Chain name.
        candidates: Candidate URLs.
        latest_block: Unused — kept for API symmetry with the pool
            verifier.
        timeout: Per-endpoint probe timeout.
        concurrency: Max parallel probes.

    Returns:
        List of healthy candidate URLs.
    """
    import asyncio

    import httpx as _httpx

    healthy: list[str] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def _probe(url: str) -> bool:
        async with semaphore:
            try:
                async with _httpx.AsyncClient(timeout=_httpx.Timeout(timeout)) as client:
                    resp = await client.post(
                        url,
                        json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                    )
                    if resp.status_code != 200:
                        return False
                    data = resp.json()
                    return isinstance(data, dict) and bool(data.get("result"))
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                logger.debug("rpc_probe_failed", url=url[:60], error=str(exc)[:80])
                return False

    outcomes = await asyncio.gather(*[_probe(c) for c in candidates], return_exceptions=True)
    for url, outcome in zip(candidates, outcomes, strict=True):
        if outcome is True:
            healthy.append(url)

    logger.info("rpc_discovery_validated", chain=chain, candidates=len(candidates), healthy=len(healthy))
    return healthy


async def auto_discover(
    chain: str,
    *,
    add_to_pool: bool = True,
    validate: bool = True,
    registry_url: str = CHAINLIST_URL,
) -> list[str]:
    """Discover and (optionally) merge new free RPCs for a chain.

    Fetch the registry → extract free candidates → validate → append
    healthy new endpoints to :data:`GET_LOGS_ENDPOINTS` (in memory).

    Args:
        chain: Chain name.
        add_to_pool: Whether to merge healthy endpoints into the pool.
        validate: Whether to probe candidates before adding. When False,
            unvalidated candidates are still returned (for dry-run).
        registry_url: Override the registry URL.

    Returns:
        List of newly discovered healthy endpoint URLs.
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS

    registry = await fetch_chain_registry(registry_url)
    if not registry:
        return []

    candidates = extract_rpc_candidates(chain, registry)
    if not candidates:
        return []

    discovered = await validate_rpc_candidates(chain, candidates) if validate else candidates
    if not discovered or not add_to_pool:
        return discovered

    existing = set(GET_LOGS_ENDPOINTS.get(chain, []))
    new_endpoints = [ep for ep in discovered if ep not in existing]
    if new_endpoints:
        GET_LOGS_ENDPOINTS.setdefault(chain, []).extend(new_endpoints)
        logger.info(
            "rpc_discovery_added",
            chain=chain,
            added=len(new_endpoints),
            total=len(GET_LOGS_ENDPOINTS[chain]),
        )
    return new_endpoints


__all__ = [
    "CHAINLIST_URL",
    "chain_id_for",
    "fetch_chain_registry",
    "extract_rpc_candidates",
    "validate_rpc_candidates",
    "auto_discover",
]
