"""
Pool token resolver — batch eth_call fallback for unknown pair token0/token1.

When swap events reference an unknown pool address (no PairCreated event
recorded in the cache), this module provides an eth_call fallback to
resolve token0/token1 by calling the pool contract directly.

Usage:
    from lite_indexer.strategies.pool_resolver import resolve_pool_tokens

    resolved = await resolve_pool_tokens(rpc, chain, pair_addresses, pair_cache)
"""

from __future__ import annotations

import asyncio
from typing import Any

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import MAX_CONCURRENT_POOL_RESOLVE, TOKEN0_SELECTOR, TOKEN1_SELECTOR
from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcClient, get_best_endpoint
from lite_indexer.state import PairCache

logger = get_logger(__name__)


async def resolve_single_pool(
    rpc: RpcClient,
    chain: str,
    pair_address: str,
) -> tuple[str, str]:
    """Resolve token0/token1 for a single pool address via eth_call.

    Args:
        rpc: RPC client instance.
        chain: Chain name.
        pair_address: Pool contract address.

    Returns:
        (token0_address, token1_address) tuple. Empty strings on failure.
    """
    try:
        endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
        if not endpoints:
            return ("", "")
        endpoint = get_best_endpoint(endpoints)

        resp0 = await rpc.eth_call(endpoint, pair_address, TOKEN0_SELECTOR)
        resp1 = await rpc.eth_call(endpoint, pair_address, TOKEN1_SELECTOR)

        token0 = ""
        token1 = ""
        if resp0 and isinstance(resp0, str) and len(resp0) >= 42:
            token0 = "0x" + resp0[-40:].lower()
        if resp1 and isinstance(resp1, str) and len(resp1) >= 42:
            token1 = "0x" + resp1[-40:].lower()

        if token0 and token1:
            return (token0, token1)
    except Exception as exc:
        logger.debug("pool_resolve_failed", pair=pair_address[:20], error=str(exc)[:60])

    return ("", "")


async def batch_resolve_pool_tokens(
    rpc: RpcClient,
    chain: str,
    pair_addresses: list[str],
    pair_cache: PairCache | None = None,
) -> dict[str, tuple[str, str]]:
    """Batch-resolve token0/token1 for unknown pool addresses via eth_call.

    Resolves multiple pool addresses concurrently (limited by
    MAX_CONCURRENT_POOL_RESOLVE). Updates the pair cache with results.

    Args:
        rpc: RPC client instance.
        chain: Chain name.
        pair_addresses: List of pool addresses to resolve.
        pair_cache: Optional pair cache to update with resolved tokens.

    Returns:
        Dict mapping pair_address -> (token0, token1) for resolved pools.
    """
    if not pair_addresses:
        return {}

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_POOL_RESOLVE)
    resolved: dict[str, tuple[str, str]] = {}

    async def _resolve_one(pair: str) -> tuple[str, str, str]:
        async with semaphore:
            t0, t1 = await resolve_single_pool(rpc, chain, pair)
            return (pair, t0, t1)

    tasks = [_resolve_one(p) for p in pair_addresses]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    for outcome in outcomes:
        if isinstance(outcome, tuple) and len(outcome) == 3:
            pair, t0, t1 = outcome
            if t0 and t1:
                resolved[pair] = (t0, t1)
                if pair_cache is not None:
                    pair_cache.set(pair, (t0, t1))

    if resolved and pair_cache is not None:
        pair_cache.save()

    return resolved


__all__ = [
    "batch_resolve_pool_tokens",
    "resolve_single_pool",
]
