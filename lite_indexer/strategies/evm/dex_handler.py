"""
DEX Swap event handler for EVM chains.

Provides fetch_swap_events() — fetches DEX Swap events (V2 + V3
Uniswap-style) via eth_getLogs. First fetches PairCreated events
from known factories to build a pair→(token0, token1) mapping,
then fetches swap events and batch-resolves unknown pools via
eth_call fallback.
"""

from __future__ import annotations

import asyncio
from typing import Any, cast

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    BURN_V2_TOPIC,
    BURN_V3_TOPIC,
    CHUNK_SIZE_EVM,
    MINT_V2_TOPIC,
    MINT_V3_TOPIC,
    SWAP_V2_TOPIC,
    SWAP_V3_TOPIC,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
)
from lite_indexer.state import PairCache
from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.deployer_handler import fetch_pair_created
from lite_indexer.strategies.evm.dex_topic import fetch_topic
from lite_indexer.strategies.pool_resolver import batch_resolve_pool_tokens

logger = get_logger(__name__)

# LP (Mint/Burn) legs, fetched alongside the swap legs. The tuple is
# (topic, label); each is one eth_getLogs pass over the same range.
# Four extra topic legs per block range: 2 swap -> 6 total.
_LP_TOPIC_LEGS: tuple[tuple[str, str], ...] = (
    (MINT_V2_TOPIC, "lp_v2_mint"),
    (BURN_V2_TOPIC, "lp_v2_burn"),
    (MINT_V3_TOPIC, "lp_v3_mint"),
    (BURN_V3_TOPIC, "lp_v3_burn"),
)


async def fetch_swap_events(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    rpc: RpcClient,
    pair_cache: PairCache,
    batch_size: int,
    chunk_size: int = CHUNK_SIZE_EVM,
) -> list[dict[str, Any]] | None:
    """Fetch DEX Swap (V2 + V3) and LP Mint/Burn events via eth_getLogs.

    First fetches PairCreated events from known factories to build a
    pair→(token0, token1) mapping, then fetches swap events plus the
    four LP legs (V2 Mint/Burn, V3 Mint/Burn) and batch-resolves
    unknown pools via eth_call fallback.

    Error handling:
    - Chain reorg: from_block > to_block returns ["__CHAIN_REORG__"]
    - All RPC errors (timeout, rate limit, malformed, empty) fall
      through to the next endpoint
    - Block range is capped by enforce_block_range

    Args:
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        rpc: RPC client instance.
        pair_cache: Pair cache for token resolution.
        batch_size: Max blocks per batch.
        chunk_size: Max blocks per eth_getLogs chunk (default: EVM chunk).

    Returns:
        List of parsed event dicts (swaps + LP legs) with token0/token1
        resolved, ["__CHAIN_REORG__"] on reorg, or ``None`` on total
        dex-leg failure (pair mapping unavailable, or any topic leg
        failed). ``None`` must NOT be treated as "no activity" — the
        caller defers.
    """
    # Chain reorg detection
    if detect_reorg(from_block, to_block):
        return cast(list[dict[str, Any]], ["__CHAIN_REORG__"])

    # Enforce chain-specific + per-provider block range limits
    from_block, to_block = enforce_block_range(
        chain,
        from_block,
        to_block,
        batch_size,
        GET_LOGS_ENDPOINTS.get(chain, []),
    )

    # Build pair→tokens mapping from PairCreated events first
    try:
        await fetch_pair_created(chain, from_block, to_block, client=client, pair_cache=pair_cache)
    except Exception as exc:
        logger.warning("pair_created_fetch_failed", chain=chain, error=str(exc)[:100])
        return None

    # Fetch V2 and V3 swaps in parallel (each topic is chunked internally)
    v2_task = fetch_topic(
        SWAP_V2_TOPIC,
        "v2",
        chain,
        from_block,
        to_block,
        client=client,
        pair_cache=pair_cache,
        chunk_size=chunk_size,
    )
    v3_task = fetch_topic(
        SWAP_V3_TOPIC,
        "v3",
        chain,
        from_block,
        to_block,
        client=client,
        pair_cache=pair_cache,
        chunk_size=chunk_size,
    )
    lp_tasks = [
        fetch_topic(
            topic,
            label,
            chain,
            from_block,
            to_block,
            client=client,
            pair_cache=pair_cache,
            chunk_size=chunk_size,
        )
        for topic, label in _LP_TOPIC_LEGS
    ]
    # One gather for every leg: a single partial-failure signal, and no
    # sequential phase where swaps land but LP legs are still in flight.
    leg_results = await asyncio.gather(v2_task, v3_task, *lp_tasks)

    # Total failure: any topic leg that could not be fetched completely.
    if any(results is None for results in leg_results):
        logger.warning(
            "dex_fetch_total_failure",
            chain=chain,
            from_block=from_block,
            to_block=to_block,
            v2_failed=leg_results[0] is None,
            v3_failed=leg_results[1] is None,
            failed_legs=sum(1 for r in leg_results if r is None),
        )
        return None
    # Merge and deduplicate
    seen_keys: set[str] = set()
    merged: list[dict[str, Any]] = []
    for results in leg_results:
        for evt in results or []:
            key = f"{evt['transactionHash']}:{evt['logIndex']}"
            if key not in seen_keys:
                seen_keys.add(key)
                merged.append(evt)

    # Batch-resolve unknown pools via eth_call fallback
    unknown_pairs: set[str] = set()
    for evt in merged:
        pair = evt.get("pairAddress", "").lower()
        if (not evt.get("token0") or not evt.get("token1")) and pair and pair not in pair_cache:
            unknown_pairs.add(pair)

    if unknown_pairs:
        resolved = await batch_resolve_pool_tokens(rpc, chain, list(unknown_pairs), pair_cache)
        if resolved:
            for evt in merged:
                pair = evt.get("pairAddress", "").lower()
                if pair in resolved:
                    evt["token0"] = resolved[pair][0]
                    evt["token1"] = resolved[pair][1]

    return merged


__all__ = [
    "fetch_swap_events",
]
