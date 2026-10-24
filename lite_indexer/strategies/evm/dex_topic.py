"""EVM dex topic fetching (split from dex_handler for size policy).

Extracted 2026-09-21: chunked eth_getLogs topic fetch + its response helper.
Split 2026-10-25: per-leg endpoint rotation and the request/retry wrapper now
live in :mod:`lite_indexer.strategies.evm.dex_topic_leg`; this module keeps the
chunk fan-out and the per-leg cost log.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS, get_endpoints_for_range
from lite_indexer.chunks import CHUNK_PARALLELISM, chunk_range, merge_chunked_results
from lite_indexer.config import CHUNK_SIZE_EVM
from lite_indexer.logging import get_logger
from lite_indexer.state import PairCache
from lite_indexer.strategies.evm.dex_topic_leg import fetch_range

logger = get_logger(__name__)


async def fetch_topic(
    topic: str,
    label: str,
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    pair_cache: PairCache,
    chunk_size: int = CHUNK_SIZE_EVM,
) -> list[dict[str, Any]] | None:
    """Fetch logs for a single event topic across all endpoints.

    Splits the range into ``chunk_size``-wide chunks and fetches each
    with one ``eth_getLogs`` call, up to :data:`CHUNK_PARALLELISM` at a
    time, then merges and de-duplicates the results. One topic per call:
    the filter is ``topics: [topic]``, so a swap leg, a V2 LP leg and a
    V3 LP leg are three separate calls over the same block range.

    Args:
        topic: Event topic hash.
        label: Human label for logging ("v2", "v3", "lp_v2", ...).
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        pair_cache: Pair cache for token resolution.
        chunk_size: Max blocks per eth_getLogs chunk.

    Returns:
        List of parsed event dicts (swap or LP, per ``topic``), or
        ``None`` on total leg failure.
    """
    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        return []
    # Only query endpoints that can serve this block range (provider limits).
    endpoints = get_endpoints_for_range(endpoints, to_block - from_block)

    started = time.monotonic()
    effective_chunk = min(chunk_size, max(1, to_block - from_block + 1))
    semaphore = asyncio.Semaphore(CHUNK_PARALLELISM)

    async def _fetch_chunk(chunk_start: int, chunk_end: int) -> list[dict[str, Any]] | None:
        async with semaphore:
            return await fetch_range(topic, label, chain, endpoints, client, pair_cache, chunk_start, chunk_end)

    tasks: list[asyncio.Task[list[dict[str, Any]] | None]] = [
        asyncio.ensure_future(_fetch_chunk(chunk_start, chunk_end))
        for chunk_start, chunk_end in chunk_range(from_block, to_block, effective_chunk)
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    chunk_results: list[list[dict[str, Any]]] = []
    for outcome in outcomes:
        if not isinstance(outcome, list):
            # A failed chunk (all endpoints down, or raised) makes the topic
            # leg incomplete: report total failure so the caller defers
            # instead of silently advancing.
            return None
        chunk_results.append(outcome)

    merged = merge_chunked_results(chunk_results)
    # Per-leg cost: the swap leg binds the slice, so measure it per leg.
    logger.info(
        "dex_leg_done",
        chain=chain,
        leg=label,
        chunks=len(tasks),
        events=len(merged),
        duration_s=round(time.monotonic() - started, 2),
    )
    return merged
