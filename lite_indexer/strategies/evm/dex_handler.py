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
import json
from typing import Any, cast

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS, get_endpoints_for_range
from lite_indexer.chunks import CHUNK_PARALLELISM, chunk_range, merge_chunked_results
from lite_indexer.config import CHUNK_SIZE_EVM, RPC_TIMEOUT, SWAP_V2_TOPIC, SWAP_V3_TOPIC
from lite_indexer.dex import parse_swap_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    apply_endpoint_backoff,
    ban_endpoint,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)
from lite_indexer.state import PairCache
from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.deployer_handler import fetch_pair_created
from lite_indexer.strategies.pool_resolver import batch_resolve_pool_tokens

logger = get_logger(__name__)


async def _fetch_topic(
    topic: str,
    label: str,
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    pair_cache: PairCache,
    chunk_size: int = CHUNK_SIZE_EVM,
) -> list[dict[str, Any]]:
    """Fetch logs for a single event topic across all endpoints.

    Splits the range into ``chunk_size``-wide chunks and fetches each
    with one ``eth_getLogs`` call, up to :data:`CHUNK_PARALLELISM` at a
    time, then merges and de-duplicates the results.

    Args:
        topic: Event topic hash.
        label: Human label for logging ("v2" or "v3").
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        pair_cache: Pair cache for token resolution.
        chunk_size: Max blocks per eth_getLogs chunk.

    Returns:
        List of parsed swap event dicts.
    """
    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        return []

    # Only query endpoints that can serve this block range (provider limits).
    endpoints = get_endpoints_for_range(endpoints, to_block - from_block)

    async def _fetch_one_range(range_start: int, range_end: int) -> list[dict[str, Any]]:
        filter_params = {
            "fromBlock": hex(range_start),
            "toBlock": hex(range_end),
            "topics": [topic],
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter_params],
        }

        start_idx = endpoints.index(get_best_endpoint(endpoints))
        rotated = endpoints[start_idx:] + endpoints[:start_idx]

        best_result: list[dict[str, Any]] | None = None
        # Hard deadline per endpoint attempt (incident 2026-08-24): a
        # trickling getLogs response body defeated httpx's per-read
        # timeout and wedged the ethereum dex pipeline for ~20h inside
        # resp.text streaming. wait_for makes the ENTIRE attempt —
        # connect + headers + full body read — bounded, no exceptions.
        attempt_deadline = RPC_TIMEOUT * 2 + 10
        for url in rotated:
            await apply_endpoint_backoff(url)
            try:
                resp = await asyncio.wait_for(
                    client.post(url, json=body, timeout=RPC_TIMEOUT),
                    timeout=attempt_deadline,
                )
                if resp.status_code == 200:
                    # Body read under the same hard deadline — this is
                    # exactly where the wedge lived.
                    resp_text = await asyncio.wait_for(
                        asyncio.to_thread(lambda r=resp: r.text), timeout=attempt_deadline
                    )
                    if not resp_text or len(resp_text) < 10:
                        record_endpoint_failure(url)
                        continue

                    try:
                        data = json.loads(resp_text)
                    except (json.JSONDecodeError, ValueError):
                        record_endpoint_failure(url)
                        continue

                    if isinstance(data, dict) and "result" in data:
                        logs = data["result"]
                        if isinstance(logs, list):
                            parsed = parse_swap_logs(
                                logs,
                                chain,
                                range_start,
                                range_end,
                                topic,
                                pair_cache.all,
                            )
                            if best_result is None or len(parsed) > len(best_result):
                                best_result = parsed
                            # A 200 with a result list means this endpoint
                            # served the range in full; all endpoints serve
                            # identical chain data. The old guard
                            # ``len(parsed) == len(logs)`` was unreachable
                            # because parse_swap_logs filters non-Swap logs,
                            # so every chunk queried ALL endpoints (5×
                            # RPC cost, 5× latency on busy chains).
                            break
                elif resp.status_code == 429:
                    await handle_rate_limit(url)
                    continue
                else:
                    record_endpoint_failure(url)
                    continue
            except TimeoutError:
                # Hard deadline blown: wedged/slow-loris endpoint. Ban it so
                # no other chunk task lands here again this cycle.
                ban_endpoint(url, reason="wedged_response_deadline")
                logger.warning("dex_fetch_wedged_endpoint_banned", endpoint=url[:60], deadline_s=attempt_deadline)
                continue
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
                record_endpoint_failure(url)
                continue
            except (httpx.RemoteProtocolError, httpx.LocalProtocolError):
                record_endpoint_failure(url)
                continue
            except (httpx.HTTPError, ConnectionError, OSError, ValueError, KeyError, TypeError) as exc:
                logger.warning("dex_fetch_unexpected", chain=chain, endpoint=url[:50], error=str(exc)[:80])
                record_endpoint_failure(url)
                continue

        return best_result or []

    effective_chunk = min(chunk_size, max(1, to_block - from_block + 1))
    semaphore = asyncio.Semaphore(CHUNK_PARALLELISM)

    async def _fetch_chunk(chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        async with semaphore:
            return await _fetch_one_range(chunk_start, chunk_end)

    tasks: list[asyncio.Task[list[dict[str, Any]]]] = []
    for chunk_start, chunk_end in chunk_range(from_block, to_block, effective_chunk):
        tasks.append(asyncio.ensure_future(_fetch_chunk(chunk_start, chunk_end)))

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    chunk_results: list[list[dict[str, Any]]] = []
    for outcome in outcomes:
        if isinstance(outcome, list):
            chunk_results.append(outcome)

    return merge_chunked_results(chunk_results)


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
) -> list[dict[str, Any]]:
    """Fetch DEX Swap events (V2 + V3) via eth_getLogs.

    First fetches PairCreated events from known factories to build a
    pair→(token0, token1) mapping, then fetches swap events.

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
        List of parsed swap event dicts with token0/token1 resolved,
        or ["__CHAIN_REORG__"] if a chain reorg was detected.
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
        return []

    # Fetch V2 and V3 swaps in parallel (each topic is chunked internally)
    v2_task = _fetch_topic(
        SWAP_V2_TOPIC,
        "v2",
        chain,
        from_block,
        to_block,
        client=client,
        pair_cache=pair_cache,
        chunk_size=chunk_size,
    )
    v3_task = _fetch_topic(
        SWAP_V3_TOPIC,
        "v3",
        chain,
        from_block,
        to_block,
        client=client,
        pair_cache=pair_cache,
        chunk_size=chunk_size,
    )
    v2_results, v3_results = await asyncio.gather(v2_task, v3_task)

    # Merge and deduplicate
    seen_keys: set[str] = set()
    merged: list[dict[str, Any]] = []
    for evt in v2_results + v3_results:
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
