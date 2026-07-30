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
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import RPC_TIMEOUT, SWAP_V2_TOPIC, SWAP_V3_TOPIC
from lite_indexer.dex import parse_swap_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    apply_endpoint_backoff,
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
) -> list[dict[str, Any]]:
    """Fetch logs for a single event topic across all endpoints.

    Args:
        topic: Event topic hash.
        label: Human label for logging ("v2" or "v3").
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        pair_cache: Pair cache for token resolution.

    Returns:
        List of parsed swap event dicts.
    """
    filter_params = {
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [topic],
    }
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [filter_params],
    }

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        return []

    start_idx = endpoints.index(get_best_endpoint(endpoints))
    rotated = endpoints[start_idx:] + endpoints[:start_idx]

    best_result: list[dict[str, Any]] | None = None
    for url in rotated:
        await apply_endpoint_backoff(url)
        try:
            resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
            if resp.status_code == 200:
                # Empty response check
                if not resp.text or len(resp.text) < 10:
                    record_endpoint_failure(url)
                    continue

                try:
                    data = resp.json()
                except (json.JSONDecodeError, ValueError):
                    record_endpoint_failure(url)
                    continue

                if isinstance(data, dict) and "result" in data:
                    logs = data["result"]
                    if isinstance(logs, list):
                        parsed = parse_swap_logs(
                            logs,
                            chain,
                            from_block,
                            to_block,
                            topic,
                            pair_cache.all,
                        )
                        if best_result is None or len(parsed) > len(best_result):
                            best_result = parsed
                            if len(parsed) == len(logs):
                                break
            elif resp.status_code == 429:
                await handle_rate_limit(url)
                continue
            else:
                record_endpoint_failure(url)
                continue
        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
            record_endpoint_failure(url)
            continue
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError):
            record_endpoint_failure(url)
            continue
        except Exception:
            record_endpoint_failure(url)
            continue

    return best_result or []


async def fetch_swap_events(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    rpc: RpcClient,
    pair_cache: PairCache,
    batch_size: int,
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

    Returns:
        List of parsed swap event dicts with token0/token1 resolved,
        or ["__CHAIN_REORG__"] if a chain reorg was detected.
    """
    # Chain reorg detection
    if detect_reorg(from_block, to_block):
        return ["__CHAIN_REORG__"]

    # Enforce chain-specific block range
    from_block, to_block = enforce_block_range(chain, from_block, to_block, batch_size)

    # Build pair→tokens mapping from PairCreated events first
    try:
        await fetch_pair_created(chain, from_block, to_block, client=client, pair_cache=pair_cache)
    except Exception as exc:
        logger.warning("pair_created_fetch_failed", chain=chain, error=str(exc)[:100])
        return []

    # Fetch V2 and V3 swaps in parallel
    v2_task = _fetch_topic(SWAP_V2_TOPIC, "v2", chain, from_block, to_block, client=client, pair_cache=pair_cache)
    v3_task = _fetch_topic(SWAP_V3_TOPIC, "v3", chain, from_block, to_block, client=client, pair_cache=pair_cache)
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
