"""
PairCreated / PoolCreated event handler for EVM chains.

Provides fetch_pair_created() — fetches PairCreated (V2) and
PoolCreated (V3) events from known DEX factory addresses and
updates the pair cache with (token0, token1) mappings.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    FACTORY_ADDRESSES,
    PAIR_CREATED_TOPIC,
    POOL_CREATED_TOPIC,
    RPC_TIMEOUT,
    V3_FACTORY_ADDRESSES,
)
from lite_indexer.dex import parse_pair_created_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    apply_endpoint_backoff,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)
from lite_indexer.state import PairCache

logger = get_logger(__name__)


async def fetch_pair_created(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    pair_cache: PairCache,
) -> list[dict[str, Any]]:
    """Fetch PairCreated (V2) + PoolCreated (V3) events from known DEX factories.

    Iterates over known factory addresses for the chain, fetches
    PairCreated / PoolCreated events via eth_getLogs, and updates
    the pair cache with (token0, token1) mappings.

    Args:
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        pair_cache: Pair cache to update with resolved tokens.

    Returns:
        List of deployer event dicts for tracking.
    """
    deployer_events: list[dict[str, Any]] = []
    queries: list[tuple[str, str, bool]] = []
    for addr in FACTORY_ADDRESSES.get(chain, []):
        queries.append((addr, PAIR_CREATED_TOPIC, False))
    for addr in V3_FACTORY_ADDRESSES.get(chain, []):
        queries.append((addr, POOL_CREATED_TOPIC, True))

    if not queries:
        logger.debug("no_factories_for_chain", chain=chain)
        return deployer_events

    pairs_before = len(pair_cache)

    for factory_addr, event_topic, _is_v3 in queries:
        filter_params = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": factory_addr,
            "topics": [event_topic],
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getLogs",
            "params": [filter_params],
        }

        endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
        if not endpoints:
            continue

        start_idx = endpoints.index(get_best_endpoint(endpoints))
        rotated = endpoints[start_idx:] + endpoints[:start_idx]

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
                            pair_tokens, new_events = parse_pair_created_logs(logs, chain)
                            pair_cache.update(pair_tokens)
                            deployer_events.extend(new_events)
                            break
                elif resp.status_code == 429:
                    await handle_rate_limit(url)
                    record_endpoint_failure(url)
                    continue
                else:
                    record_endpoint_failure(url)
                    continue
            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
                record_endpoint_failure(url)
                continue
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
                logger.warning("factory_events_error", factory=factory_addr[:20], error=str(exc)[:60])
                record_endpoint_failure(url)
                continue

    if len(pair_cache) > pairs_before:
        pair_cache.save()

    return deployer_events


__all__ = [
    "fetch_pair_created",
]
