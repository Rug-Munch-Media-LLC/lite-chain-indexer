"""
PairCreated / PoolCreated event handler for EVM chains.

Provides fetch_pair_created() — fetches PairCreated (V2) and
PoolCreated (V3) events from known DEX factory addresses and
updates the pair cache with (token0, token1) mappings.

Launches are gated by a false-launch guard: an event is only emitted
when the factory transaction receipt has status == 1 AND the pair
address has deployed code (eth_getCode != 0x). Verification is
best-effort — probe errors skip only the affected event.
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

# Short timeout for best-effort launch-verification probes (receipt + getCode).
# Intentional contrast with RPC_TIMEOUT (30s): a slow probe must not stall the
# batch, it should just skip the single event under verification.
_PAIR_VERIFY_TIMEOUT = 5.0


async def _rpc_probe(
    client: httpx.AsyncClient,
    endpoint: str,
    method: str,
    params: list[Any],
    timeout: float,
) -> dict[str, Any] | None:
    """Best-effort single-shot JSON-RPC probe (no rotation, no retries).

    Used by the pair-launch guard for eth_getTransactionReceipt and
    eth_getCode. Returns the full response dict on success, or None on
    any transport/HTTP/parse error — the caller decides how to skip.
    """
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }
    try:
        resp = await client.post(endpoint, json=body, timeout=timeout)
        if resp.status_code != 200:
            return None
        data = resp.json()
    except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError):
        return None
    if isinstance(data, dict) and "result" in data:
        return data
    return None


async def _receipt_status_ok(
    client: httpx.AsyncClient,
    endpoint: str,
    tx_hash: str,
    timeout: float,
) -> bool:
    """True when the factory tx receipt exists and ``status == 0x1``.

    Fail-closed: a missing/None receipt, a probe error, or an absent
    ``status`` field (pre-Byzantium EIP-658 receipt shapes) all return
    False so the launch event is skipped rather than falsely emitted.
    """
    result = await _rpc_probe(client, endpoint, "eth_getTransactionReceipt", [tx_hash], timeout)
    if result is None or not isinstance(result.get("result"), dict):
        return False
    receipt = result["result"]
    status = receipt.get("status")
    if status is None:
        logger.warning("pair_receipt_status_missing", tx_hash=tx_hash[:20])
        return False
    try:
        return int(str(status), 16) == 1
    except ValueError:
        logger.warning("pair_receipt_status_invalid", tx_hash=tx_hash[:20], status=str(status)[:20])
        return False


async def _pair_code_ok(
    client: httpx.AsyncClient,
    endpoint: str,
    pair: str,
    timeout: float,
) -> bool:
    """True when the pair address has deployed code (``eth_getCode != 0x``)."""
    result = await _rpc_probe(client, endpoint, "eth_getCode", [pair, "latest"], timeout)
    if result is None or not isinstance(result.get("result"), str):
        return False
    code = result["result"]
    return code not in ("0x", "0x0", "")


async def _verify_pair_launches(
    chain: str,
    events: list[dict[str, Any]],
    *,
    client: httpx.AsyncClient,
    endpoints: list[str],
) -> tuple[list[dict[str, Any]], dict[str, tuple[str, str]]]:
    """Verify factory launch events before they are emitted.

    False-launch guard: a PairCreated/PoolCreated event is only kept when
    (a) the factory tx receipt has ``status == 1`` and (b) ``eth_getCode``
    at the pair address returns non-empty code. Reverted factory txs and
    no-code pair addresses are skipped (the event AND its pair-cache entry).

    Best-effort by design: a probe error (timeout/connection/HTTP) logs and
    skips ONLY that event — it never raises or fails the batch. Receipt
    lookups are deduped per transaction hash (a factory tx can emit
    multiple pair events).

    Returns:
        Tuple of (verified_events, verified_pairs) where verified_pairs maps
        pair_address -> (token0, token1) and is consistent with the events.
    """
    endpoint = get_best_endpoint(endpoints)
    verified: list[dict[str, Any]] = []
    receipt_ok_cache: dict[str, bool] = {}

    for event in events:
        tx_hash = event.get("transactionHash", "")
        pair = event.get("pair_address", "")
        if not tx_hash or not pair:
            logger.debug("pair_launch_missing_fields", chain=chain, tx_hash=tx_hash, pair=pair)
            continue

        if tx_hash not in receipt_ok_cache:
            receipt_ok_cache[tx_hash] = await _receipt_status_ok(client, endpoint, tx_hash, _PAIR_VERIFY_TIMEOUT)
        if not receipt_ok_cache[tx_hash]:
            logger.debug("pair_launch_skipped_receipt", chain=chain, tx_hash=tx_hash[:20], pair=pair[:20])
            continue

        if not await _pair_code_ok(client, endpoint, pair, _PAIR_VERIFY_TIMEOUT):
            logger.debug("pair_launch_skipped_code", chain=chain, pair=pair[:20])
            continue

        verified.append(event)

    verified_pairs: dict[str, tuple[str, str]] = {
        e["pair_address"]: (e["token0"], e["token1"])
        for e in verified
        if e.get("pair_address") and e.get("token0") and e.get("token1")
    }
    return verified, verified_pairs


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
                            # False-launch guard: only emit launches whose factory
                            # tx receipt succeeded (status == 1) and whose pair
                            # address has deployed code. Skips are per-event and
                            # never fail the batch.
                            if new_events:
                                new_events, pair_tokens = await _verify_pair_launches(
                                    chain,
                                    new_events,
                                    client=client,
                                    endpoints=endpoints,
                                )
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
