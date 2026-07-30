"""
ERC-20 Transfer event handler for EVM chains.

Provides fetch_transfers() — fetches ERC-20 Transfer events via
eth_getLogs with only the Transfer event topic filter (no address
filter) so it catches every transfer on the chain.

Uses a curated list of endpoints known to support eth_getLogs.
Falls through errors to try the next endpoint.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import TRANSFER_EVENT_TOPIC, RPC_TIMEOUT
from lite_indexer.dex import parse_transfer_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    apply_endpoint_backoff,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)

from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range

logger = get_logger(__name__)


async def fetch_transfers(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    rpc: RpcClient,
    batch_size: int,
) -> list[dict[str, Any]]:
    """Fetch ERC-20 Transfer events via eth_getLogs.

    Calls eth_getLogs with only the Transfer event topic filter
    (no address filter) so we catch every transfer on the chain.

    Uses a curated list of endpoints known to support eth_getLogs.
    Falls through errors to try the next endpoint.

    Error handling:
    - Chain reorg: from_block > to_block returns ["__CHAIN_REORG__"]
    - Empty RPC response: returns []
    - Malformed JSON: caught by generic except, logs + continues
    - Rate limit (429): exponential backoff
    - Block range too large: capped by enforce_block_range
    - Network timeout: caught, logs + tries next endpoint

    Args:
        chain: Chain name (e.g. "ethereum", "base").
        from_block: Start block number (inclusive).
        to_block: End block number (inclusive).
        client: Shared HTTP client.
        rpc: RPC client instance.
        batch_size: Max blocks per batch for range enforcement.

    Returns:
        List of parsed transfer event dicts, or ["__CHAIN_REORG__"]
        if a chain reorg was detected.
    """
    # Chain reorg detection
    if detect_reorg(from_block, to_block):
        return ["__CHAIN_REORG__"]

    # Enforce chain-specific block range
    from_block, to_block = enforce_block_range(chain, from_block, to_block, batch_size)

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.error("no_endpoints_for_chain", chain=chain)
        return []

    # Rotate starting endpoint for load distribution
    start_idx = endpoints.index(get_best_endpoint(endpoints))
    rotated = endpoints[start_idx:] + endpoints[:start_idx]

    filter_params = {
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [TRANSFER_EVENT_TOPIC],
    }
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [filter_params],
    }

    best_result: list[dict[str, Any]] | None = None
    last_error: str | None = None

    for url in rotated:
        await apply_endpoint_backoff(url)

        try:
            resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)

            # Empty or too-small response
            if resp.status_code == 200:
                raw_text = resp.text
                if not raw_text or len(raw_text) < 10:
                    last_error = "empty_response"
                    logger.debug("empty_rpc_response", endpoint=url[:50], chain=chain)
                    record_endpoint_failure(url)
                    continue

                try:
                    data = resp.json()
                except (json.JSONDecodeError, ValueError) as exc:
                    last_error = f"malformed_json: {str(exc)[:40]}"
                    logger.debug(
                        "rpc_malformed_json",
                        endpoint=url[:50],
                        chain=chain,
                        error=str(exc)[:60],
                    )
                    record_endpoint_failure(url)
                    continue

                if not isinstance(data, dict):
                    last_error = "non_dict_response"
                    record_endpoint_failure(url)
                    continue

                if "result" in data:
                    logs = data["result"]
                    if isinstance(logs, list):
                        parsed = parse_transfer_logs(logs, chain, from_block, to_block)
                        if best_result is None or len(parsed) > len(best_result):
                            best_result = parsed
                            if len(parsed) == len(logs):
                                break  # Got all events
                    elif isinstance(logs, dict) and "error" in logs:
                        last_error = logs["error"].get("message", str(logs))
                        record_endpoint_failure(url)
                        continue
                    else:
                        # result is null — endpoint works, no logs in range
                        if logs is None:
                            if best_result is None:
                                best_result = []
                            break
                        last_error = "unexpected_result_type"
                        record_endpoint_failure(url)
                        continue
                elif "error" in data:
                    last_error = data["error"].get("message", str(data))
                    record_endpoint_failure(url)
                    continue
            elif resp.status_code == 429:
                last_error = "rate_limited"
                await handle_rate_limit(url)
                continue
            else:
                last_error = f"http_{resp.status_code}"
                record_endpoint_failure(url)
                continue

        except httpx.TimeoutException:
            last_error = "timeout"
            record_endpoint_failure(url)
            continue
        except (httpx.ConnectError, httpx.ReadError, httpx.WriteError) as exc:
            last_error = str(exc)[:60]
            record_endpoint_failure(url)
            continue
        except (httpx.RemoteProtocolError, httpx.LocalProtocolError) as exc:
            last_error = f"protocol_error: {str(exc)[:40]}"
            record_endpoint_failure(url)
            continue
        except Exception as exc:
            last_error = str(exc)[:80]
            record_endpoint_failure(url)
            continue

    if best_result is not None:
        return best_result

    logger.error(
        "all_endpoints_failed",
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        last_error=last_error,
    )
    return []


__all__ = [
    "fetch_transfers",
]
