"""
ERC-20 Transfer event handler for EVM chains.

Provides ``fetch_transfers()`` — fetches ERC-20 Transfer events via
``eth_getLogs`` with the Transfer event topic filter (optionally scoped
to a specific token address), using chunked parallel fetching.

Two optimizations over per-block polling:

1. **Bloom pre-filtering (TrueBlocks approach)** — when an ``address``
   filter is requested with a :class:`BloomCache`, block headers for the
   range are fetched (and cached), and only blocks whose ``logsBloom``
   may contain the address are queried for logs. Rarely-traded tokens
   skip the ``eth_getLogs`` call entirely when no block can match.

2. **Chunk-based indexing** — the range is split into configurable
   chunks (default 50 blocks) and each chunk is fetched with one
   ``eth_getLogs`` call, up to ``CHUNK_PARALLELISM`` at a time. This
   cuts RPC round trips and avoids "response too large" errors.

Uses a curated list of endpoints known to support eth_getLogs.
Falls through errors to try the next endpoint.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS, get_endpoints_for_range, last_resort_endpoints
from lite_indexer.chunks import CHUNK_PARALLELISM, chunk_range, merge_chunked_results
from lite_indexer.config import BLOOM_ENABLED, CHUNK_SIZE_EVM, RPC_TIMEOUT, TRANSFER_EVENT_TOPIC
from lite_indexer.dex import parse_transfer_logs
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    RpcFetchFailedError,
    apply_endpoint_backoff,
    ban_endpoint,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)
from lite_indexer.rpc.pool import (
    desperation_probe_due,
    force_probe_endpoint,
    least_banned_endpoint,
    mark_desperation_probe,
    pool_fully_down,
)
from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.bloom import (
    BloomCache,
    candidate_blocks_for_address,
    fetch_block_blooms,
)

logger = get_logger(__name__)


# ── Adaptive chunk-size reduction ─────────────────────────────────────
# After repeated pool-wide failures (RpcFetchFailedError), the effective
# chunk size is halved (floor ADAPTIVE_CHUNK_FLOOR) so restrictive free
# RPCs with small range caps (blastapi=10, blockrazor=25) or small result
# caps can serve the range. A success resets the reduction. This makes
# catch-up degrade gracefully instead of failing the same 50-block chunk
# forever when every endpoint caps below the default chunk size.
_ADAPTIVE_CHUNK_LEVELS: dict[str, int] = {}
ADAPTIVE_CHUNK_FLOOR: int = 5
ADAPTIVE_CHUNK_MAX_HALVES: int = 4


def _adaptive_chunk_size(chain: str, base_chunk: int) -> int:
    """Return the effective chunk size for ``chain`` after repeated failures."""
    halves: int = min(_ADAPTIVE_CHUNK_LEVELS.get(chain, 0), ADAPTIVE_CHUNK_MAX_HALVES)
    divisor: int = 2**halves
    return max(ADAPTIVE_CHUNK_FLOOR, base_chunk // divisor)


def _bump_adaptive_chunk(chain: str) -> None:
    """Record one pool-wide failure — next fetch uses a smaller chunk."""
    _ADAPTIVE_CHUNK_LEVELS[chain] = _ADAPTIVE_CHUNK_LEVELS.get(chain, 0) + 1


def _reset_adaptive_chunk(chain: str) -> None:
    """Reset the adaptive chunk size after a successful fetch."""
    _ADAPTIVE_CHUNK_LEVELS.pop(chain, None)


def _contiguous_ranges(blocks: set[int]) -> list[tuple[int, int]]:
    """Group a set of block numbers into minimal contiguous ranges.

    Used to build the smallest set of ``eth_getLogs`` ranges covering
    only blocks that passed the bloom pre-filter.

    Args:
        blocks: Block numbers.

    Returns:
        List of ``(start, end)`` inclusive ranges.
    """
    if not blocks:
        return []
    ordered = sorted(blocks)
    ranges: list[tuple[int, int]] = []
    start = prev = ordered[0]
    for block in ordered[1:]:
        if block == prev + 1:
            prev = block
            continue
        ranges.append((start, prev))
        start = prev = block
    ranges.append((start, prev))
    return ranges


async def _fetch_range(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    topic: str,
    address: str | None = None,
    endpoints: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Fetch logs for one contiguous range via a single eth_getLogs call.

    Tries every suitable endpoint in rotation, keeping the most complete
    result. Raises :class:`RpcFetchFailedError` when every endpoint fails
    (data-loss guard — the caller must not advance state past the range).

    Args:
        chain: Chain name.
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        client: Shared HTTP client.
        topic: Event topic hash to filter on.
        address: Optional contract address to filter on.
        endpoints: Endpoint list to use (defaults to chain config).

    Returns:
        Parsed transfer event dicts for the range.
    """
    endpoints = endpoints or GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        return []

    filter_params: dict[str, Any] = {
        "fromBlock": hex(from_block),
        "toBlock": hex(to_block),
        "topics": [topic],
    }
    if address:
        filter_params["address"] = address

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [filter_params],
    }

    # Rotate starting endpoint for load distribution
    start_idx = endpoints.index(get_best_endpoint(endpoints))
    rotated = endpoints[start_idx:] + endpoints[:start_idx]

    # Pool-wide outage desperation probe: when EVERY endpoint is banned or
    # circuit-open, do not hammer the whole pool every cycle (that deepens
    # the bans and stalls the chain). Instead force-probe the least-banned
    # endpoint at most once per desperation interval so the chain can
    # self-heal the moment any endpoint recovers.
    if pool_fully_down(endpoints):
        if desperation_probe_due(endpoints):
            mark_desperation_probe(endpoints)
            probe = least_banned_endpoint(endpoints)
            force_probe_endpoint(probe)
            logger.warning(
                "rpc_desperation_probe",
                chain=chain,
                endpoint=probe[:60],
                from_block=from_block,
                to_block=to_block,
            )
            rotated = [probe]
        else:
            # Probe not due yet — do not burn the pool. Raise the
            # data-loss guard so the cycle retries on the next probe tick.
            raise RpcFetchFailedError(
                f"pool fully down for {chain} range {from_block}-{to_block}: waiting for desperation probe"
            )

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
                        # A 200 with a result list means this endpoint
                        # served the range in full. All endpoints serve
                        # identical chain data for the same range+topic,
                        # so stop rotating here. The old guard
                        # ``len(parsed) == len(logs)`` was unreachable in
                        # practice because parse_transfer_logs filters
                        # (removed logs, NFTs, dedup) — so every chunk
                        # queried ALL endpoints sequentially and busy
                        # chains (ethereum) blew the 120s sync timeout,
                        # re-saving block 100 forever.
                        break
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
            elif resp.status_code == 403:
                # Provider blacklists the endpoint (bsc-rpc.publicnode.com
                # returned http_403 on ranged topic-only eth_getLogs during
                # the 08/04 outage). Hard-ban for an exponential window.
                last_error = "http_403"
                record_endpoint_failure(url)
                ban_endpoint(url, reason="http_403")
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
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            last_error = str(exc)[:80]
            logger.warning("transfer_fetch_unexpected", chain=chain, endpoint=url[:50], error=str(exc)[:80])
            record_endpoint_failure(url)
            continue

    # Last-resort fallback: every endpoint in the active pool failed.
    # Before giving up (data-loss guard → retry next cycle), try the
    # chain's known-good last-resort endpoints (official RPCs / gateway.org)
    # so a total pool wipe still has a shot at serving the range.
    if best_result is None:
        last_resort = [ep for ep in last_resort_endpoints(chain) if ep not in rotated]
        if last_resort:
            logger.warning(
                "last_resort_endpoints_tried",
                chain=chain,
                count=len(last_resort),
                from_block=from_block,
                to_block=to_block,
            )
            for url in last_resort:
                await apply_endpoint_backoff(url)
                try:
                    resp = await client.post(url, json=body, timeout=RPC_TIMEOUT)
                    if resp.status_code == 200 and resp.text and len(resp.text) >= 10:
                        data = resp.json()
                        if isinstance(data, dict) and isinstance(data.get("result"), list):
                            logs = data["result"]
                            parsed = parse_transfer_logs(logs, chain, from_block, to_block)
                            if best_result is None or len(parsed) > len(best_result):
                                best_result = parsed
                                if len(parsed) == len(logs):
                                    break
                        elif isinstance(data, dict) and data.get("result") is None:
                            if best_result is None:
                                best_result = []
                            break
                    elif resp.status_code == 429:
                        await handle_rate_limit(url)
                    elif resp.status_code == 403:
                        record_endpoint_failure(url)
                        ban_endpoint(url, reason="http_403")
                except (
                    httpx.HTTPError,
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    last_error = str(exc)[:80]
                    logger.warning(
                        "last_resort_fetch_failed",
                        chain=chain,
                        endpoint=url[:50],
                        error=str(exc)[:60],
                    )
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
    # Do NOT return [] here — an empty list would be indistinguishable
    # from "no events in range" and the indexer would advance its state
    # past this range, permanently losing data. Raise so the caller
    # retries the range on the next cycle.
    raise RpcFetchFailedError(f"all endpoints failed for {chain} range {from_block}-{to_block}: {last_error}")


async def fetch_transfers(
    chain: str,
    from_block: int,
    to_block: int,
    *,
    client: httpx.AsyncClient,
    rpc: RpcClient,
    batch_size: int,
    chunk_size: int | None = None,
    address: str | None = None,
    bloom_cache: BloomCache | None = None,
) -> list[dict[str, Any]]:
    """Fetch ERC-20 Transfer events via eth_getLogs.

    Fetches all Transfer events for the range (topic-only filter) or for
    a specific token address, using chunked parallel fetching and,
    optionally, bloom pre-filtering.

    Error handling:
    - Chain reorg: from_block > to_block returns ["__CHAIN_REORG__"]
    - Empty RPC response: returns []
    - Malformed JSON: caught by generic except, logs + continues
    - Rate limit (429): exponential backoff
    - Block range too large: capped by enforce_block_range
    - Network timeout: caught, logs + tries next endpoint
    - All endpoints fail for a chunk: raises RpcFetchFailedError

    Args:
        chain: Chain name (e.g. "ethereum", "base").
        from_block: Start block number (inclusive).
        to_block: End block number (inclusive).
        client: Shared HTTP client.
        rpc: RPC client instance.
        batch_size: Max blocks per batch for range enforcement.
        chunk_size: Max blocks per eth_getLogs chunk. When None, the
            whole range is fetched in a single call (legacy behavior).
        address: Optional 0x-prefixed token address to filter on.
        bloom_cache: Optional bloom cache for pre-filtering. Only used
            when ``address`` is provided (TrueBlocks-style address
            queries) and bloom filtering is enabled.

    Returns:
        List of parsed transfer event dicts, or ["__CHAIN_REORG__"]
        if a chain reorg was detected.
    """
    # Chain reorg detection
    if detect_reorg(from_block, to_block):
        return cast(list[dict[str, Any]], ["__CHAIN_REORG__"])

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.error("no_endpoints_for_chain", chain=chain)
        return []

    # Enforce chain-specific + per-provider block range limits, then drop
    # endpoints that cannot serve the resulting range (free RPCs often cap
    # eth_getLogs at 10-100 blocks). The effective range is capped to the
    # largest provider limit, so at least one endpoint always remains.
    from_block, to_block = enforce_block_range(chain, from_block, to_block, batch_size, endpoints)
    endpoints = get_endpoints_for_range(endpoints, to_block - from_block)

    effective_chunk = chunk_size if chunk_size and chunk_size > 0 else CHUNK_SIZE_EVM
    # A chunk must never exceed the batch size.
    effective_chunk = min(effective_chunk, max(1, to_block - from_block + 1))
    # Adaptive: after repeated pool-wide failures, use a smaller chunk so
    # range/result-capped free RPCs can serve the range.
    effective_chunk = _adaptive_chunk_size(chain, effective_chunk)
    if chunk_size is None:
        # Legacy single-call behavior — no chunking.
        return await _fetch_range(
            chain,
            from_block,
            to_block,
            client=client,
            topic=TRANSFER_EVENT_TOPIC,
            address=address,
            endpoints=endpoints,
        )

    use_bloom = BLOOM_ENABLED and address is not None and bloom_cache is not None
    ranges: list[tuple[int, int]] = [(from_block, to_block)]

    if use_bloom and bloom_cache is not None and address is not None:
        # TrueBlocks-style pre-filter: warm the bloom cache for the range,
        # then only query blocks whose logsBloom may contain the address.
        blocks = set(range(from_block, to_block + 1))
        blooms = await fetch_block_blooms(
            chain,
            blocks,
            client=client,
            endpoints=endpoints,
            cache=bloom_cache,
        )
        if not blooms:
            # No blooms could be fetched — fall through to the unfiltered
            # address query (never skip data on an unknown bloom).
            logger.debug("bloom_cache_empty_fallback", chain=chain, range_size=len(blocks))
        else:
            candidates = await candidate_blocks_for_address(chain, blocks, address, bloom_cache)
            if not candidates:
                logger.info(
                    "bloom_prefilter_skipped",
                    chain=chain,
                    address=address[:12],
                    blocks=len(blocks),
                )
                return []  # No block can contain this address — zero RPC log calls.
            ranges = _contiguous_ranges(candidates)
            logger.info(
                "bloom_prefilter_candidates",
                chain=chain,
                address=address[:12],
                blocks=len(blocks),
                candidates=len(candidates),
                ranges=len(ranges),
            )

    # Chunked parallel fetch: one eth_getLogs per chunk, bounded concurrency.
    semaphore = asyncio.Semaphore(CHUNK_PARALLELISM)

    async def _fetch_chunk(chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        async with semaphore:
            return await _fetch_range(
                chain,
                chunk_start,
                chunk_end,
                client=client,
                topic=TRANSFER_EVENT_TOPIC,
                address=address,
                endpoints=endpoints,
            )

    tasks: list[asyncio.Task[list[dict[str, Any]]]] = []
    for range_start, range_end in ranges:
        for chunk_start, chunk_end in chunk_range(range_start, range_end, effective_chunk):
            tasks.append(asyncio.ensure_future(_fetch_chunk(chunk_start, chunk_end)))

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    chunk_results: list[list[dict[str, Any]]] = []
    for outcome in outcomes:
        if isinstance(outcome, Exception):
            # Data-loss guard: propagate — the caller must not advance
            # state past a range that was not fully fetched.
            if isinstance(outcome, RpcFetchFailedError):
                # Reduce the chunk size so the retry (next cycle) has a
                # better chance against range/result-capped free RPCs.
                _bump_adaptive_chunk(chain)
                raise outcome
            logger.error("chunk_fetch_failed", chain=chain, error=str(outcome)[:120])
            raise RpcFetchFailedError(f"chunk fetch failed for {chain}: {str(outcome)[:120]}")
        if isinstance(outcome, list):
            chunk_results.append(outcome)

    # Every chunk succeeded — restore the default chunk size.
    _reset_adaptive_chunk(chain)
    return merge_chunked_results(chunk_results)


__all__ = [
    "fetch_transfers",
    "_fetch_range",
    "_contiguous_ranges",
]
