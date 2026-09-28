"""EVM dex topic fetching (split from dex_handler for size policy).

Extracted 2026-09-21: chunked eth_getLogs topic fetch + its response helper.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS, get_endpoints_for_range
from lite_indexer.chunks import CHUNK_PARALLELISM, chunk_range, merge_chunked_results
from lite_indexer.config import CHUNK_SIZE_EVM, RPC_TIMEOUT
from lite_indexer.dex.log_context import LogDecodeContext
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    apply_endpoint_backoff,
    ban_endpoint,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)
from lite_indexer.state import PairCache
from lite_indexer.strategies.evm._getlogs_body import build_getlogs_body
from lite_indexer.strategies.evm.topic_decode import decode_topic_page

logger = get_logger(__name__)


def _read_response_text(response: httpx.Response) -> str:
    """Read ``response.text`` off-thread, bounded by the caller's deadline."""
    return response.text


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

    async def _fetch_one_range(range_start: int, range_end: int) -> list[dict[str, Any]] | None:
        body = build_getlogs_body(topic, range_start, range_end)

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
                        asyncio.to_thread(_read_response_text, resp), timeout=attempt_deadline
                    )
                    page_ctx = LogDecodeContext(
                        chain=chain,
                        from_block=range_start,
                        to_block=range_end,
                        topic=topic,
                        pair_tokens=pair_cache.all,
                    )
                    page = decode_topic_page(resp_text, page_ctx, label, url)
                    if page is None:
                        record_endpoint_failure(url)
                        continue
                    # The response covers the range tail, so this
                    # endpoint served the range in full; all endpoints
                    # serve identical chain data. The old guard
                    # ``len(parsed) == len(logs)`` was unreachable
                    # because the decoder filters non-Swap logs, so
                    # every chunk queried ALL endpoints (5x RPC cost,
                    # 5x latency on busy chains).
                    if best_result is None or len(page) > len(best_result):
                        best_result = page
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

        # None = every endpoint failed for this range (total failure);
        # [] would be indistinguishable from "no swaps in range".
        return best_result

    effective_chunk = min(chunk_size, max(1, to_block - from_block + 1))
    semaphore = asyncio.Semaphore(CHUNK_PARALLELISM)

    async def _fetch_chunk(chunk_start: int, chunk_end: int) -> list[dict[str, Any]] | None:
        async with semaphore:
            return await _fetch_one_range(chunk_start, chunk_end)

    tasks: list[asyncio.Task[list[dict[str, Any]] | None]] = []
    for chunk_start, chunk_end in chunk_range(from_block, to_block, effective_chunk):
        tasks.append(asyncio.ensure_future(_fetch_chunk(chunk_start, chunk_end)))

    outcomes = await asyncio.gather(*tasks, return_exceptions=True)

    chunk_results: list[list[dict[str, Any]]] = []
    for outcome in outcomes:
        if isinstance(outcome, list):
            chunk_results.append(outcome)
        else:
            # A failed chunk (all endpoints down, or raised) makes the
            # topic leg incomplete: report total failure so the caller
            # defers instead of silently advancing.
            return None

    return merge_chunked_results(chunk_results)
