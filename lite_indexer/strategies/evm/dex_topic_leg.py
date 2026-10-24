"""One eth_getLogs leg over the endpoint list (split from ``dex_topic``).

Split out 2026-10-25 (structure canon: one primary function per module).
Holds the per-leg endpoint rotation, the hard per-attempt deadline and the
per-endpoint error taxonomy; ``fetch_topic`` keeps the chunk fan-out.

Rotation matters: every leg used to start on the SAME best endpoint, so six
legs collided on one provider account and a dead "best" endpoint made all
six pay its connect penalty (measured 2026-10-24: 3.06 s of real work
presented as 30.1 s). Offsetting the start per leg costs a dead endpoint at
most one leg.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.config import RPC_TIMEOUT
from lite_indexer.dex.log_context import LogDecodeContext
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    apply_endpoint_backoff,
    ban_endpoint,
    get_best_endpoint,
    handle_rate_limit,
    record_endpoint_failure,
)
from lite_indexer.strategies.evm._getlogs_body import build_getlogs_body
from lite_indexer.strategies.evm.dex_leg_route import (
    DEX_LEG_CONNECT_TIMEOUT_S,
    leg_start_index,
)
from lite_indexer.strategies.evm.topic_decode import decode_topic_page

logger = get_logger(__name__)

#: Hard deadline per endpoint attempt (incident 2026-08-24): a trickling
#: getLogs body defeated httpx's per-read timeout and wedged the ethereum dex
#: pipeline for ~20h inside ``resp.text`` streaming. ``wait_for`` makes the
#: ENTIRE attempt — connect + headers + full body read — bounded.
ATTEMPT_DEADLINE_S = RPC_TIMEOUT * 2 + 10


def _read_response_text(response: httpx.Response) -> str:
    """Read ``response.text`` off-thread, bounded by the caller's deadline."""
    return response.text


def leg_endpoint_order(endpoints: list[str], label: str) -> list[str]:
    """Endpoints in the order this leg should try them (per-leg start offset)."""
    best_idx = endpoints.index(get_best_endpoint(endpoints))
    start_idx = (best_idx + leg_start_index(label, len(endpoints))) % len(endpoints)
    return endpoints[start_idx:] + endpoints[:start_idx]


async def _read_body(response: httpx.Response) -> str:
    """Body read under the same hard deadline as the request."""
    return await asyncio.wait_for(asyncio.to_thread(_read_response_text, response), timeout=ATTEMPT_DEADLINE_S)


async def attempt_endpoint(
    client: httpx.AsyncClient,
    url: str,
    body: dict[str, Any],
    ctx: LogDecodeContext,
    label: str,
) -> tuple[bool, list[dict[str, Any]] | None]:
    """One endpoint attempt → ``(served, page)``; ``served`` False = try the next."""
    await apply_endpoint_backoff(url)
    try:
        resp = await asyncio.wait_for(
            client.post(
                url,
                json=body,
                timeout=httpx.Timeout(RPC_TIMEOUT, connect=DEX_LEG_CONNECT_TIMEOUT_S),
            ),
            timeout=ATTEMPT_DEADLINE_S,
        )
        if resp.status_code == 200:
            page = decode_topic_page(await _read_body(resp), ctx, label, url)
            if page is None:
                record_endpoint_failure(url)
                return False, None
            return True, page
        if resp.status_code == 429:
            await handle_rate_limit(url)
            return False, None
        record_endpoint_failure(url)
        return False, None
    except TimeoutError:
        # Hard deadline blown: wedged/slow-loris endpoint. Ban it so no other
        # chunk task lands here again this cycle.
        ban_endpoint(url, reason="wedged_response_deadline")
        logger.warning("dex_fetch_wedged_endpoint_banned", endpoint=url[:60], deadline_s=ATTEMPT_DEADLINE_S)
        return False, None
    except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError, httpx.WriteError):
        record_endpoint_failure(url)
        return False, None
    except (httpx.RemoteProtocolError, httpx.LocalProtocolError):
        record_endpoint_failure(url)
        return False, None
    except (httpx.HTTPError, ConnectionError, OSError, ValueError, KeyError, TypeError) as exc:
        logger.warning("dex_fetch_unexpected", chain=ctx.chain, endpoint=url[:50], error=str(exc)[:80])
        record_endpoint_failure(url)
        return False, None


async def fetch_range(
    topic: str,
    label: str,
    chain: str,
    endpoints: list[str],
    client: httpx.AsyncClient,
    pair_cache: Any,
    range_start: int,
    range_end: int,
) -> list[dict[str, Any]] | None:
    """Fetch one block range, rotating the endpoint start for this leg.

    Returns the longest decoded page, or ``None`` when every endpoint failed
    for the range (``[]`` would be indistinguishable from "no swaps in range").
    """
    body = build_getlogs_body(topic, range_start, range_end)
    ctx = LogDecodeContext(
        chain=chain,
        from_block=range_start,
        to_block=range_end,
        topic=topic,
        pair_tokens=pair_cache.all,
    )
    best_result: list[dict[str, Any]] | None = None
    for url in leg_endpoint_order(endpoints, label):
        served, page = await attempt_endpoint(client, url, body, ctx, label)
        if not served:
            continue
        # The response covers the range tail, so this endpoint served the range
        # in full; all endpoints serve identical chain data. The old guard
        # ``len(parsed) == len(logs)`` was unreachable because the decoder
        # filters non-Swap logs, so every chunk queried ALL endpoints
        # (5x RPC cost, 5x latency on busy chains).
        if best_result is None or len(page) > len(best_result):
            best_result = page
        break
    return best_result
