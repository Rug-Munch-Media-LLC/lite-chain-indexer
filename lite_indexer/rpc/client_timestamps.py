"""Block timestamp enrichment — cheap, bounded, deadline-capped fetch.

Split out of :mod:`lite_indexer.rpc.client` (2026-09-22). Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time so tests patching facade attributes keep
working unchanged.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any

import lite_indexer.rpc.client as _facade
from lite_indexer.config import (
    CHAIN_BLOCK_TIMES,
    DEFAULT_BLOCK_TIME,
    TIMESTAMP_PHASE_DEADLINE_S,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc.client_ts_fetch import _ts_fetch_batched, _TsState

logger = get_logger(__name__)

# Max eth_getBlockByNumber header fetches per timestamp-enrichment call.
# Larger sets are sampled and interpolated (see fetch_block_timestamps).
_TS_SAMPLE_MAX_FETCHES = 64


def _ts_sample(missing: list[int]) -> list[int]:
    """Sample at most ``_TS_SAMPLE_MAX_FETCHES`` anchors (ends included)."""
    limit = _facade._TS_SAMPLE_MAX_FETCHES
    if len(missing) <= limit:
        return list(missing)
    stride = -(-len(missing) // limit)  # ceil division
    anchors = set(missing[::stride])
    anchors.update((missing[0], missing[-1]))
    return sorted(anchors)


def _ts_interpolate(block_numbers: set[int], timestamps: dict[int, str], chain: str) -> None:
    """Interpolate/extrapolate unresolved blocks from the known anchors.

    Anchors include cache hits, so a fully cached neighbourhood needs no
    fetch at all. Outside the anchor range the chain's average block
    time extrapolates.
    """
    avg_block_time = CHAIN_BLOCK_TIMES.get(chain, DEFAULT_BLOCK_TIME)
    anchor_items = sorted((bn, ts) for bn, ts in timestamps.items())
    if not anchor_items:
        return
    parsed: list[tuple[int, datetime]] = []
    for bn, ts_str in anchor_items:
        try:
            parsed.append((bn, datetime.fromisoformat(ts_str.replace("Z", "+00:00"))))
        except ValueError:
            continue
    for bn in block_numbers:
        if bn in timestamps:
            continue
        # Find surrounding anchors.
        prev = next((p for p in reversed(parsed) if p[0] <= bn), None)
        nxt = next((p for p in parsed if p[0] >= bn), None)
        if prev and nxt and nxt[0] > prev[0]:
            frac = (bn - prev[0]) / (nxt[0] - prev[0])
            est_ts = prev[1] + (nxt[1] - prev[1]) * frac
        elif prev:
            offset = avg_block_time * (bn - prev[0])
            est_ts = prev[1] + timedelta(seconds=offset)
        elif nxt:
            offset = avg_block_time * (nxt[0] - bn)
            est_ts = nxt[1] - timedelta(seconds=offset)
        else:
            continue
        timestamps[bn] = est_ts.strftime("%Y-%m-%dT%H:%M:%SZ")


async def fetch_block_timestamps(
    client: Any,
    endpoints: list[str],
    block_numbers: set[int],
    chain: str = "ethereum",
    bloom_cache: Any = None,
    *,
    deadline_s: float | None = None,
) -> dict[int, str]:
    """Fetch timestamps for a set of block numbers, cheaply and non-blocking.

    The timestamp phase is enrichment, never the durable record, so it is
    designed to be the CHEAPEST possible RPC cost and to never stall a sync
    cycle (the 2026-09-14 root cause: ~1 ``eth_getBlockByNumber`` per block
    for a 500-block range, which 429'd, tripped ``all_endpoints_down`` and
    hung the cycle before the ClickHouse insert).

    Layered guarantees:

    1. **Cache first.** Block timestamps are immutable, so a bounded
       in-process LRU serves every repeat/overlapping block for free.
    2. **Bounded fetch.** Cache misses are sampled to at most
       ``_TS_SAMPLE_MAX_FETCHES`` anchors and sent as JSON-RPC *batches*
       (one HTTP POST per ``RPC_BATCH_MAX`` elements) with batching's
       per-element fallback disabled — a failing batch costs zero extra
       calls instead of one per element.
    3. **Hard deadline.** The whole fetch is capped by ``deadline_s``
       (default ``TIMESTAMP_PHASE_DEADLINE_S``); on timeout or error the
       phase returns what it has and the rest is interpolated.
    4. **Interpolation.** Cache hits + fetched anchors are linearly
       interpolated/extrapolated with the chain's average block time, so a
       short deadline degrades accuracy, never the cycle.

    Args:
        client: RpcClient instance (or a test double exposing ``call_batch``).
        endpoints: List of RPC endpoints for the chain.
        block_numbers: Set of block numbers to resolve.
        chain: Chain name used to look up the average block time.
        bloom_cache: Optional :class:`BloomCache` to populate with header
            logsBloom values (headers are already being fetched).
        deadline_s: Overall deadline for the RPC portion, in seconds.
            ``None`` uses ``TIMESTAMP_PHASE_DEADLINE_S``.

    Returns:
        Dict of {block_number: ISO-8601 timestamp string}. Blocks that could
        not be resolved or interpolated are simply absent.
    """
    if not block_numbers:
        return {}

    budget = TIMESTAMP_PHASE_DEADLINE_S if deadline_s is None else deadline_s

    # 1. Cache first: only fetch what is not already known (immutable).
    timestamps: dict[int, str] = {}
    missing: list[int] = []
    for bn in sorted(block_numbers):
        cached = _facade._ts_cache_get(chain, bn)
        if cached is not None:
            timestamps[bn] = cached
        else:
            missing.append(bn)

    if missing:
        # 2. Bounded fetch: sample anchors for large sets (interpolate the
        # rest) and collapse the anchors into JSON-RPC batches.
        fetch_list = _ts_sample(missing)
        endpoint = _facade.get_best_endpoint(endpoints)
        state = _TsState(chain=chain, bloom_cache=bloom_cache, timestamps=timestamps)

        # 3. Hard deadline. wait_for cancels the fetch (including any
        # in-flight throttle/budget sleep) so the phase can never outlive
        # the cycle.
        try:
            await asyncio.wait_for(
                _ts_fetch_batched(client, endpoint, fetch_list, state),
                timeout=max(0.0, budget),
            )
        except TimeoutError:
            logger.warning(
                "timestamp_phase_deadline",
                chain=chain,
                blocks=len(missing),
                anchors=len(fetch_list),
                deadline_s=round(budget, 2),
            )
        except Exception as exc:  # noqa: BLE001 — enrichment must never abort the cycle
            logger.warning(
                "timestamp_phase_failed",
                chain=chain,
                blocks=len(missing),
                error=str(exc)[:120],
            )

    # 4. Interpolate unresolved blocks between the known anchors (real
    # timestamps) and extrapolate outside the anchor range with the chain's
    # average block time.
    if len(timestamps) < len(block_numbers):
        _ts_interpolate(block_numbers, timestamps, chain)

    return timestamps
