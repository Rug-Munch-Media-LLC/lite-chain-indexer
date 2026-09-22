"""Timestamp-phase fetchers — batched + individual anchor resolution.

Split out of :mod:`lite_indexer.rpc.client_timestamps` (2026-09-22) so
the phase stays under the per-file size ratchet. Patchable
collaborators are resolved through the ``lite_indexer.rpc.client``
facade module at call time.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

import lite_indexer.rpc.client as _facade
from lite_indexer.config import GET_LOGS_SEMAPHORE_LIMIT
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass(slots=True)
class _TsState:
    """Shared state for one timestamp phase (keeps helpers ≤5 params)."""

    chain: str
    bloom_cache: Any
    timestamps: dict[int, str] = field(default_factory=dict)


async def _ts_record_block(state: _TsState, block_num: int, block: Any) -> str | None:
    """Parse a header's timestamp, warm bloom + the timestamp LRU."""
    if not (block and "timestamp" in block):
        return None
    try:
        ts_int = int(block["timestamp"], 16)
    except (ValueError, TypeError):
        return None
    if state.bloom_cache is not None and block.get("logsBloom"):
        with contextlib.suppress(Exception):
            await state.bloom_cache.set(state.chain, block_num, str(block["logsBloom"]))
    ts = datetime.fromtimestamp(ts_int, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    _facade._ts_cache_put(state.chain, block_num, ts)
    return ts


async def _ts_fetch_batched(
    client: Any,
    endpoint: str,
    fetch_list: list[int],
    state: _TsState,
) -> None:
    """ONE JSON-RPC array POST per RPC_BATCH_MAX anchors.

    The per-element fallback is intentionally OFF here: a failed batch
    must cost zero extra calls (interpolation covers the gap) rather
    than explode into one call per anchor.
    """
    batch_fn = getattr(client, "call_batch", None)
    if not callable(batch_fn):
        await _ts_fetch_individual(client, endpoint, fetch_list, state)
        return
    responses = await batch_fn(
        endpoint,
        [("eth_getBlockByNumber", [hex(bn), False]) for bn in fetch_list],
        fallback=False,
    )
    for block_num, resp in zip(fetch_list, responses, strict=False):
        block = resp.get("result") if isinstance(resp, dict) and "result" in resp else resp
        ts = await _ts_record_block(state, block_num, block)
        if ts is not None:
            state.timestamps[block_num] = ts


async def _ts_fetch_individual(
    client: Any,
    endpoint: str,
    fetch_list: list[int],
    state: _TsState,
) -> None:
    """No batch support (lightweight test double / non-batch client):
    bounded concurrency, still under the phase deadline."""
    semaphore = asyncio.Semaphore(GET_LOGS_SEMAPHORE_LIMIT)

    async def _get_ts(block_num: int) -> tuple[int, str | None]:
        async with semaphore:
            try:
                block = await client.eth_get_block_by_number(endpoint, block_num)
            except (
                httpx.HTTPError,
                ConnectionError,
                OSError,
                TimeoutError,
                ValueError,
                KeyError,
                TypeError,
            ) as exc:
                logger.warning("block_ts_fetch_failed", block=block_num, error=str(exc)[:80])
                return block_num, None
            return block_num, await _ts_record_block(state, block_num, block)

    results = await asyncio.gather(*(_get_ts(bn) for bn in fetch_list), return_exceptions=True)
    for res in results:
        if isinstance(res, tuple) and res[1] is not None:
            state.timestamps[res[0]] = res[1]
