"""Block->timestamp LRU cache for the RPC client (extracted 2026-09-21).

Micro-pass 1 of the rpc/client.py split: the state + helpers move together so
the cache stays single-writer, event-loop-scoped, and independently testable.
"""

from __future__ import annotations

from collections import OrderedDict

from lite_indexer.config import TIMESTAMP_CACHE_MAX

# Immutable block->timestamp LRU keyed by (chain, block). Timestamps never
# change for a given (chain, block), so results are safe to serve across
# cycles and overlapping catch-up ranges — the same header is never re-fetched.
# Bounded by TIMESTAMP_CACHE_MAX; single-writer event-loop access only.
_ts_cache: OrderedDict[tuple[str, int], str] = OrderedDict()


def _ts_cache_get(chain: str, block: int) -> str | None:
    key = (chain, block)
    value = _ts_cache.get(key)
    if value is None:
        return None
    _ts_cache.move_to_end(key)
    return value


def _ts_cache_put(chain: str, block: int, ts: str) -> None:
    if not ts:
        return
    key = (chain, block)
    _ts_cache[key] = ts
    _ts_cache.move_to_end(key)
    while len(_ts_cache) > TIMESTAMP_CACHE_MAX:
        _ts_cache.popitem(last=False)


def clear_timestamp_cache() -> None:
    """Drop the in-process block->timestamp LRU (tests/ops)."""
    _ts_cache.clear()
