"""Finalization-aware RPC response cache — NodeCore port #2.

Deterministic reads are cached with TTLs scaled by block finality:

* ``eth_chainId``                      → identity, cached 24h
* block/receipt reads for blocks older
  than ``FINALITY_BLOCKS`` from tip    → immutable, cached long
* near-tip reads                       → NOT cached (reorg window)
* empty results ("0x", null, {}, [])   → NEVER cached (NodeCore's
  EmptyResponses list — caching them poisons the pool)

In-process LRU keyed by (method, canonical params). Single-writer asyncio
safe. Complements the query-layer fetch-through cache (which is
domain-aware) by sitting at the raw RPC layer where every caller benefits.
"""

from __future__ import annotations

import os
import time
from collections import OrderedDict
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

CACHE_ENABLED = os.getenv("RPC_RESPCACHE_ENABLED", "1") != "0"
MAX_ENTRIES = int(os.getenv("RPC_RESPCACHE_MAX", "4096"))

# Deterministic-method policies: method -> TTL seconds.
IDENTITY_TTL_S = 24 * 3600.0
FINALIZED_TTL_S = float(os.getenv("RPC_RESPCACHE_FINALIZED_TTL", "3600"))
# Blocks within this distance from tip are treated as mutable (reorg risk).
FINALITY_BLOCKS = int(os.getenv("RPC_RESPCACHE_FINALITY_BLOCKS", "64"))

IDENTITY_METHODS = {"eth_chainId"}
BLOCK_SCOPED_METHODS = {
    # method -> index of the block-scoped param (None = not cacheable here)
    "eth_getBlockByNumber": 0,
    "eth_getTransactionReceipt": None,  # receipt of an old tx is stable; scoped via block lookup below
}
EMPTY_RESULTS = {'"0x"', "null", "{}", "[]"}

_cache: OrderedDict[tuple[str, str], tuple[float, Any]] = OrderedDict()


def _canonical(params: list[Any] | None) -> str:
    return repr(params or [])


def _empty_result(result: Any) -> bool:
    """NodeCore's EmptyResponses guard: never cache nothing."""
    if result is None:
        return True
    text = result if isinstance(result, str) else repr(result)
    return text.strip() in EMPTY_RESULTS


def _ttl_for(method: str, params: list[Any] | None, tip: int | None) -> float | None:
    """TTL for this call, or None when it must not be cached."""
    if method in IDENTITY_METHODS:
        return IDENTITY_TTL_S
    if method == "eth_getBlockByNumber" and params:
        raw = params[0]
        try:
            block = int(raw, 16) if isinstance(raw, str) and raw.startswith("0x") else int(raw)
        except (TypeError, ValueError):
            return None
        if tip is not None and (tip - block) < FINALITY_BLOCKS:
            return None  # reorg window — skip
        return FINALIZED_TTL_S
    return None


def get(method: str, params: list[Any] | None = None) -> dict[str, Any] | None:
    """Cached response for a deterministic call, or None."""
    if not CACHE_ENABLED:
        return None
    key = (method, _canonical(params))
    hit = _cache.get(key)
    if hit is None:
        return None
    expires, payload = hit
    if time.time() > expires:
        _cache.pop(key, None)
        return None
    _cache.move_to_end(key)
    logger.debug("rpc_respcache_hit", method=method)
    return payload


def put(
    method: str,
    params: list[Any] | None,
    response: dict[str, Any] | None,
    *,
    tip: int | None = None,
) -> bool:
    """Cache a successful response under finality rules.

    Returns True when stored. Empty results are rejected outright.
    """
    if not CACHE_ENABLED or response is None:
        return False
    result = response.get("result") if isinstance(response, dict) else None
    if _empty_result(result):
        return False
    ttl = _ttl_for(method, params, tip)
    if ttl is None:
        return False
    key = (method, _canonical(params))
    if len(_cache) >= MAX_ENTRIES:
        _cache.popitem(last=False)  # evict oldest
    _cache[key] = (time.time() + ttl, response)
    return True


def clear() -> None:
    _cache.clear()


def stats() -> dict[str, int]:
    return {"entries": len(_cache), "max": MAX_ENTRIES}
