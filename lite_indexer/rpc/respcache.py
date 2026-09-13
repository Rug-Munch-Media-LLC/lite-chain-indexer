"""Finalization-aware RPC response cache — L1 memory + L2 SQLite.

Deterministic reads are cached with TTLs scaled by block finality:

* ``eth_chainId``                      → identity, cached 24h
* block/receipt reads for blocks older
  than ``RPC_CACHE_CONFIRMATIONS``      → immutable, cached long
* hash-scoped reads (block by hash)     → immutable, cached long
* historical ``eth_getLogs`` ranges     → cached once below confirm depth
* near-tip / tip / volatile reads       → NOT cached (reorg window)
* empty results ("0x", null, {}, [])    → never cached, EXCEPT historical
  getLogs ranges, where an empty result is a stable "no logs" answer

L1 is the in-process bounded LRU (single-writer asyncio safe). L2 is the
persistent SQLite store (``rpc/cache_db.py``) consulted after L1 and
populated on a miss, so caches survive restarts and backfill/reconcile never
re-fetch immutable history. ``invalidate_range`` purges both tiers and is
called from the reconciliation rewind path.

All persistence is failure-safe: a broken/missing DB degrades to L1-only.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from lite_indexer.config import RPC_CACHE_CONFIRMATIONS, RPC_CACHE_TTL_S
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import cache_db

logger = get_logger(__name__)

CACHE_ENABLED = os.getenv("RPC_RESPCACHE_ENABLED", "1") != "0"
MAX_ENTRIES = int(os.getenv("RPC_RESPCACHE_MAX", "4096"))

# Deterministic-method policies: method -> TTL seconds.
IDENTITY_TTL_S = 24 * 3600.0
FINALIZED_TTL_S = float(os.getenv("RPC_RESPCACHE_FINALIZED_TTL", "3600"))
# Blocks within this distance from tip are treated as mutable (reorg risk).
FINALITY_BLOCKS = int(os.getenv("RPC_RESPCACHE_FINALITY_BLOCKS", str(RPC_CACHE_CONFIRMATIONS)))

IDENTITY_METHODS = {"eth_chainId"}
# Methods whose params carry an immutable content hash (block/tx hash).
HASH_SCOPED_METHODS = {
    "eth_getBlockByHash",
    "eth_getTransactionByHash",
    "eth_getTransactionByBlockHashAndIndex",
}
BLOCK_SCOPED_METHODS = {
    # method -> index of the block-scoped param (None = not cacheable here)
    "eth_getBlockByNumber": 0,
    "eth_getTransactionReceipt": None,  # receipt of an old tx is stable; scoped via block lookup below
}
LOG_METHOD = "eth_getLogs"
# Methods where an empty result IS a stable answer (historical range).
EMPTY_CACHEABLE_METHODS = {LOG_METHOD}
EMPTY_RESULTS = {'"0x"', "null", "{}", "[]"}

_cache: OrderedDict[tuple[str, str], tuple[float, dict[str, Any]]] = OrderedDict()
# Last observed chain head per chain — lets a read decide "below confirmations"
# without an extra eth_blockNumber round trip.
_heads: dict[str, int] = {}


@dataclass(frozen=True)
class _Scope:
    """Cache placement decision for one call."""

    ttl: float
    key: str
    from_block: int | None = None
    to_block: int | None = None
    block_number: int | None = None
    block_hash: str | None = None


def _canonical(params: list[Any] | None) -> str:
    return repr(params or [])


def _params_hash(params: list[Any] | None) -> str:
    return hashlib.sha256(_canonical(params).encode("utf-8", "replace")).hexdigest()[:32]


def _empty_result(result: Any) -> bool:
    """NodeCore's EmptyResponses guard: never cache nothing."""
    if result is None:
        return True
    text = result if isinstance(result, str) else repr(result)
    return text.strip() in EMPTY_RESULTS


def _parse_int(value: Any) -> int | None:
    try:
        if isinstance(value, str) and value.startswith("0x"):
            return int(value, 16)
        return int(value)
    except (TypeError, ValueError):
        return None


def note_head(chain: str | None, tip: int | None) -> None:
    """Record the latest observed head for ``chain`` (monotonic max)."""
    if not chain or tip is None:
        return
    if tip > _heads.get(chain, -1):
        _heads[chain] = int(tip)


def known_head(chain: str | None) -> int | None:
    """Latest observed head for ``chain`` (None when never seen)."""
    if not chain:
        return None
    return _heads.get(chain)


def _confirmed(block: int | None, tip: int | None) -> bool:
    """Whether ``block`` is at least ``CONFIRMATIONS`` below a known tip."""
    if block is None or tip is None:
        return False
    return block <= (tip - RPC_CACHE_CONFIRMATIONS)


def _l2_writable(scope: _Scope, chain: str | None, tip: int | None) -> bool:
    """Whether a cache decision may be persisted to the durable L2 store.

    L2 survives restarts, so it must only hold reads that are immutable by
    content (identity / hash-scoped) or provably below the confirm depth of
    the TRUSTED chain head. The per-call ``tip`` can come from a single
    endpoint that is stale or ahead; the guard therefore checks against
    ``known_head`` — the pool's monotonic observed head — and falls back to
    the caller's tip only when no trusted head has been recorded yet. This
    closes the sawtooth case where a volatile near-tip answer was written to
    L2 because one endpoint reported a higher head than the rest of the pool.
    """
    if scope.block_hash is not None:
        return True  # hash-scoped reads are immutable by content
    if scope.from_block is None and scope.block_number is None:
        return True  # identity read
    target = scope.to_block if scope.to_block is not None else scope.block_number
    if target is None:
        return False
    trusted = known_head(chain)
    if trusted is None:
        trusted = tip
    if trusted is None:
        return False
    return _confirmed(target, trusted)


def _decide(
    method: str,
    params: list[Any] | None,
    *,
    chain: str | None,
    from_block: int | None,
    to_block: int | None,
    block_hash: str | None,
    block_number: int | None,
    tip: int | None,
) -> _Scope | None:
    """Return the cache placement for this call, or None when volatile."""
    resolved_tip = tip if tip is not None else known_head(chain)

    if method in IDENTITY_METHODS:
        return _Scope(ttl=IDENTITY_TTL_S, key=f"id:{_params_hash(params)}")

    if method in HASH_SCOPED_METHODS and block_hash:
        scope = f"bh:{block_hash}"
        return _Scope(ttl=RPC_CACHE_TTL_S, block_hash=block_hash, key=scope)

    if method == "eth_getBlockByNumber":
        number = block_number if block_number is not None else _parse_int(params[0] if params else None)
        if block_hash:
            return _Scope(ttl=RPC_CACHE_TTL_S, block_number=number, block_hash=block_hash, key=f"bh:{block_hash}")
        if not _confirmed(number, resolved_tip):
            return None
        return _Scope(ttl=RPC_CACHE_TTL_S, block_number=number, key=f"bn:{number}")

    if method == LOG_METHOD:
        if from_block is None or to_block is None:
            return None
        if not _confirmed(to_block, resolved_tip):
            return None
        scope = f"r:{int(from_block)}:{int(to_block)}"
        return _Scope(
            ttl=RPC_CACHE_TTL_S,
            from_block=int(from_block),
            to_block=int(to_block),
            key=scope,
        )
    return None


def _l2_key(chain: str | None, method: str, params: list[Any] | None, scope: str) -> str:
    return f"{chain or '*'}|{method}|{_params_hash(params)}|{scope}"


def _evict_l1_if_full() -> None:
    while len(_cache) >= MAX_ENTRIES:
        _cache.popitem(last=False)
        metrics_incr("rpc_cache_evictions")


def _l2_get(scope: _Scope, chain: str | None, method: str, params: list[Any] | None) -> dict[str, Any] | None:
    try:
        raw = cache_db.get_db().get(_l2_key(chain, method, params, scope.key))
    except Exception as exc:  # noqa: BLE001 — L2 must never break an L1 read
        logger.debug("rpc_cache_l2_get_failed", method=method, error=str(exc)[:100])
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _l2_put(
    scope: _Scope,
    chain: str | None,
    method: str,
    params: list[Any] | None,
    response: dict[str, Any],
) -> None:
    try:
        cache_db.get_db().put(
            _l2_key(chain, method, params, scope.key),
            chain=chain or "*",
            method=method,
            payload=json.dumps(response, separators=(",", ":"), default=str),
            from_block=scope.from_block,
            to_block=scope.to_block,
            block_number=scope.block_number,
            block_hash=scope.block_hash,
            ttl=scope.ttl,
        )
    except Exception as exc:  # noqa: BLE001 — L2 write is best-effort
        logger.debug("rpc_cache_l2_put_failed", method=method, error=str(exc)[:100])


def get(
    method: str,
    params: list[Any] | None = None,
    *,
    chain: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    block_hash: str | None = None,
    block_number: int | None = None,
    tip: int | None = None,
) -> dict[str, Any] | None:
    """Cached response for a deterministic call, consulting L1 then L2.

    Returns None for volatile/uncacheable calls (no metric emitted for those)
    and for a genuine miss (``rpc_cache_misses``).
    """
    if not CACHE_ENABLED:
        return None
    scope = _decide(
        method,
        params,
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        block_hash=block_hash,
        block_number=block_number,
        tip=tip,
    )
    if scope is None:
        return None

    key = (method, _canonical(params))
    hit = _cache.get(key)
    if hit is not None:
        expires, cached_payload = hit
        if time.time() > expires:
            _cache.pop(key, None)
        else:
            _cache.move_to_end(key)
            metrics_incr("rpc_cache_hits")
            logger.debug("rpc_respcache_hit", method=method, tier="l1")
            return cached_payload

    l2_payload = _l2_get(scope, chain, method, params)
    if l2_payload is not None:
        _evict_l1_if_full()
        _cache[key] = (time.time() + scope.ttl, l2_payload)
        metrics_incr("rpc_cache_hits")
        logger.debug("rpc_respcache_hit", method=method, tier="l2")
        return l2_payload

    metrics_incr("rpc_cache_misses")
    return None


def put(
    method: str,
    params: list[Any] | None,
    response: dict[str, Any] | None,
    *,
    chain: str | None = None,
    from_block: int | None = None,
    to_block: int | None = None,
    block_hash: str | None = None,
    block_number: int | None = None,
    tip: int | None = None,
) -> bool:
    """Cache a successful response in L1 + L2 under finality rules.

    Returns True when stored. Empty results are rejected except for
    historical getLogs ranges (where empty is a stable "no logs").
    """
    if not CACHE_ENABLED or response is None:
        return False
    scope = _decide(
        method,
        params,
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        block_hash=block_hash,
        block_number=block_number,
        tip=tip,
    )
    if scope is None:
        return False
    result = response.get("result") if isinstance(response, dict) else None
    if method not in EMPTY_CACHEABLE_METHODS and _empty_result(result):
        return False

    # L2 (durable) guard must be evaluated against the head known BEFORE this
    # call's tip is recorded, otherwise a stale/ahead tip would vouch for
    # itself. L1 still caches; only the durable write is withheld.
    l2_ok = _l2_writable(scope, chain, tip)

    if tip is not None:
        note_head(chain, tip)

    key = (method, _canonical(params))
    _evict_l1_if_full()
    _cache[key] = (time.time() + scope.ttl, response)
    if l2_ok:
        _l2_put(scope, chain, method, params, response)
    else:
        metrics_incr("cache_write_skipped_stale_tip")
        logger.debug(
            "rpc_cache_l2_skip_stale_tip",
            method=method,
            chain=chain,
            block_number=scope.block_number,
            to_block=scope.to_block,
        )
    return True


def invalidate_range(chain: str, from_block: int, to_block: int) -> int:
    """Purge L1 + L2 entries affected by a reorg/rewind of a block range.

    L1 is range-less, so it is cleared wholesale (cheap, conservative); L2
    deletes exactly the overlapping range/block rows. Returns L2 rows deleted.
    """
    _cache.clear()
    try:
        return cache_db.get_db().invalidate_range(chain, from_block, to_block)
    except Exception as exc:  # noqa: BLE001 — invalidation must never raise
        logger.debug("rpc_cache_invalidate_failed", chain=chain, error=str(exc)[:100])
        return 0


def clear() -> None:
    """Clear the in-process L1 only (L2 persists — see :func:`clear_persistent`)."""
    _cache.clear()


def clear_persistent() -> int:
    """Clear the L2 store (tests/ops). Returns rows deleted."""
    try:
        return cache_db.get_db().clear()
    except Exception as exc:  # noqa: BLE001
        logger.debug("rpc_cache_clear_persistent_failed", error=str(exc)[:100])
        return 0


def reset(db_path: str | None = None) -> None:
    """Reset L1 and rebuild the L2 store (tests/ops); file is preserved.

    ``db_path`` overrides the store location; omitted keeps the current one.
    """
    _cache.clear()
    _heads.clear()
    if db_path is not None:
        cache_db.reset_db(db_path)
    else:
        cache_db.reset_db(cache_db.configured_path())


def stats() -> dict[str, int]:
    """L1 entry count plus L2 store stats."""
    out: dict[str, int] = {"entries": len(_cache), "max": MAX_ENTRIES}
    try:
        for key, value in cache_db.get_db().stats().items():
            out[f"l2_{key}"] = int(value)
    except Exception as exc:  # noqa: BLE001 — stats must never raise
        logger.debug("rpc_cache_stats_failed", error=str(exc)[:100])
    return out


__all__ = [
    "clear",
    "clear_persistent",
    "get",
    "invalidate_range",
    "known_head",
    "note_head",
    "put",
    "reset",
    "stats",
]
