"""Persistent L2 response cache — SQLite (WAL) behind the in-memory LRU.

Facade over the cohesive cache modules; every public name that existed before
the split is still importable from here:

* :mod:`lite_indexer.rpc.cache_schema` — DDL + sizing constants
* :mod:`lite_indexer.rpc.cache_conn` — connection / lifecycle
* :mod:`lite_indexer.rpc.cache_rows` — payload reads + writes
* :mod:`lite_indexer.rpc.cache_maint` — reorg invalidation, clear, stats
* :mod:`lite_indexer.rpc.cache_gating` — capability/health/ban gating state
* :mod:`lite_indexer.rpc.cache_store` — the assembled ``ResponseCacheDB``

The tier is bounded by ``RPC_CACHE_MAX_ENTRIES`` + ``RPC_CACHE_MAX_MB`` with
LRU-ish eviction and TTL pruning, reorg-aware range invalidation, and is
failure-safe: every operation swallows DB errors and reports a miss / no-op,
so a broken or missing database can never break an RPC read.

Stored keys are opaque strings built by ``rpc/respcache.py``. The payload is
canonical JSON text. This module never sees provider names or secrets.
"""

from __future__ import annotations

from lite_indexer.config import (
    RPC_CACHE_DB,
    RPC_CACHE_ENABLED,
    RPC_CACHE_MAX_ENTRIES,
    RPC_CACHE_MAX_MB,
    RPC_CACHE_TTL_S,
)
from lite_indexer.rpc.cache_gating import GATING_STATE_MAX_ENTRIES
from lite_indexer.rpc.cache_store import ResponseCacheDB

# ── Process-wide singleton ─────────────────────────────────────────────

_DB: ResponseCacheDB | None = None
_OVERRIDE_PATH: str | None = None


def get_db() -> ResponseCacheDB:
    """Return the process-wide L2 store, building it lazily from config."""
    global _DB
    if _DB is None:
        _DB = ResponseCacheDB(
            _OVERRIDE_PATH or RPC_CACHE_DB,
            max_entries=RPC_CACHE_MAX_ENTRIES,
            ttl_s=RPC_CACHE_TTL_S,
            max_mb=RPC_CACHE_MAX_MB,
            enabled=RPC_CACHE_ENABLED,
        )
    return _DB


def reset_db(path: str | None = None) -> None:
    """Drop the singleton (tests/ops).

    When ``path`` is given it becomes the override for the next connection;
    when omitted the current effective path (override or config) is kept, so
    a caller can rebuild the store in place without losing its location.
    """
    global _DB, _OVERRIDE_PATH
    if _DB is not None:
        _DB.close()
    _DB = None
    if path is not None:
        _OVERRIDE_PATH = path


def configured_path() -> str:
    """The path the next :func:`get_db` call will use."""
    return _OVERRIDE_PATH or RPC_CACHE_DB


# ── Module-level gating-state wrappers ─────────────────────────────────


def state_get(kind: str, key: str, now: float | None = None) -> str | None:
    """Module-level gating-state read (see :meth:`ResponseCacheDB.state_get`)."""
    return get_db().state_get(kind, key, now)


def state_put(kind: str, key: str, payload: str, *, ttl: float = 0.0, now: float | None = None) -> bool:
    """Module-level gating-state upsert (see :meth:`ResponseCacheDB.state_put`)."""
    return get_db().state_put(kind, key, payload, ttl=ttl, now=now)


def state_delete(kind: str, key: str) -> None:
    """Module-level gating-state delete (see :meth:`ResponseCacheDB.state_delete`)."""
    get_db().state_delete(kind, key)


def state_load(kind: str, now: float | None = None) -> dict[str, str]:
    """Module-level gating-state bulk read (see :meth:`ResponseCacheDB.state_load`)."""
    return get_db().state_load(kind, now)


def state_count(kind: str) -> int:
    """Module-level gating-state count (see :meth:`ResponseCacheDB.state_count`)."""
    return get_db().state_count(kind)


__all__ = [
    "GATING_STATE_MAX_ENTRIES",
    "RPC_CACHE_DB",
    "RPC_CACHE_ENABLED",
    "RPC_CACHE_MAX_ENTRIES",
    "RPC_CACHE_MAX_MB",
    "RPC_CACHE_TTL_S",
    "ResponseCacheDB",
    "configured_path",
    "get_db",
    "reset_db",
    "state_count",
    "state_delete",
    "state_get",
    "state_load",
    "state_put",
]
