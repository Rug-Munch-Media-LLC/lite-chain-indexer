"""
Persistent L2 response cache — SQLite (WAL) behind the in-memory LRU.

The in-process cache (``rpc/respcache.py``) dies on restart, so backfill and
reconciliation re-fetch immutable history every process lifetime. This module
adds a durable tier:

  * SQLite in WAL mode (concurrent readers, durable commits),
  * bounded by ``RPC_CACHE_MAX_ENTRIES`` + ``RPC_CACHE_MAX_MB`` with LRU-ish
    eviction (oldest ``accessed_at`` first) and TTL pruning,
  * reorg-aware range invalidation (``invalidate_range``),
  * **failure-safe**: every operation swallows DB errors and reports a miss
    / no-op, so a broken or missing database can never break an RPC read.

Stored keys are opaque strings built by ``rpc/respcache.py``. The payload is
canonical JSON text. This module never sees provider names or secrets.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from lite_indexer.config import (
    RPC_CACHE_DB,
    RPC_CACHE_ENABLED,
    RPC_CACHE_MAX_ENTRIES,
    RPC_CACHE_MAX_MB,
    RPC_CACHE_TTL_S,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rpc_cache (
    key          TEXT PRIMARY KEY,
    chain        TEXT NOT NULL,
    method       TEXT NOT NULL,
    from_block   INTEGER,
    to_block     INTEGER,
    block_number INTEGER,
    block_hash   TEXT,
    payload      TEXT NOT NULL,
    size_bytes   INTEGER NOT NULL,
    hits         INTEGER NOT NULL DEFAULT 0,
    created_at   REAL NOT NULL,
    accessed_at  REAL NOT NULL,
    expires_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_expires ON rpc_cache(expires_at);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_range ON rpc_cache(chain, from_block, to_block);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_block ON rpc_cache(chain, block_number);
CREATE INDEX IF NOT EXISTS idx_rpc_cache_accessed ON rpc_cache(accessed_at);
"""

# Enforce the size cap every N writes (SUM over 200k rows is not free).
_SIZE_CHECK_EVERY = 32


class ResponseCacheDB:
    """Durable SQLite cache store. All methods are exception-safe."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_entries: int,
        ttl_s: float,
        max_mb: int,
        enabled: bool = True,
    ) -> None:
        self.path = str(path)
        self.max_entries = max(0, int(max_entries))
        self.ttl_s = max(0.0, float(ttl_s))
        self.max_mb = max(0, int(max_mb))
        self.enabled = enabled
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        self._writes = 0

    # ── Connection / lifecycle ────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection | None:
        """Open (once) the WAL database; None when unavailable."""
        if not self.enabled:
            return None
        if self._conn is not None:
            return self._conn
        try:
            db_path = Path(self.path)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=5.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.executescript(_SCHEMA)
            conn.commit()
            self._conn = conn
        except Exception as exc:  # noqa: BLE001 — cache must never break reads
            logger.debug("rpc_cache_db_unavailable", path=self.path[:80], error=str(exc)[:100])
            self._conn = None
        return self._conn

    def close(self) -> None:
        """Close the connection (idempotent)."""
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:  # noqa: BLE001
                logger.debug("rpc_cache_db_close_failed", error=str(exc)[:80])

    # ── Reads ─────────────────────────────────────────────────────────

    def get(self, key: str, now: float | None = None) -> str | None:
        """Return the cached payload text, or None (miss / expired / error)."""
        conn = self._connect()
        if conn is None:
            return None
        now = time.time() if now is None else now
        try:
            with self._lock:
                row = conn.execute("SELECT payload, expires_at FROM rpc_cache WHERE key = ?", (key,)).fetchone()
                if row is None:
                    return None
                payload, expires_at = row
                if float(expires_at) <= now:
                    conn.execute("DELETE FROM rpc_cache WHERE key = ?", (key,))
                    conn.commit()
                    return None
                conn.execute(
                    "UPDATE rpc_cache SET hits = hits + 1, accessed_at = ? WHERE key = ?",
                    (now, key),
                )
                conn.commit()
                return str(payload)
        except Exception as exc:  # noqa: BLE001
            logger.debug("rpc_cache_db_get_failed", error=str(exc)[:100])
            return None

    # ── Writes ────────────────────────────────────────────────────────

    def put(
        self,
        key: str,
        *,
        chain: str,
        method: str,
        payload: str,
        from_block: int | None = None,
        to_block: int | None = None,
        block_number: int | None = None,
        block_hash: str | None = None,
        ttl: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Insert/replace one entry, then enforce TTL + caps. False on error."""
        conn = self._connect()
        if conn is None:
            return False
        now = time.time() if now is None else now
        ttl = self.ttl_s if ttl is None else max(0.0, ttl)
        try:
            with self._lock:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO rpc_cache
                        (key, chain, method, from_block, to_block, block_number,
                         block_hash, payload, size_bytes, hits, created_at,
                         accessed_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        key,
                        chain,
                        method,
                        from_block,
                        to_block,
                        block_number,
                        block_hash,
                        payload,
                        len(payload.encode("utf-8", "replace")),
                        now,
                        now,
                        now + ttl,
                    ),
                )
                evicted = self._enforce_limits(conn, now)
                conn.commit()
            self._writes += 1
            if evicted:
                metrics_incr("rpc_cache_evictions", float(evicted))
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("rpc_cache_db_put_failed", error=str(exc)[:100])
            return False

    def _enforce_limits(self, conn: sqlite3.Connection, now: float) -> int:
        """Prune expired rows, then evict oldest until under entry/size caps."""
        evicted = 0
        cursor = conn.execute("DELETE FROM rpc_cache WHERE expires_at <= ?", (now,))
        evicted += max(0, cursor.rowcount)
        if self.max_entries > 0:
            row = conn.execute("SELECT COUNT(*) FROM rpc_cache").fetchone()
            count = int(row[0]) if row else 0
            if count > self.max_entries:
                evicted += self._evict_oldest(conn, count - self.max_entries)
        if self.max_mb > 0 and self._writes % _SIZE_CHECK_EVERY == 0:
            row = conn.execute("SELECT COALESCE(SUM(size_bytes), 0), COUNT(*) FROM rpc_cache").fetchone()
            total, count = (int(row[0]), int(row[1])) if row else (0, 0)
            limit = self.max_mb * 1024 * 1024
            if total > limit and count > 0:
                evicted += self._evict_oldest(conn, max(1, count // 10))
        return evicted

    @staticmethod
    def _evict_oldest(conn: sqlite3.Connection, limit: int) -> int:
        cursor = conn.execute(
            "DELETE FROM rpc_cache WHERE key IN "
            "(SELECT key FROM rpc_cache ORDER BY accessed_at ASC, created_at ASC LIMIT ?)",
            (int(limit),),
        )
        return max(0, cursor.rowcount)

    # ── Invalidation ──────────────────────────────────────────────────

    def invalidate_range(self, chain: str, from_block: int, to_block: int) -> int:
        """Purge entries whose block range/number overlaps ``[from, to]``.

        Rows keyed only by block hash are not range-addressable and are left
        alone (a hash identifies immutable content). Returns rows deleted.

        Note: the ``_range`` predicate uses two parameters per bound.
        """
        conn = self._connect()
        if conn is None:
            return 0
        lo, hi = int(from_block), int(to_block)
        if lo > hi:
            lo, hi = hi, lo
        try:
            with self._lock:
                cursor = conn.execute(
                    """
                    DELETE FROM rpc_cache
                    WHERE chain = ?
                      AND (
                        (from_block IS NOT NULL AND to_block IS NOT NULL
                         AND to_block >= ? AND from_block <= ?)
                        OR (block_number IS NOT NULL AND block_number BETWEEN ? AND ?)
                      )
                    """,
                    (chain, lo, hi, lo, hi),
                )
                deleted = max(0, cursor.rowcount)
                conn.commit()
            return deleted
        except Exception as exc:  # noqa: BLE001
            logger.debug("rpc_cache_db_invalidate_failed", error=str(exc)[:100])
            return 0

    def clear(self) -> int:
        """Delete every entry (tests/ops). Returns rows deleted."""
        conn = self._connect()
        if conn is None:
            return 0
        try:
            with self._lock:
                cursor = conn.execute("DELETE FROM rpc_cache")
                deleted = max(0, cursor.rowcount)
                conn.commit()
            return deleted
        except Exception as exc:  # noqa: BLE001
            logger.debug("rpc_cache_db_clear_failed", error=str(exc)[:100])
            return 0

    def stats(self) -> dict[str, Any]:
        """Entry count + on-disk payload bytes (empty dict when unavailable)."""
        conn = self._connect()
        if conn is None:
            return {}
        try:
            with self._lock:
                row = conn.execute("SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM rpc_cache").fetchone()
            return {
                "entries": int(row[0]) if row else 0,
                "size_bytes": int(row[1]) if row else 0,
                "max_entries": self.max_entries,
                "max_mb": self.max_mb,
            }
        except Exception as exc:  # noqa: BLE001
            logger.debug("rpc_cache_db_stats_failed", error=str(exc)[:100])
            return {}


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


__all__ = [
    "ResponseCacheDB",
    "configured_path",
    "get_db",
    "reset_db",
]
