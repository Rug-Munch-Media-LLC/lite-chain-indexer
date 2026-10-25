"""Row reads/writes for the durable RPC cache.

``get`` / ``put`` plus the TTL-and-cap eviction that runs after every write.
Exception-safe by contract: a DB error is a cache miss, never a raise.
"""

from __future__ import annotations

import sqlite3
import time

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.cache_conn import CacheConn
from lite_indexer.rpc.cache_schema import SIZE_CHECK_EVERY

logger = get_logger(__name__)


class CacheRowsMixin(CacheConn):
    """Payload row reads and writes for :class:`ResponseCacheDB`."""

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
        if self.max_mb > 0 and self._writes % SIZE_CHECK_EVERY == 0:
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


__all__ = ["CacheRowsMixin"]
