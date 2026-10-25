"""Reorg invalidation, clearing and stats for the durable RPC cache.

Range invalidation is reorg-aware: any entry whose block range or block
number overlaps the rewound span is purged, while hash-keyed entries (which
identify immutable content) are left alone.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.rpc.cache_conn import CacheConn

logger = get_logger(__name__)


class CacheMaintMixin(CacheConn):
    """Maintenance operations for :class:`ResponseCacheDB`."""

    def invalidate_range(self, chain: str, from_block: int, to_block: int) -> int:
        """Purge entries whose block range/number overlaps ``[from, to]``.

        Rows keyed only by block hash are not range-addressable and are left
        alone (a hash identifies immutable content). Returns rows deleted.
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


__all__ = ["CacheMaintMixin"]
