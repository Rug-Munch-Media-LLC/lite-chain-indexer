"""Connection lifecycle for the durable RPC cache (WAL SQLite).

Owns opening/closing the database and the schema bootstrap. Every failure
degrades to ``None`` (no connection) so a broken or missing database can
never break an RPC read.
"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

from lite_indexer.logging import get_logger
from lite_indexer.rpc.cache_schema import _SCHEMA

logger = get_logger(__name__)


class CacheConn:
    """Holds the SQLite connection, its lock and the sizing knobs."""

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


__all__ = ["CacheConn"]
