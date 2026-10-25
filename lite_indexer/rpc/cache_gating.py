"""Durable gating state (capability verdicts / endpoint health / bans).

A small, bounded key/value table that survives restarts. Every method is
exception-safe: a read-only or broken DB degrades to a cache miss and the
caller re-derives the verdict.
"""

from __future__ import annotations

import time

from lite_indexer.logging import get_logger
from lite_indexer.rpc.cache_conn import CacheConn
from lite_indexer.rpc.cache_schema import GATING_STATE_MAX_ENTRIES

logger = get_logger(__name__)

#: Gating-state kinds whose write already failed in this process (one
#: warning each — never a per-write log storm).
_WRITE_FAILED_KINDS: set[str] = set()


class GatingStateMixin(CacheConn):
    """Key/value gating state for :class:`ResponseCacheDB`."""

    def state_get(self, kind: str, key: str, now: float | None = None) -> str | None:
        """Return a gating-state payload, or None (miss / expired / error)."""
        conn = self._connect()
        if conn is None:
            return None
        now = time.time() if now is None else now
        try:
            with self._lock:
                row = conn.execute(
                    "SELECT payload, expires_at FROM gating_state WHERE kind = ? AND key = ?",
                    (kind, key),
                ).fetchone()
                if row is None:
                    return None
                payload, expires_at = row
                if float(expires_at) <= now:
                    conn.execute("DELETE FROM gating_state WHERE kind = ? AND key = ?", (kind, key))
                    conn.commit()
                    return None
                return str(payload)
        except Exception as exc:  # noqa: BLE001
            logger.debug("gating_state_get_failed", kind=kind, error=str(exc)[:100])
            return None

    def state_put(
        self,
        kind: str,
        key: str,
        payload: str,
        *,
        ttl: float = 0.0,
        now: float | None = None,
    ) -> bool:
        """Upsert one gating-state entry (ttl <= 0 means no expiry)."""
        conn = self._connect()
        if conn is None:
            return False
        now = time.time() if now is None else now
        expires_at = now + ttl if ttl > 0 else float("inf")
        try:
            with self._lock:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO gating_state
                        (kind, key, payload, updated_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (kind, key, payload, now, expires_at),
                )
                if GATING_STATE_MAX_ENTRIES > 0:
                    row = conn.execute("SELECT COUNT(*) FROM gating_state").fetchone()
                    count = int(row[0]) if row else 0
                    if count > GATING_STATE_MAX_ENTRIES:
                        conn.execute(
                            "DELETE FROM gating_state WHERE rowid IN "
                            "(SELECT rowid FROM gating_state ORDER BY updated_at ASC LIMIT ?)",
                            (count - GATING_STATE_MAX_ENTRIES,),
                        )
                conn.commit()
            return True
        except Exception as exc:  # noqa: BLE001
            # WARNING, not debug: this is how 2026-10-16..23 happened — the
            # capability/health/ban store was read-only (root-owned volume,
            # uid 1000 process) and every verdict was silently dropped, so
            # each restart re-derived "unknown capability" and the pool gauge
            # read 0. Log once per kind per process, then stay quiet.
            if kind not in _WRITE_FAILED_KINDS:
                _WRITE_FAILED_KINDS.add(kind)
                logger.warning(
                    "gating_state_put_failed",
                    kind=kind,
                    error=str(exc)[:120],
                    path=self.path[:80],
                )
            return False

    def state_delete(self, kind: str, key: str) -> None:
        """Remove one gating-state entry (best-effort)."""
        conn = self._connect()
        if conn is None:
            return
        try:
            with self._lock:
                conn.execute("DELETE FROM gating_state WHERE kind = ? AND key = ?", (kind, key))
                conn.commit()
        except Exception as exc:  # noqa: BLE001
            logger.debug("gating_state_delete_failed", kind=kind, error=str(exc)[:100])

    def state_load(self, kind: str, now: float | None = None) -> dict[str, str]:
        """Return every live entry for ``kind`` as ``{key: payload}``."""
        conn = self._connect()
        if conn is None:
            return {}
        now = time.time() if now is None else now
        try:
            with self._lock:
                conn.execute("DELETE FROM gating_state WHERE expires_at <= ?", (now,))
                conn.commit()
                rows = conn.execute(
                    "SELECT key, payload FROM gating_state WHERE kind = ?",
                    (kind,),
                ).fetchall()
            return {str(k): str(v) for k, v in rows}
        except Exception as exc:  # noqa: BLE001
            logger.debug("gating_state_load_failed", kind=kind, error=str(exc)[:100])
            return {}

    def state_count(self, kind: str) -> int:
        """Number of live entries for ``kind`` (0 when unavailable)."""
        conn = self._connect()
        if conn is None:
            return 0
        try:
            with self._lock:
                row = conn.execute("SELECT COUNT(*) FROM gating_state WHERE kind = ?", (kind,)).fetchone()
            return int(row[0]) if row else 0
        except Exception as exc:  # noqa: BLE001
            logger.debug("gating_state_count_failed", kind=kind, error=str(exc)[:100])
            return 0


__all__ = ["GATING_STATE_MAX_ENTRIES", "GatingStateMixin"]
