"""
Query cache — bounded TTL cache for on-demand RPC results.

The QueryRouter's RPC leg is the expensive one (free tiers are
rate-limited), so every fetched result is cached here with a TTL
(default ``ON_DEMAND_RPC_CACHE_TTL`` = 1h). Repeated queries for the
same tx / address range / block / contract resolve from memory instead
of dialing the RPC pool again.

Thread-safe by design: the API runs sync endpoints on a threadpool and
bridges to the async router on a dedicated event loop, so the cache is
guarded by a lock and can be shared across threads.

Eviction is a simple FIFO scan once the cache exceeds ``max_entries``
(the working set is small — a handful of on-demand lookups per minute).
"""

from __future__ import annotations

import threading
import time
from typing import Any

from lite_indexer.config import (
    ON_DEMAND_RPC_CACHE_MAX_ENTRIES,
    ON_DEMAND_RPC_CACHE_TTL,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# A cache key is an opaque tuple (chain, kind, *params) built by the
# QueryRouter — e.g. ("ethereum", "transfer", "0xabc...") or
# ("base", "transfers", "0xwallet", 1234, 5678, 100).
CacheKey = tuple[Any, ...]


class QueryCache:
    """Thread-safe, TTL-bounded in-memory cache for on-demand RPC results.

    Args:
        ttl: Entry lifetime in seconds (default 3600).
        max_entries: Soft cap on stored entries; exceeding it triggers a
            FIFO eviction pass (default 2048).

    Attributes:
        hits: Number of cache hits (diagnostics).
        misses: Number of cache misses (diagnostics).
    """

    def __init__(
        self,
        ttl: float = ON_DEMAND_RPC_CACHE_TTL,
        max_entries: int = ON_DEMAND_RPC_CACHE_MAX_ENTRIES,
    ) -> None:
        self._ttl = float(ttl)
        self._max_entries = int(max_entries)
        self._data: dict[CacheKey, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: CacheKey) -> Any | None:
        """Return the cached value for ``key``, or None when absent/expired.

        Expired entries are removed lazily on read. ``None`` is never a
        valid cached payload (the router only caches non-None results), so
        a None return unambiguously means "not cached".
        """
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            expires_at, value = entry
            if time.monotonic() >= expires_at:
                self._data.pop(key, None)
                self.misses += 1
                return None
            self.hits += 1
            return value

    def set(self, key: CacheKey, value: Any) -> None:
        """Cache ``value`` under ``key`` for the configured TTL."""
        with self._lock:
            if len(self._data) >= self._max_entries:
                self._evict_locked()
            self._data[key] = (time.monotonic() + self._ttl, value)

    def _evict_locked(self) -> None:
        """Drop expired entries; if still over budget, drop oldest by expiry."""
        now = time.monotonic()
        expired = [k for k, (exp, _) in self._data.items() if now >= exp]
        for k in expired:
            self._data.pop(k, None)
        # Still over budget: evict the entry expiring soonest (FIFO-ish).
        if len(self._data) >= self._max_entries and self._data:
            oldest = min(self._data, key=lambda k: self._data[k][0])
            self._data.pop(oldest, None)

    def clear(self) -> None:
        """Drop every cached entry."""
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        """Return the number of cached entries (non-expired not enforced)."""
        return len(self._data)

    def stats(self) -> dict[str, int]:
        """Return diagnostic counters."""
        return {"hits": self.hits, "misses": self.misses, "entries": len(self._data)}

    def __repr__(self) -> str:  # pragma: no cover — debug aid
        return f"<QueryCache entries={len(self._data)} hits={self.hits} misses={self.misses}>"


__all__ = ["QueryCache", "CacheKey"]
