"""
Query cache — bounded TTL cache for on-demand RPC results.

The QueryRouter's RPC leg is the expensive one (free tiers are
rate-limited), so every fetched result is cached here. Repeated queries
for the same tx / address range / block / contract resolve from memory
instead of dialing the RPC pool again.

Two cache classes matter to the fetch-through write-through contract:

* **Positive rows** (real transactions / receipts / transfer rows) are
  immutable chain data — they are cached with NO expiry
  (``set_positive``). A finalized on-chain fact never changes.
* **Negative results** (not found / pending) are cached for only
  ``NEGATIVE_RESULT_TTL`` (default 300s) via ``set_negative`` — a
  not-yet-mined tx may appear seconds later, so the negative answer must
  not poison the cache forever.

Plain ``set(key, value)`` keeps the legacy behavior (TTL
``ON_DEMAND_RPC_CACHE_TTL`` = 1h) so existing callers are unchanged.

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

# Lifetime (seconds) for negative results (not found / pending). Chain
# data is immutable, so positives never expire; only negatives need a TTL
# so a pending transaction gets re-probed shortly after it is mined.
NEGATIVE_RESULT_TTL: float = 300.0


class QueryCache:
    """Thread-safe, TTL-bounded in-memory cache for on-demand RPC results.

    Args:
        ttl: Entry lifetime in seconds for plain ``set`` (default 3600).
        max_entries: Soft cap on stored entries; exceeding it triggers a
            FIFO eviction pass (default 2048).
        negative_ttl: Lifetime in seconds for ``set_negative`` entries
            (default 300).

    Attributes:
        hits: Number of cache hits (diagnostics).
        misses: Number of cache misses (diagnostics).
    """

    def __init__(
        self,
        ttl: float = ON_DEMAND_RPC_CACHE_TTL,
        max_entries: int = ON_DEMAND_RPC_CACHE_MAX_ENTRIES,
        *,
        negative_ttl: float = NEGATIVE_RESULT_TTL,
    ) -> None:
        self._ttl = float(ttl)
        self._negative_ttl = float(negative_ttl)
        self._max_entries = int(max_entries)
        # value = (expires_at, payload); expires_at None → never expires.
        self._data: dict[CacheKey, tuple[float | None, Any]] = {}
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
            if expires_at is not None and time.monotonic() >= expires_at:
                self._data.pop(key, None)
                self.misses += 1
                return None
            self.hits += 1
            return value

    def set(self, key: CacheKey, value: Any, *, ttl: float | None = None) -> None:
        """Cache ``value`` under ``key``.

        ``ttl=None`` (default) keeps the configured ``self._ttl`` lifetime
        (legacy behavior). Pass an explicit ``ttl`` to override per entry.
        Prefer :meth:`set_positive` for immutable chain rows (no expiry)
        and :meth:`set_negative` for not-found/pending results (negative
        TTL).
        """
        expires_at = time.monotonic() + (self._ttl if ttl is None else float(ttl))
        with self._lock:
            if len(self._data) >= self._max_entries:
                self._evict_locked()
            self._data[key] = (expires_at, value)

    def set_positive(self, key: CacheKey, value: Any) -> None:
        """Cache an immutable positive row with NO expiry.

        Chain data is immutable — a finalized/confirmed transaction,
        receipt or transfer row never changes, so it is cached forever
        (bounded only by ``max_entries`` eviction).
        """
        with self._lock:
            if len(self._data) >= self._max_entries:
                self._evict_locked()
            self._data[key] = (None, value)

    def set_negative(self, key: CacheKey, value: Any) -> None:
        """Cache a negative result (not found / pending) for the negative TTL.

        The short lifetime (default 300s) lets a pending transaction be
        re-probed shortly after it is mined instead of being treated as
        permanently missing.
        """
        self.set(key, value, ttl=self._negative_ttl)

    def _evict_locked(self) -> None:
        """Drop expired entries; if still over budget, drop oldest by expiry."""
        now = time.monotonic()
        expired = [k for k, (exp, _) in self._data.items() if exp is not None and now >= exp]
        for k in expired:
            self._data.pop(k, None)
        # Still over budget: evict the entry expiring soonest (FIFO-ish);
        # never-expiring entries sort last.
        if len(self._data) >= self._max_entries and self._data:
            oldest = min(self._data, key=lambda k: self._data[k][0] if self._data[k][0] is not None else float("inf"))
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


__all__ = ["QueryCache", "CacheKey", "NEGATIVE_RESULT_TTL"]
