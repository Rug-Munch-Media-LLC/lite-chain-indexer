"""Coverage tests for ``lite_indexer.query.cache`` positive/negative TTLs.

Hermetic — pure in-memory cache, no network. Complements the existing
``test_query_router.py`` cache tests by pinning the positive/negative
write-through contract: positives never expire, negatives expire on the
negative TTL, and eviction prefers the soonest-expiring entry.
"""

from __future__ import annotations

import time

from lite_indexer.query.cache import NEGATIVE_RESULT_TTL, QueryCache


def test_positive_entry_never_expires() -> None:
    cache = QueryCache(ttl=0.01)
    cache.set_positive(("k",), "v")
    time.sleep(0.02)
    assert cache.get(("k",)) == "v"


def test_negative_entry_expires_on_negative_ttl() -> None:
    cache = QueryCache(ttl=3600, negative_ttl=0.01)
    cache.set_negative(("k",), "not-found")
    assert cache.get(("k",)) == "not-found"
    time.sleep(0.02)
    assert cache.get(("k",)) is None


def test_negative_ttl_default_is_300_seconds() -> None:
    assert NEGATIVE_RESULT_TTL == 300.0


def test_plain_set_uses_configured_ttl() -> None:
    cache = QueryCache(ttl=0.01)
    cache.set(("k",), "v")
    time.sleep(0.02)
    assert cache.get(("k",)) is None


def test_explicit_ttl_overrides_configured_ttl() -> None:
    cache = QueryCache(ttl=0.01)
    cache.set(("k",), "v", ttl=3600)
    time.sleep(0.02)
    assert cache.get(("k",)) == "v"


def test_eviction_prefers_soonest_expiring_entry() -> None:
    cache = QueryCache(ttl=3600, max_entries=2)
    cache.set_positive(("never",), "immutable")
    cache.set(("soon",), "expiring", ttl=0.01)
    cache.set(("new",), "fresh")
    # The expiring entry is evicted first; the never-expiring one survives.
    assert cache.get(("never",)) == "immutable"
    assert cache.get(("new",)) == "fresh"


def test_clear_drops_every_entry() -> None:
    cache = QueryCache()
    cache.set(("a",), 1)
    cache.set_positive(("b",), 2)
    cache.clear()
    assert len(cache) == 0


def test_stats_reports_entries() -> None:
    cache = QueryCache()
    cache.set(("a",), 1)
    stats = cache.stats()
    assert stats["entries"] == 1
    assert "hits" in stats and "misses" in stats


def test_expired_entry_is_removed_lazily_on_read() -> None:
    cache = QueryCache(ttl=0.01)
    cache.set(("k",), "v")
    time.sleep(0.02)
    assert cache.get(("k",)) is None
    assert len(cache) == 0
