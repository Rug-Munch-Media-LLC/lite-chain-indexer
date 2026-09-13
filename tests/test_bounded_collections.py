"""Bounded-collection tests — caches must evict, not grow forever.

Covers the reusable helpers (``BoundedDict`` / ``BoundedSet``) plus the
two pre-existing bounded caches the daemon relies on (``PairCache``,
``BloomCache``). One eviction test per container kind.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer import state
from lite_indexer.strategies.evm.bloom import BloomCache
from lite_indexer.util.bounded import BoundedDict, BoundedSet


def test_bounded_dict_evicts_oldest_at_capacity() -> None:
    cache: BoundedDict[str, int] = BoundedDict(2)
    cache["a"] = 1
    cache["b"] = 2
    cache["c"] = 3

    assert len(cache) == 2
    assert "a" not in cache
    assert cache["b"] == 2
    assert cache["c"] == 3


def test_bounded_dict_read_refreshes_lru_order() -> None:
    cache: BoundedDict[str, int] = BoundedDict(2)
    cache["a"] = 1
    cache["b"] = 2
    _ = cache["a"]  # touch "a" so "b" becomes the eviction candidate
    cache["c"] = 3

    assert "b" not in cache
    assert cache["a"] == 1


def test_bounded_set_evicts_oldest_at_capacity() -> None:
    seen: BoundedSet[str] = BoundedSet(2)
    seen.add("a")
    seen.add("b")
    seen.add("c")
    seen.add("b")  # re-add must not grow past the cap

    assert len(seen) == 2
    assert "a" not in seen
    assert "b" in seen


def test_pair_cache_evicts_oldest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(state, "STATE_DIR", tmp_path)
    monkeypatch.setattr(state, "PAIR_CACHE_MAX", 2)
    cache = state.PairCache("ethereum")
    cache.set("0xaaa", ("t0", "t1"))
    cache.set("0xbbb", ("t0", "t1"))
    cache.set("0xccc", ("t0", "t1"))

    assert cache.count == 2
    assert cache.get("0xaaa") is None
    assert cache.get("0xccc") == ("t0", "t1")


async def test_bloom_cache_evicts_oldest() -> None:
    cache = BloomCache(max_size=2, ttl_seconds=3600)
    cache._redis_url = ""  # never touch Redis in this test
    bloom = "00" * 256

    await cache.set("ethereum", 1, bloom)
    await cache.set("ethereum", 2, bloom)
    await cache.set("ethereum", 3, bloom)

    assert cache.size == 2
    assert await cache.get("ethereum", 1) is None
    assert await cache.get("ethereum", 3) is not None
