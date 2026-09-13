"""Bounded, LRU-ordered containers for long-lived in-process caches.

The daemon runs for weeks; any dict or set keyed by chain entities
(pairs, mints, files, tips) grows without an explicit ceiling and shows
up as the slow RSS climb that OOM-killed the service on 2026-09-13.
These containers add a hard cap and evict the oldest entry first.

Both are deliberately small: they implement only the surface the
indexer uses so behaviour stays obvious and testable.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, Mapping, MutableMapping
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class BoundedDict(MutableMapping[K, V], Generic[K, V]):
    """Dict with a hard item cap; writing past it evicts the oldest key.

    Reads and writes move the touched key to the most-recent position, so
    eviction is LRU. ``maxsize`` must be positive.
    """

    def __init__(self, maxsize: int, initial: Mapping[K, V] | None = None) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._maxsize = maxsize
        self._data: OrderedDict[K, V] = OrderedDict()
        if initial:
            for key, value in initial.items():
                self[key] = value

    @property
    def maxsize(self) -> int:
        """Maximum number of retained items."""
        return self._maxsize

    def __getitem__(self, key: K) -> V:
        value = self._data[key]
        self._data.move_to_end(key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        if key in self._data:
            self._data.move_to_end(key)
        self._data[key] = value
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def __delitem__(self, key: K) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


class BoundedSet(Generic[K]):
    """Insertion-ordered set with a hard item cap (oldest evicted first).

    Used where only membership + de-dup is needed but the key space is
    unbounded over a daemon's lifetime (e.g. seen launchpad mints).
    """

    def __init__(self, maxsize: int) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._maxsize = maxsize
        self._data: OrderedDict[K, None] = OrderedDict()

    @property
    def maxsize(self) -> int:
        """Maximum number of retained items."""
        return self._maxsize

    def add(self, item: K) -> None:
        """Add ``item``; evict the oldest entry when at capacity."""
        if item in self._data:
            self._data.move_to_end(item)
            return
        self._data[item] = None
        while len(self._data) > self._maxsize:
            self._data.popitem(last=False)

    def discard(self, item: K) -> None:
        """Remove ``item`` if present (no error when absent)."""
        self._data.pop(item, None)

    def __contains__(self, item: object) -> bool:
        return item in self._data

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)
