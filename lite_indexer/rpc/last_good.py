"""Bounded last-known-good values per chain.

The point of this module is the opposite of a cache: a cache optimises hit
rate and may silently return yesterday's data, this store exists so that a
degraded serve is *labelled* degraded. Every recall reports its age, and the
caller decides whether that is acceptable by passing ``max_age_s``.

Bounded by construction: one entry per chain, at most ``MAX_CHAINS`` chains,
oldest evicted first. In-process only — a restart legitimately has no
last-known-good, and inventing one from disk is how stale data becomes
indistinguishable from fresh.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass

MAX_CHAINS = 64


@dataclass(frozen=True, slots=True)
class LastGood:
    """A value observed at a point in time.

    Attributes:
        value: The value that was served successfully.
        recorded_at: ``time.monotonic()`` reading at record time.
    """

    value: int
    recorded_at: float

    def age_s(self, now: float | None = None) -> float:
        """Seconds since the value was recorded."""
        return max(0.0, (now if now is not None else time.monotonic()) - self.recorded_at)


_store: OrderedDict[str, LastGood] = OrderedDict()


def remember(chain: str, value: int, now: float | None = None) -> None:
    """Record ``value`` as the last-known-good tip for ``chain``.

    Args:
        chain: Chain name.
        value: The confirmed tip/block number served by a provider.
        now: Monotonic timestamp (injectable for tests).
    """
    stamp = now if now is not None else time.monotonic()
    _store[chain] = LastGood(value=value, recorded_at=stamp)
    _store.move_to_end(chain)
    while len(_store) > MAX_CHAINS:
        _store.popitem(last=False)


def recall(chain: str, *, max_age_s: float | None = None, now: float | None = None) -> LastGood | None:
    """Return the last-known-good value for ``chain``, or ``None``.

    Args:
        chain: Chain name.
        max_age_s: When set, a value older than this is treated as absent.
        now: Monotonic timestamp (injectable for tests).

    Returns:
        The stored :class:`LastGood`, or ``None`` when unknown or too old.
    """
    entry = _store.get(chain)
    if entry is None:
        return None
    if max_age_s is not None and entry.age_s(now) > max_age_s:
        return None
    return entry


def forget(chain: str) -> None:
    """Drop the stored value for ``chain`` (explicit invalidation)."""
    _store.pop(chain, None)


def reset() -> None:
    """Drop every stored value (test seam; also what a restart does)."""
    _store.clear()


__all__ = ["MAX_CHAINS", "LastGood", "forget", "recall", "remember", "reset"]
