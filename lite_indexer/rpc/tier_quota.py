"""One quota code path for both tiers — free and paid differ by policy only.

Policy (not a bypass switch): free admits head + aggregated reads at low
concurrency and a strict interval; paid admits head + historical + aggregated
at high concurrency with no interval.

A free (keyless) endpoint may follow the tip and serve aggregate/derived
reads, but it may not be spent on fresh historical reads: those are exactly
the requests a shared rate budget cannot afford, so they must come from the
response cache or an aggregated source instead. A keyed (paid/freemium)
endpoint serves everything at higher concurrency.

The tier is derived from the endpoint itself
(:func:`lite_indexer.rpc.freemium.is_freemium_endpoint`) and is re-derived on
every call. There is deliberately **no environment flag, function argument or
policy field** that lets a caller opt out of the free tier: the only way to
get paid behaviour is to configure a keyed endpoint. That is the property the
tests pin.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from lite_indexer.metrics import incr
from lite_indexer.rpc.freemium import is_freemium_endpoint
from lite_indexer.rpc.rpc_unavailable import QuotaExceeded


class ReadClass(StrEnum):
    """What kind of read is being admitted."""

    HEAD = "head"
    HISTORICAL = "historical"
    AGGREGATED = "aggregated"


@dataclass(frozen=True, slots=True)
class TierPolicy:
    """What one tier may do.

    Attributes:
        fresh_reads: Read classes that may be dispatched to the network.
        max_concurrency: In-flight requests allowed per endpoint.
        min_interval_s: Minimum gap between dispatches (0 = unthrottled).
    """

    fresh_reads: frozenset[ReadClass]
    max_concurrency: int
    min_interval_s: float


FREE_POLICY = TierPolicy(
    fresh_reads=frozenset({ReadClass.HEAD, ReadClass.AGGREGATED}),
    max_concurrency=2,
    min_interval_s=0.25,
)

PAID_POLICY = TierPolicy(
    fresh_reads=frozenset({ReadClass.HEAD, ReadClass.HISTORICAL, ReadClass.AGGREGATED}),
    max_concurrency=8,
    min_interval_s=0.0,
)


def tier_for(endpoint: str) -> str:
    """Tier name for an endpoint: ``"paid"`` when keyed, else ``"free"``."""
    return "paid" if is_freemium_endpoint(endpoint) else "free"


def policy_for(tier: str) -> TierPolicy:
    """Policy object for a tier name."""
    return PAID_POLICY if tier == "paid" else FREE_POLICY


@dataclass(slots=True)
class _State:
    """Per-endpoint admission bookkeeping."""

    in_flight: int = 0
    last_dispatch: float = 0.0
    admitted: int = 0
    rejected: int = 0


class TierQuota:
    """Admission control shared by free and paid reads.

    Every read (head, historical, aggregated) passes through :meth:`admit`
    before a provider is dialled, and calls :meth:`release` when the request
    finishes. State is per endpoint and bounded by the pool size.
    """

    def __init__(self, now: Callable[[], float] = time.monotonic) -> None:
        self._now = now
        self._states: dict[str, _State] = {}

    def _state(self, endpoint: str) -> _State:
        state = self._states.get(endpoint)
        if state is None:
            state = self._states[endpoint] = _State()
        return state

    def admit(self, endpoint: str, read_class: ReadClass) -> str:
        """Admit one read, or raise :class:`QuotaExceeded`.

        Args:
            endpoint: Endpoint that would serve the read.
            read_class: The class of read being attempted.

        Returns:
            The tier the endpoint was admitted under (``"free"``/``"paid"``).

        Raises:
            QuotaExceeded: The tier forbids this read class, the endpoint is
                at its concurrency limit, or the minimum interval has not
                elapsed (``retry_after_s`` says how long to wait).
        """
        tier = tier_for(endpoint)
        policy = policy_for(tier)
        if read_class not in policy.fresh_reads:
            self._state(endpoint).rejected += 1
            incr("rpc_quota_rejections")
            raise QuotaExceeded(
                tier,
                read_class.value,
                detail="serve from cache or an aggregated source instead",
            )
        state = self._state(endpoint)
        if state.in_flight >= policy.max_concurrency:
            state.rejected += 1
            incr("rpc_quota_rejections")
            raise QuotaExceeded(tier, read_class.value, detail="endpoint concurrency exhausted")
        wait = policy.min_interval_s - (self._now() - state.last_dispatch)
        if policy.min_interval_s > 0 and wait > 0:
            state.rejected += 1
            incr("rpc_quota_rejections")
            raise QuotaExceeded(tier, read_class.value, retry_after_s=wait, detail="rate limit")
        state.in_flight += 1
        state.last_dispatch = self._now()
        state.admitted += 1
        incr("rpc_quota_admitted")
        return tier

    def release(self, endpoint: str) -> None:
        """Release a slot taken by :meth:`admit` (safe to call twice)."""
        state = self._states.get(endpoint)
        if state is not None and state.in_flight > 0:
            state.in_flight -= 1

    def snapshot(self) -> dict[str, dict[str, object]]:
        """Per-endpoint admission counters (metrics/health surface)."""
        return {
            ep: {
                "tier": tier_for(ep),
                "in_flight": s.in_flight,
                "admitted": s.admitted,
                "rejected": s.rejected,
            }
            for ep, s in self._states.items()
        }

    def reset(self) -> None:
        """Drop all admission state (test seam)."""
        self._states.clear()


QUOTA = TierQuota()


def admit(endpoint: str, read_class: ReadClass) -> str:
    """Admit one read through the process-wide quota (see :meth:`TierQuota.admit`)."""
    return QUOTA.admit(endpoint, read_class)


def release(endpoint: str) -> None:
    """Release one slot on the process-wide quota."""
    QUOTA.release(endpoint)


__all__ = [
    "FREE_POLICY",
    "PAID_POLICY",
    "QUOTA",
    "ReadClass",
    "TierPolicy",
    "TierQuota",
    "admit",
    "policy_for",
    "release",
    "tier_for",
]
