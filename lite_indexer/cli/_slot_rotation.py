"""Fair slot rotation for the daemon's active-chain cap.

A plain ``asyncio.Semaphore`` is FIFO but NOT fair across chains that re-queue
at very different rates: the 0.4 s Solana loop and the 2 s Base loop are back
in the queue constantly, so low-priority chains (bsc, fantom, celo) can wait
behind them indefinitely. :class:`SlotRotation` grants the next free slot to
the chain served the FEWEST times (least-served-first, ties broken by
priority, then arrival order), so every chain gets a turn before any chain
gets a second one.

Served-count fairness bounds the number of GRANTS a waiter sits out, not the
WALL TIME (a turn is hard-bounded by ``SYNC_SLICE_TIMEOUT_S``); ``max_wait_s``
adds a hard age bound at the next release.

Single-threaded asyncio only: no locks, no threads, no timers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.cli._slot_fairness import default_store
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Wall-clock bound on a queued acquire: an aged waiter wins the next slot.
DEFAULT_MAX_WAIT_S = 120.0

#: A queued acquire longer than this is a starvation signal (WARNING).
SLOT_WAIT_WARN_S = 120.0


@dataclass
class _Waiter:
    """One pending acquire: identity, ordering keys, and wakeup signal."""

    chain: str
    priority: int
    arrival: int
    enqueued: float
    event: asyncio.Event = field(default_factory=asyncio.Event)


class SlotRotation:
    """At most ``slots`` chains hold a slot, granted least-served-first."""

    def __init__(
        self,
        slots: int,
        priority_for: callable[[str], int] | None = None,
        max_wait_s: float = DEFAULT_MAX_WAIT_S,
        clock: callable[[], float] | None = None,
        fairness: Any = None,
    ) -> None:
        """Create the rotation.

        Args:
            slots: Maximum number of concurrently held slots.
            priority_for: Optional callable mapping a chain to its priority.
                Higher priority wins only AMONG chains served equally often.
            max_wait_s: Wall-clock age at which a waiter outranks served-count
                fairness. 0 disables the age bound (pure least-served-first).
            clock: Monotonic clock seam (tests inject a fake); defaults to
                :func:`time.monotonic`.
        """
        self._slots = max(1, int(slots))
        self._priority_for = priority_for or (lambda chain: 0)
        self._max_wait_s = max(0.0, float(max_wait_s))
        self._clock = clock or time.monotonic
        self._active = 0
        self._waiters: list[_Waiter] = []
        self._served: dict[str, int] = {}
        self._arrivals = 0
        self._held: set[str] = set()
        # CI recreates the container every few minutes: persist served counts.
        self._fairness = fairness
        if fairness is not None:
            self._served, self._arrivals = fairness.load()

    # ── public API ────────────────────────────────────────────────────
    async def acquire(self, chain: str) -> None:
        """Wait for a slot for ``chain``.

        A chain parked here is INVISIBLE in the sync logs — it emits no
        ``sync_start`` and no error, which is why 12 of 16 chains looked
        "dead" for 20 min on 2026-10-23/24. Both edges are therefore logged
        with the wall-clock wait, and a wait over ``SLOT_WAIT_WARN_S`` warns
        so a starved chain names itself.
        """
        if self._active < self._slots and not self._waiters:
            self._grant(chain)
            logger.info("slot_granted", chain=chain, wait_s=0.0, active=self._active)
            return
        self._arrivals += 1
        waiter = _Waiter(
            chain=chain,
            priority=self._priority_for(chain),
            arrival=self._arrivals,
            enqueued=self._clock(),
        )
        self._waiters.append(waiter)
        logger.info("slot_queued", chain=chain, depth=len(self._waiters), active=self._active)
        try:
            await waiter.event.wait()
        except asyncio.CancelledError:
            # Shutdown/cancel while queued: drop the waiter, or hand an
            # ALREADY-granted slot back, so cancellation never leaks capacity.
            if waiter in self._waiters:
                self._waiters.remove(waiter)
            else:
                self.release(chain)
            raise
        waited = self._clock() - waiter.enqueued
        fields = {"chain": chain, "wait_s": round(waited, 1), "active": self._active}
        if waited >= SLOT_WAIT_WARN_S:
            logger.warning("slot_wait_exceeded", **fields)
        else:
            logger.info("slot_granted", **fields)

    def release(self, chain: str) -> None:
        """Return ``chain``'s slot, granting it to the next fair waiter."""
        self._active -= 1
        self._held.discard(chain)
        logger.info("slot_released", chain=chain, depth=len(self._waiters), active=self._active)
        if self._waiters:
            self._promote(self._pick())
        # else: the slot simply stays free.

    @property
    def active(self) -> int:
        """Number of currently held slots."""
        return self._active

    @property
    def waiting(self) -> int:
        """Number of chains queued for a slot."""
        return len(self._waiters)

    @property
    def oldest_wait_s(self) -> float:
        """Age of the longest-queued waiter (0.0 when the queue is empty)."""
        if not self._waiters:
            return 0.0
        return self._clock() - min(w.enqueued for w in self._waiters)

    def snapshot(self) -> dict[str, tuple[float, float]]:
        """chain -> (state, wait_s); 2=holding, 1=queued (starved)."""
        now = self._clock()
        snap = {c: (2.0, 0.0) for c in self._held}
        snap.update({w.chain: (1.0, now - w.enqueued) for w in self._waiters})
        return snap

    # ── internals ─────────────────────────────────────────────────────
    def _pick(self) -> _Waiter:
        """Choose the next waiter: aged first, else least-served."""
        if self._max_wait_s > 0:
            aged = [w for w in self._waiters if self._clock() - w.enqueued >= self._max_wait_s]
            if aged:
                # Hard anti-starvation bound: oldest waiter wins.
                return min(aged, key=lambda w: (w.enqueued, -w.priority, w.arrival))
        lo = min(self._served.get(w.chain, 0) for w in self._waiters)
        eligible = [w for w in self._waiters if self._served.get(w.chain, 0) == lo]
        return min(eligible, key=lambda w: (-w.priority, w.arrival))

    def _promote(self, waiter: _Waiter) -> None:
        """Hand a freed slot straight to ``waiter`` (no free/acquire race)."""
        self._waiters.remove(waiter)
        self._grant(waiter.chain)
        waiter.event.set()

    def _grant(self, chain: str) -> None:
        self._active += 1
        self._held.add(chain)
        self._served[chain] = self._served.get(chain, 0) + 1
        if self._fairness is not None:
            self._fairness.save(self._served, self._arrivals)


def build_rotation(
    slots: int,
    priority_for: callable[[str], int] | None = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
    fairness: Any = None,
) -> SlotRotation | None:
    """Return a fair rotation for ``slots``, or None when the cap is disabled."""
    global CURRENT
    if fairness is None and slots:
        fairness = default_store()  # persists across CI container recreates
    CURRENT = SlotRotation(slots, priority_for=priority_for, max_wait_s=max_wait_s, fairness=fairness)
    return CURRENT if slots else None


CURRENT: SlotRotation | None = None  # newest rotation, read by the metrics exporter


__all__ = ["DEFAULT_MAX_WAIT_S", "SLOT_WAIT_WARN_S", "SlotRotation", "build_rotation"]
