"""Fair slot rotation for the daemon's active-chain cap.

A plain ``asyncio.Semaphore`` is FIFO but NOT fair across chains that re-queue
at very different rates: the 0.4 s Solana loop and the 2 s Base loop are back
in the queue constantly, so low-priority chains (bsc, fantom, celo) can wait
behind them indefinitely. Measured 2026-10-23 with a 3-slot semaphore: after
7 minutes only solana/base/polygon had ingested, while 13 chains sat
900-1300 s stale — the soft-ban storm was traded for starvation.

:class:`SlotRotation` fixes that by granting the next free slot to the chain
that has been served the FEWEST times (least-served-first, ties broken by
priority, then arrival order). Every chain therefore gets a turn before any
chain gets a second one.

Served-count fairness alone bounds the number of GRANTS a waiter sits out, not
the WALL TIME: a sync cycle was measured at 780-1270 s (2026-10-23), so with
16 chains a strict round-robin still parks a chain for hours. ``max_wait_s``
therefore adds a hard age bound — once a waiter has been queued that long it
outranks served-count at the next release (oldest first). Under saturation
that degrades to arrival order, which is exactly the anti-starvation contract.

Single-threaded asyncio only: no locks, no threads, no timers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

#: Default wall-clock bound on a queued acquire. Sized above the fastest
#: healthy cycle (Base ~2 s) but far below a slow backfill cycle, so an aged
#: waiter wins the next free slot instead of waiting out whole rounds.
DEFAULT_MAX_WAIT_S = 120.0


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

    # ── public API ────────────────────────────────────────────────────
    async def acquire(self, chain: str) -> None:
        """Wait for a slot for ``chain``."""
        if self._active < self._slots and not self._waiters:
            self._grant(chain)
            return
        self._arrivals += 1
        waiter = _Waiter(
            chain=chain,
            priority=self._priority_for(chain),
            arrival=self._arrivals,
            enqueued=self._clock(),
        )
        self._waiters.append(waiter)
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

    def release(self, chain: str) -> None:
        """Return ``chain``'s slot, granting it to the next fair waiter."""
        self._active -= 1
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
        self._served[chain] = self._served.get(chain, 0) + 1


def build_rotation(
    slots: int,
    priority_for: callable[[str], int] | None = None,
    max_wait_s: float = DEFAULT_MAX_WAIT_S,
) -> SlotRotation | None:
    """Return a fair rotation for ``slots``, or None when the cap is disabled."""
    return SlotRotation(slots, priority_for=priority_for, max_wait_s=max_wait_s) if slots and slots > 0 else None


__all__ = ["DEFAULT_MAX_WAIT_S", "SlotRotation", "build_rotation"]
