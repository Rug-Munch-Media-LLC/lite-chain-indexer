"""Fair slot rotation for the daemon's active-chain cap.

A plain ``asyncio.Semaphore`` is FIFO but NOT fair across chains that re-queue
at very different rates: the 0.4 s Solana loop and the 2 s Base loop are back
in the queue constantly, so low-priority chains (bsc, fantom, celo) can wait
behind them indefinitely. Measured 2026-10-23 with a 3-slot semaphore: after
7 minutes only solana/base/polygon had ingested, while 13 chains sat
900-1300 s stale — the soft-ban storm was traded for starvation.

:class:`SlotRotation` fixes that by granting the next free slot to the chain
that has been served the FEWEST times (least-recently-served, ties broken by
priority, then arrival order). Every chain therefore gets a turn before any
chain gets a second one, which is exactly the "explicit rotation" contract:
the chain count stays bounded AND no chain is permanently parked.

Single-threaded asyncio only: no locks, no threads, no timers.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field


@dataclass
class _Waiter:
    """One pending acquire: identity, ordering key, and wakeup signal."""

    chain: str
    priority: int
    arrival: int
    event: asyncio.Event = field(default_factory=asyncio.Event)


class SlotRotation:
    """At most ``slots`` chains hold a slot, granted least-served-first."""

    def __init__(self, slots: int, priority_for: callable[[str], int] | None = None) -> None:
        """Create the rotation.

        Args:
            slots: Maximum number of concurrently held slots.
            priority_for: Optional callable mapping a chain to its priority.
                Higher priority wins only AMONG chains served equally often.
        """
        self._slots = max(1, int(slots))
        self._priority_for = priority_for or (lambda chain: 0)
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
        waiter = _Waiter(chain=chain, priority=self._priority_for(chain), arrival=self._arrivals)
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

    # ── internals ─────────────────────────────────────────────────────
    def _pick(self) -> _Waiter:
        """Choose the least-served waiter (ties: priority, then arrival)."""
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


def build_rotation(slots: int, priority_for: callable[[str], int] | None = None) -> SlotRotation | None:
    """Return a fair rotation for ``slots``, or None when the cap is disabled."""
    return SlotRotation(slots, priority_for=priority_for) if slots and slots > 0 else None


__all__ = ["SlotRotation", "build_rotation"]
