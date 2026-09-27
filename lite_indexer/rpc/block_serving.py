"""Which pool endpoints have DEMONSTRATED they serve a given block range.

:mod:`lite_indexer.rpc.tip_capable` answers a different question — "can this
endpoint hash the TIP?" — and the 2026-09-26 hyperliquid stall proved that is
the wrong question. What reorg staging actually needs is "can this endpoint
serve block N", which is a property of the BLOCK and the provider's retained
window, not a fixed trait of the endpoint. One endpoint can be capable for the
tip and incapable for a block 45 million behind it.

So the registry is keyed on a served WINDOW per (chain, endpoint): the lowest
and highest block we have watched an endpoint answer for, widened by the
attempts that came back empty. An endpoint is offered first for a block that
falls inside its demonstrated window. A 200-with-no-block is evidence the
block is outside the window, so the lower bound climbs; it is never evidence
of refusal, and it must not be recorded as one.

Everything is in-process evidence: a restart re-probes, which is the correct
and cheap behaviour. The registry is bounded (a handful of chains times a
handful of endpoints) and is read on the hot path only by
:func:`order_by_served_range`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from lite_indexer.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = get_logger(__name__)

#: How far below a served block to keep trusting the lower bound. A gateway
#: window is contiguous, so an endpoint that served block N almost certainly
#: serves everything above N; the slack only guards a provider that prunes
#: from both ends.
WINDOW_SLACK_BLOCKS = 1_000_000


@dataclass(slots=True)
class ServedWindow:
    """The contiguous block range an endpoint has been observed to serve.

    Attributes:
        lowest: Lowest block observed served (or the highest observed
            unserved, as the floor of what it will admit to having).
        highest: Highest block observed served.
    """

    lowest: int
    highest: int

    def covers(self, block: int) -> bool:
        """Whether this endpoint is known to serve ``block``."""
        return self.lowest - WINDOW_SLACK_BLOCKS <= block <= self.highest


_windows: dict[tuple[str, str], ServedWindow] = {}


def record_served(chain: str, endpoint: str, block: int) -> None:
    """Record that ``endpoint`` returned a real block for ``block``.

    Widens the existing window rather than replacing it: the endpoints that
    have proved useful over a long crawl must stay at the head of the pool
    order, not be reset to a single block on every call.
    """
    key = (chain, endpoint)
    window = _windows.get(key)
    if window is None:
        _windows[key] = ServedWindow(lowest=block, highest=block)
        return
    window.lowest = min(window.lowest, block)
    window.highest = max(window.highest, block)


def record_unserved(chain: str, endpoints: Sequence[str], block: int) -> None:
    """Record that every endpoint in ``endpoints`` had no block for ``block``.

    A 200 with a null result means "outside my retained window", so the
    floor rises to just below the requested block. That is the evidence that
    later demotes these endpoints for still-older blocks, and it is recorded
    as a window fact, never as a refusal.
    """
    for endpoint in endpoints:
        key = (chain, endpoint)
        window = _windows.get(key)
        if window is None:
            _windows[key] = ServedWindow(lowest=block, highest=block)
            continue
        window.highest = min(window.highest, block - 1)
        window.lowest = min(window.lowest, window.highest)


def order_by_served_range(chain: str, endpoints: Sequence[str], block: int) -> list[str]:
    """Order the pool so endpoints known to serve ``block`` are tried first.

    The order is a pure function of the recorded evidence and the configured
    pool — deliberately unrandomised. The pre-fix selector ended in a random
    power-of-two-choices draw, which is precisely why reordering the pool
    could not fix a stall that only two of six endpoints could serve: order
    never decided the winner, the coin did.

    Args:
        chain: Chain name.
        endpoints: The configured pool, in order.
        block: The block about to be read.

    Returns:
        A new list. Endpoints with a window covering ``block`` lead in pool
        order; endpoints with a window that excludes it are pushed behind
        the unknowns; unrecorded endpoints keep their configured position
        among themselves, so a chain never probed yet behaves exactly as
        before.
    """
    return sorted(endpoints, key=lambda ep: _rank(chain, ep, block))


def _rank(chain: str, endpoint: str, block: int) -> int:
    """Sort rank for one endpoint: 0 serves it, 1 unknown, 2 known not to."""
    window = _windows.get((chain, endpoint))
    if window is None:
        return 1
    return 0 if window.covers(block) else 2


def served_window(chain: str, endpoint: str) -> ServedWindow | None:
    """Return the recorded window for an endpoint, or ``None`` when unknown."""
    return _windows.get((chain, endpoint))


def reset_windows() -> None:
    """Drop all recorded evidence (test seam; also what a restart does)."""
    _windows.clear()


__all__ = [
    "WINDOW_SLACK_BLOCKS",
    "ServedWindow",
    "order_by_served_range",
    "record_served",
    "record_unserved",
    "reset_windows",
    "served_window",
]
