"""Pure getLogs range-ladder policy (no I/O, no imports from the registry).

The bounded capability probe must satisfy three contracts: it ascends a
fixed candidate ladder, it stops at the first rung the endpoint rejects, and
it never spends more than its call budget. All three are decisions about a
sequence of outcomes — not about HTTP — so they live here as one function
over plain values. The caller (``rpc.capabilities.probe_getlogs_range``) owns
the socket and passes each rung's outcome in as it is observed.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence


def ladder_rungs(candidates: Sequence[int], max_calls: int) -> Iterator[int]:
    """Yield candidate range sizes to probe, in ascending order.

    Stops once ``max_calls`` rungs have been yielded, which is what bounds
    the probe's wall time to ``max_calls * per-request timeout``.

    Args:
        candidates: Ascending candidate range sizes.
        max_calls: Hard ceiling on the number of probes.

    Yields:
        Each range size to try, largest-first only if ``candidates`` is.
    """
    for size in candidates:
        if max_calls <= 0:
            return
        yield size
        max_calls -= 1


def conservative_bound(candidates: Sequence[int], max_calls: int, accepted: Sequence[int]) -> int:
    """Conservative eth_getLogs range for an endpoint from the rungs it accepted.

    Returns the **smallest accepted rung**, never the largest: routing only
    needs a range size that is known to work, and an endpoint's ceiling can
    move under load. A caller that stops probing at the first rejection
    therefore gets exactly the same answer as one that keeps probing to its
    budget — the extra rungs can only lower confidence, never raise it.

    Args:
        candidates: Ascending candidate range sizes.
        max_calls: Probe call budget (bounds the ladder actually walked).
        accepted: Rungs, in probe order, the endpoint answered OK for.

    Returns:
        The smallest accepted size within the budget, or ``0`` when no rung
        was accepted (the endpoint cannot serve logs at any probed size).
    """
    walked = set(ladder_rungs(candidates, max_calls))
    in_budget = [size for size in accepted if size in walked]
    return min(in_budget) if in_budget else 0


__all__ = ["conservative_bound", "ladder_rungs"]
