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

    Returns the **largest accepted rung**, not the smallest. An accepted rung
    is a proven fact — the endpoint answered that exact range — so the largest
    one is both safe and the only value that carries the ladder's information.
    Returning the smallest made the ladder inert: the first rung is always the
    smallest, so any endpoint that could serve the first rung was recorded at
    that rung forever and higher rungs could never be reported. Measured
    2026-10-24: ``bsc.api.pocket.network`` served 50 blocks / 11,216 logs in
    1.96 s and ``rpc-bsc.48.club`` served 200 blocks / 47,220 logs in 1.08 s,
    yet the pool still reported ``max_span{bsc}=10`` — a probe artefact, not a
    chain limit.

    The bound stays conservative in the sense that matters: it is never a
    value the endpoint has not served. A rung the probe did not reach (outside
    ``max_calls``) still cannot raise the bound.

    Args:
        candidates: Ascending candidate range sizes.
        max_calls: Probe call budget (bounds the ladder actually walked).
        accepted: Rungs, in probe order, the endpoint answered OK for.

    Returns:
        The largest accepted size within the budget, or ``0`` when no rung
        was accepted (the endpoint cannot serve logs at any probed size).
    """
    walked = set(ladder_rungs(candidates, max_calls))
    in_budget = [size for size in accepted if size in walked]
    return max(in_budget) if in_budget else 0


def probe_timeout_for(
    size: int,
    base_timeout: float,
    base_span: int,
    cap: float,
) -> float:
    """Per-rung probe timeout, scaled by the span being asked for.

    A fixed timeout cannot measure a range capability: an ``eth_getLogs``
    answer costs roughly proportional to the logs it returns, so the same
    wall-clock budget that is generous for 10 blocks is a guaranteed timeout
    for 100. Measured 2026-10-24 on bsc: the 100-block rung took 23-35 s on
    endpoints that serve it happily (``rpc-bsc.48.club`` 23.5 s,
    ``bsc.api.pocket.network`` 27.0 s) against a probe timeout of 2.5 s, so
    every rung above the first was recorded as a rejection and the endpoint
    was pinned at 10 blocks — a probe artefact compounding the
    ``conservative_bound`` one.

    Scaling keeps the rung-10 cost exactly where it was (no startup
    regression for endpoints that only serve small ranges) and lets a larger
    rung prove itself within a budget proportional to its own size.

    Args:
        size: Rung size in blocks being probed.
        base_timeout: Timeout that is correct for ``base_span`` blocks.
        base_span: The rung whose cost ``base_timeout`` represents.
        cap: Hard ceiling for any single rung.

    Returns:
        A timeout in seconds, never below ``base_timeout``, never above ``cap``.
    """
    if base_span <= 0:
        return min(cap, base_timeout)
    multiples = max(1, int(size) // int(base_span))
    return min(cap, base_timeout * multiples)


__all__ = ["conservative_bound", "ladder_rungs", "probe_timeout_for"]
