"""Adaptive blockwise sampling stride for Solana catch-up.

Deep windows fetch every Nth slot (sampled coverage is sufficient for
tracked-mint capture). When per-block RPC latency degrades the stride
escalates so a window still fits inside the chain's production rate; it
returns to the base as soon as latency recovers. Never below the base
(data coverage), never above 2x base (RPC relief).
"""
from __future__ import annotations

import os

#: Per-block mean latency above which the stride escalates (seconds).
SLOW_FETCH_S = 4.0
#: Per-block mean latency below which the stride returns to base (seconds).
FAST_FETCH_S = 1.5


def base_stride() -> int:
    """Operator base stride from ``SOLANA_SAMPLE_STRIDE`` (default 4)."""
    try:
        value = int(os.environ.get("SOLANA_SAMPLE_STRIDE", "4") or 4)
    except ValueError:
        return 4
    return max(1, value)


def next_sample_stride(prev_stride: int | None, mean_fetch_s: float, base: int) -> int:
    """Stride for the next window: escalate on slow fetch, recover on fast.

    Args:
        prev_stride: Stride used by the previous blockwise window (None = base).
        mean_fetch_s: Mean per-block fetch latency of that window.
        base: Operator base stride (floor).

    Returns:
        The next stride, bounded to ``[base, 2 * base]``.
    """
    base = max(1, int(base))
    current = max(base, int(prev_stride or base))
    if mean_fetch_s > SLOW_FETCH_S:
        return min(current * 2, base * 2)
    if mean_fetch_s < FAST_FETCH_S:
        return base
    return current
