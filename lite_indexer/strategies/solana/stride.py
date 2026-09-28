"""Chunk sizing for Solana catch-up — every slot in the window is fetched.

The window itself (``SOLANA_BLOCK_MAX_PER_SYNC``) is the only throughput
knob. It was previously an *adaptive sample stride* that skipped every
non-conforming slot (``slot % stride == 0``): for a fund-flow indexer that
is permanent, silent data loss, and it degraded exactly when RPC latency
rose — the condition under which completeness matters most.

Bounded concurrency (chunks of ``chunk_size`` slots) absorbs a slow RPC
just as a stride did, without discarding a single slot. The RPC-budget
lever is therefore: shrink the window, never subsample it.
"""

from __future__ import annotations

import os

#: Hard floor on window depth — one slot per window, i.e. no sampling.
MIN_WINDOW_SLOTS = 1
#: Upper bound on window depth (slots fetched concurrently in one window).
MAX_WINDOW_SLOTS = 2000


def _env_int(name: str, default: int) -> int:
    """Read an int env knob, falling back to the default on missing/invalid."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip().lstrip("-").isdigit():
        return default
    return int(raw)


def base_stride() -> int:
    """Deprecated alias for the window depth. Retained for the RPC knob.

    The returned value is now the *window* size in slots, not a modulus.
    Callers must pass every slot in the window to the fetcher.
    """
    return window_slots()


def window_slots() -> int:
    """Slots to fetch per catch-up window (``SOLANA_WINDOW_SLOTS``).

    Falls back to the legacy ``SOLANA_BLOCK_MAX_PER_SYNC`` knob, then to
    200. Clamped to ``[MIN_WINDOW_SLOTS, MAX_WINDOW_SLOTS]`` so a bad env
    value can never widen the window without bound.
    """
    value = _env_int("SOLANA_WINDOW_SLOTS", _env_int("SOLANA_BLOCK_MAX_PER_SYNC", 200))
    return max(MIN_WINDOW_SLOTS, min(int(value), MAX_WINDOW_SLOTS))


__all__ = ["MAX_WINDOW_SLOTS", "MIN_WINDOW_SLOTS", "base_stride", "window_slots"]
