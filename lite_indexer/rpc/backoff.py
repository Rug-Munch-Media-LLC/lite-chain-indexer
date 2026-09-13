"""
Jittered exponential backoff (AWS-style full jitter).

Deterministic exponential backoff synchronizes every worker: they all sleep
the same amount and retry the same instant, re-hammering a recovering
provider (thundering herd). Full jitter spreads the retries:

    delay = random.uniform(0, min(cap, base * 2 ** attempt))

``Retry-After`` is the exception — when the server names an exact window the
caller waits it verbatim (see ``rpc/throttle.py`` / ``rpc/client.py``); this
module is only for speculative exponential backoff.

Tunables (see ``config/rpc.py``):
    RPC_BACKOFF_JITTER       enable/disable jitter (default on)
    RPC_BACKOFF_JITTER_SEED  deterministic seed for tests/replay
"""

from __future__ import annotations

import random

from lite_indexer.config import (
    RPC_BACKOFF_JITTER,
    RPC_BACKOFF_JITTER_SEED,
)

_Seed = int | float | str | bytes | bytearray | None


def _make_rng(seed: _Seed = None) -> random.Random:
    """Build an RNG; empty/None seed falls back to system entropy."""
    if seed is None or seed == "":
        return random.Random()
    return random.Random(seed)


def _initial_seed() -> _Seed:
    return RPC_BACKOFF_JITTER_SEED or None


_RNG = _make_rng(_initial_seed())


def set_jitter_seed(seed: _Seed = None) -> None:
    """Reseed the module RNG (tests / replay). ``None`` restores entropy."""
    global _RNG
    _RNG = _make_rng(seed)


def jitter_enabled() -> bool:
    """Whether jitter is enabled (reads the config flag at call time)."""
    return RPC_BACKOFF_JITTER


def full_jitter(upper: float, *, rng: random.Random | None = None) -> float:
    """Return a delay in ``[0, upper]`` (``upper`` exactly when disabled).

    Args:
        upper: Maximum delay in seconds (<= 0 returns 0.0).
        rng: Optional RNG override (tests). Defaults to the module RNG.

    Returns:
        A full-jitter delay, or ``upper`` when jitter is disabled.
    """
    if upper <= 0:
        return 0.0
    if not RPC_BACKOFF_JITTER:
        return upper
    return (rng or _RNG).uniform(0.0, upper)


def exponential_backoff(
    attempt: int,
    *,
    base: float,
    cap: float,
    rng: random.Random | None = None,
) -> float:
    """Full-jitter exponential backoff for a zero-indexed ``attempt``.

    The deterministic ceiling is ``min(cap, base * 2 ** attempt)``; the
    returned delay is uniform over ``[0, ceiling]``.

    Args:
        attempt: Zero-indexed retry attempt.
        base: Base delay in seconds.
        cap: Maximum delay ceiling in seconds.
        rng: Optional RNG override (tests).

    Returns:
        A jittered delay in ``[0, min(cap, base * 2**attempt)]``.
    """
    ceiling = min(cap, base * (2 ** max(0, attempt)))
    return full_jitter(ceiling, rng=rng)


__all__ = [
    "exponential_backoff",
    "full_jitter",
    "jitter_enabled",
    "set_jitter_seed",
]
