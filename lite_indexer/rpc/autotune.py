"""Auto-tune soft rate limiting — NodeCore-style strain adaptation.

Ported from drpcorg/nodecore's rate-limit auto-tune concept: instead of
hard-banning an endpoint when it degrades, progressively widen the
inter-call spacing while its recent error rate stays above threshold, and
halve the spacing back once a full window comes in healthy. Keeps a
straining free-tier provider usable (at reduced pace) rather than losing
it entirely — critical now that keyed freemium depth is quota-protected.

State is in-process (the hard backoff/ban layer lives in throttle.py /
pool.py; this is the fine-grained dial between them).

Config:
    RPC_AUTOTUNE_ENABLED     "0" disables (default on)
    RPC_AUTOTUNE_WINDOW_S    rolling window for error rate (default 120)
    RPC_AUTOTUNE_THRESHOLD   error fraction that triggers widening (0.30)
    RPC_AUTOTUNE_STEP_S      spacing increment per triggering window (0.5)
    RPC_AUTOTUNE_MAX_SPACING hard ceiling on enforced gap (8.0)
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import defaultdict
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

AUTOTUNE_ENABLED = os.getenv("RPC_AUTOTUNE_ENABLED", "1") != "0"
AUTOTUNE_WINDOW_S = float(os.getenv("RPC_AUTOTUNE_WINDOW_S", "120"))
AUTOTUNE_THRESHOLD = float(os.getenv("RPC_AUTOTUNE_THRESHOLD", "0.30"))
AUTOTUNE_STEP_S = float(os.getenv("RPC_AUTOTUNE_STEP_S", "0.5"))
AUTOTUNE_MAX_SPACING = float(os.getenv("RPC_AUTOTUNE_MAX_SPACING", "8.0"))

_state: dict[str, dict[str, Any]] = defaultdict(
    lambda: {"calls": [], "errors": [], "spacing": 0.0, "last_window_end": 0.0}
)


def _prune(entry: dict[str, Any], now: float) -> None:
    cutoff = now - AUTOTUNE_WINDOW_S
    entry["calls"] = [t for t in entry["calls"] if t > cutoff]
    entry["errors"] = [t for t in entry["errors"] if t > cutoff]


def record_call(endpoint: str, *, ok: bool, now: float | None = None) -> None:
    """Record one call outcome; adjust enforced spacing when warranted.

    The error rate is evaluated on every call (cheap: bounded lists), but
    adjustments are rate-limited to one per half-window so a rapid burst
    can't spike the spacing to the cap instantly.
    """
    if not AUTOTUNE_ENABLED:
        return
    now = time.time() if now is None else now
    entry = _state[endpoint]
    _prune(entry, now)
    entry["calls"].append(now)
    if not ok:
        entry["errors"].append(now)

    calls = entry["calls"]
    if len(calls) < 10:
        return  # not enough signal yet
    if now - entry["last_window_end"] < AUTOTUNE_WINDOW_S / 2:
        return  # adjustment rate-limit
    err_rate = len(entry["errors"]) / len(calls)
    old = entry["spacing"]
    if err_rate >= AUTOTUNE_THRESHOLD:
        entry["spacing"] = min(AUTOTUNE_MAX_SPACING, max(AUTOTUNE_STEP_S, old * 2 or AUTOTUNE_STEP_S))
    elif err_rate == 0.0 and old > 0.0:
        entry["spacing"] = max(0.0, old / 2)
    else:
        return  # no change → don't consume the adjustment slot
    entry["last_window_end"] = now
    logger.info(
        "rpc_autotune_adjusted",
        endpoint=endpoint[:50],
        err_rate=round(err_rate, 3),
        spacing_s=round(entry["spacing"], 2),
    )


def current_spacing(endpoint: str, now: float | None = None) -> float:
    """Currently enforced minimum gap between calls to ``endpoint``."""
    if not AUTOTUNE_ENABLED:
        return 0.0
    now = time.time() if now is None else now
    entry = _state[endpoint]
    _prune(entry, now)
    return float(entry["spacing"])


async def autotune_wait(endpoint: str, last_call_at: float | None = None) -> None:
    """Sleep out any remaining enforced gap before the next dispatch.

    Args:
        endpoint: RPC endpoint URL.
        last_call_at: epoch of the previous dispatch to this endpoint
            (callers that track it); omit to only honor a freshly-tripped
            spacing from the last recorded error burst.
    """
    spacing = current_spacing(endpoint)
    if spacing <= 0.0:
        return
    if last_call_at is not None:
        elapsed = time.time() - last_call_at
        remaining = spacing - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)
    # Without last_call_at we cannot know the true gap; apply a bounded
    # courtesy pause proportional to spacing so bursts still get damped.
    else:
        await asyncio.sleep(min(spacing, 1.0))


def reset(endpoint: str | None = None) -> None:
    """Clear autotune state (tests, operator action)."""
    if endpoint is None:
        _state.clear()
    else:
        _state.pop(endpoint, None)
