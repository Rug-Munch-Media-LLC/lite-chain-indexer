"""Wait computation for the RPC throttle.

Three priority layers, in order: an explicit Retry-After / backoff window,
pre-emptive pacing from the advertised budget, and the per-method request/s
budget.
"""

from __future__ import annotations

import time

from lite_indexer.config import (
    RPC_METHOD_DEFAULT_RATE,
    RPC_METHOD_REQ_PER_SEC,
    RPC_PREEMPTIVE_THRESHOLD_PCT,
    RPC_THROTTLE_MAX_WAIT,
)
from lite_indexer.rpc.throttle_base import ThrottleBase


class ThrottleWaitMixin(ThrottleBase):
    """How long to wait before the next request to an endpoint."""

    def wait_seconds_for(self, endpoint: str, method: str, now: float | None = None) -> float:
        """How long to sleep before the next request to ``endpoint``.

        Priority order:
        1. Explicit Retry-After / backoff window → wait exactly until it
           elapses.
        2. Advertised budget above the pre-emptive threshold → pace the
           request so the window is not exhausted.
        3. Per-method request/s budget → pace to the method rate.

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method name.
            now: Epoch time override (tests).

        Returns:
            Seconds to sleep (0 = call now).
        """
        state = self._entry(endpoint)
        now = time.time() if now is None else now

        # 1. Explicit backoff window (Retry-After from a real 429).
        if state.in_backoff(now):
            return state.remaining_wait(now)

        # 2. Pre-emptive: advertised budget nearly consumed → spread the
        # remaining calls across the rest of the window.
        if (
            state.limit
            and state.remaining is not None
            and state.reset_at is not None
            and state.consumed_pct() >= RPC_PREEMPTIVE_THRESHOLD_PCT
        ):
            seconds_left = max(0.0, state.reset_at - now)
            if seconds_left > 0 and state.remaining > 0:
                pace = seconds_left / state.remaining
                if pace > 0:
                    return min(pace, RPC_THROTTLE_MAX_WAIT)

        # 3. Per-method budget (rolling window enforcement). The window is
        # 1/rate seconds (eth_getLogs @ 3 req/s → 0.33s spacing) with a
        # tiny floor to avoid zero-width windows at very high rates.
        rate = RPC_METHOD_REQ_PER_SEC.get(method, RPC_METHOD_DEFAULT_RATE)
        if rate <= 0:
            return 0.0
        window = max(0.01, 1.0 / rate)
        times = state.request_times.setdefault(method, [])
        recent = [t for t in times if now - t < window]
        state.request_times[method] = recent
        if not recent:
            return 0.0
        # Allow at most one request per window slice.
        next_slot = recent[-1] + window
        return min(max(0.0, next_slot - now), RPC_THROTTLE_MAX_WAIT)

    def allow(self, endpoint: str, now: float | None = None) -> bool:
        """Whether a request to ``endpoint`` may proceed immediately.

        Args:
            endpoint: RPC endpoint URL.
            now: Epoch time override (tests).

        Returns:
            False when the endpoint is inside an explicit backoff window.
        """
        return not self._entry(endpoint).in_backoff(now)

    def mark_sent(self, endpoint: str, method: str, now: float | None = None) -> None:
        """Record that a request was actually sent (budget accounting)."""
        state = self._entry(endpoint)
        now = time.time() if now is None else now
        state.request_times.setdefault(method, []).append(now)
        state.updated_at = now


__all__ = ["ThrottleWaitMixin"]
