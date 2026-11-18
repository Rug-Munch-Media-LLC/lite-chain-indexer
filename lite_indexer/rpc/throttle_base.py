"""Per-endpoint throttle state and the tracker's state access.

``EndpointState`` is what gets persisted; ``ThrottleBase`` owns the state
dict and the read-only views over it. Persistence, waiting and recording are
separate mixins composed in :mod:`lite_indexer.rpc.throttle`.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lite_indexer.config import RPC_THROTTLE_FILE


@dataclass
class EndpointState:
    """Persisted per-endpoint throttle state."""

    endpoint: str
    # Advertised budget from the last X-RateLimit-* headers.
    limit: int | None = None
    remaining: int | None = None
    reset_at: float | None = None
    # Absolute epoch time until which we must not call this endpoint.
    backoff_until: float = 0.0
    # Rolling per-method request timestamps for budget enforcement.
    request_times: dict[str, list[float]] = field(default_factory=dict)
    # Rolling 429 count (feeds circuit-breaker-style escalation).
    rate_limit_hits: int = 0
    # Whether the last response was a 429 (persisted for restart safety).
    last_status: int = 200
    updated_at: float = 0.0

    def in_backoff(self, now: float | None = None) -> bool:
        """Whether the endpoint is inside an explicit backoff window."""
        now = time.time() if now is None else now
        return self.backoff_until > now

    def remaining_wait(self, now: float | None = None) -> float:
        """Seconds until the backoff window elapses (0 when open)."""
        now = time.time() if now is None else now
        return max(0.0, self.backoff_until - now)

    def consumed_pct(self) -> float:
        """Percent of the advertised budget consumed (None-safe → 0.0)."""
        if self.limit and self.remaining is not None:
            return max(0.0, 100.0 * (1.0 - self.remaining / self.limit))
        return 0.0


class ThrottleBase:
    """State container shared by the throttle mixins."""

    def __init__(self, path: str | Path | None = None, persist_interval: float = 60.0) -> None:
        self._path = Path(path or RPC_THROTTLE_FILE)
        self._persist_interval = persist_interval
        self._last_persist: float = 0.0
        self._state: dict[str, EndpointState] = {}
        self._load()

    def _load(self) -> None:
        """Hydrate from disk (no-op; overridden by the persistence mixin)."""

    def persist_if_stale(self, now: float | None = None) -> None:
        """Flush hook (no-op; overridden by the persistence mixin)."""

    def _entry(self, endpoint: str) -> EndpointState:
        state = self._state.get(endpoint)
        if state is None:
            state = EndpointState(endpoint=endpoint)
            self._state[endpoint] = state
        return state

    def state_for(self, endpoint: str) -> dict[str, Any]:
        """Public read-only view of an endpoint's throttle state."""
        state = self._state.get(endpoint)
        if state is None:
            return {}
        return {
            "limit": state.limit,
            "remaining": state.remaining,
            "consumed_pct": round(state.consumed_pct(), 1),
            "backoff_until": state.backoff_until,
            "in_backoff": state.in_backoff(),
            "rate_limit_hits": state.rate_limit_hits,
            "last_status": state.last_status,
        }

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Full state view (for logging / metrics)."""
        return {ep: self.state_for(ep) for ep in self._state}

    def reset(self, endpoint: str | None = None) -> None:
        """Clear the tracker (one endpoint or all)."""
        if endpoint is None:
            self._state.clear()
        else:
            self._state.pop(endpoint, None)


__all__ = ["EndpointState", "ThrottleBase"]
