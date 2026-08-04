"""
Intelligent RPC throttling — header tracking, adaptive backoff, pre-emptive
throttling, per-method budgets, and disk-persisted state.

Three layers make this distinct from the per-provider token bucket
(``rpc/ratelimit.py``) and the circuit breaker (``rpc/routing.py``):

1. **Rate-limit header tracking** — ``X-RateLimit-Remaining`` /
   ``X-RateLimit-Limit`` / ``Retry-After`` are captured per endpoint and
   honored exactly: a 429 that advertises ``Retry-After: 60`` sleeps 60s,
   not a guess. Header-less endpoints fall back to the token bucket.

2. **Pre-emptive throttling** — when an endpoint advertises a budget and
   we have consumed more than :data:`RPC_PREEMPTIVE_THRESHOLD_PCT`
   (default 80%), the next request is delayed so the budget is never
   exhausted — we slow down *before* the 429 arrives.

3. **Per-method budgets** — ``eth_getLogs`` is expensive on free tiers
   (3-10 req/s) while ``eth_blockNumber`` is nearly free. Each method has
   its own request/s budget (:data:`RPC_METHOD_REQ_PER_SEC`) so cheap
   calls never starve expensive ones (and vice versa).

All state is persisted to :data:`RPC_THROTTLE_FILE` (a JSON file) so a
restart mid-429-window does not reset the tracker — the daemon wakes up
still knowing which endpoints told it to back off and for how long.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from lite_indexer.config import (
    RPC_METHOD_DEFAULT_RATE,
    RPC_METHOD_REQ_PER_SEC,
    RPC_PREEMPTIVE_THRESHOLD_PCT,
    RPC_THROTTLE_FILE,
    RPC_THROTTLE_MAX_WAIT,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Header name variants seen in the wild (Alchemy, Blast, QuickNode,
# generic Cloudflare/proxy layers all differ slightly).
_RATE_LIMIT_HEADERS = (
    "x-ratelimit-limit",
    "x-ratelimit-remaining",
    "x-ratelimit-reset",
    "x-ratelimit-reset-epoch",
    "ratelimit-limit",
    "ratelimit-remaining",
    "retry-after",
)

# Sentinel returned by wait computations when an endpoint is inside an
# explicit Retry-After window.
_RETRY_AFTER = "retry_after"


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """Case-insensitive header lookup."""
    return headers.get(name) or headers.get(name.lower()) or headers.get(name.upper())


def _parse_int(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(value.strip())
    except (TypeError, ValueError):
        return None


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.strip())
    except (TypeError, ValueError):
        return None


def extract_rate_limit(headers: Mapping[str, str]) -> dict[str, int | float | None]:
    """Parse rate-limit headers into a normalized dict.

    Handles both header families:
      - ``X-RateLimit-Limit`` + ``X-RateLimit-Remaining`` + ``X-RateLimit-Reset``
        (epoch seconds when the window resets)
      - ``X-RateLimit-Reset`` in delta-seconds form (some proxies)
      - ``Retry-After`` (HTTP-date or delta-seconds) for 429 responses

    Args:
        headers: HTTP response headers (any Mapping, case-insensitive).

    Returns:
        ``{limit, remaining, reset_at, retry_after}`` with ``None`` for
        headers that were absent or unparseable.
    """
    limit = _parse_int(_header_value(headers, "x-ratelimit-limit"))
    if limit is None:
        limit = _parse_int(_header_value(headers, "ratelimit-limit"))
    remaining = _parse_int(_header_value(headers, "x-ratelimit-remaining"))
    if remaining is None:
        remaining = _parse_int(_header_value(headers, "ratelimit-remaining"))

    reset_at: float | None = None
    reset = _header_value(headers, "x-ratelimit-reset")
    if reset is None:
        reset = _header_value(headers, "x-ratelimit-reset-epoch")
    if reset is not None:
        parsed = _parse_float(reset)
        if parsed is not None:
            # If the reset value is in the near future it is a delta
            # (proxy style); otherwise treat it as an epoch timestamp.
            reset_at = time.time() + parsed if parsed < 1_000_000_000 else parsed

    retry_after: float | None = None
    ra = _header_value(headers, "retry-after")
    if ra is not None:
        parsed = _parse_float(ra)
        if parsed is not None:
            retry_after = parsed
        else:
            # HTTP-date form (e.g. "Wed, 21 Oct 2026 07:28:00 GMT").
            from email.utils import parsedate_to_datetime

            try:
                retry_after = max(0.0, (parsedate_to_datetime(ra) - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError):  # pragma: no cover — defensive
                retry_after = None

    return {
        "limit": limit,
        "remaining": remaining,
        "reset_at": reset_at,
        "retry_after": retry_after,
    }


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


class RPCRateTracker:
    """Persisted per-endpoint rate-limit tracker.

    Single-writer, JSON-on-disk: every mutation bumps ``updated_at`` and
    the tracker is flushed to :data:`RPC_THROTTLE_FILE` when
    :meth:`persist` is called (the RPC layer calls it periodically and
    on shutdown). Load is best-effort — a corrupt file degrades to an
    empty tracker rather than crashing the indexer.
    """

    def __init__(self, path: str | Path | None = None) -> None:
        self._path = Path(path or RPC_THROTTLE_FILE)
        self._state: dict[str, EndpointState] = {}
        self._load()

    # ── Persistence ───────────────────────────────────────────────────

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            for endpoint, data in raw.items():
                if not isinstance(data, dict):
                    continue
                data["endpoint"] = endpoint
                self._state[endpoint] = EndpointState(**data)
            if self._state:
                logger.debug("rpc_throttle_loaded", endpoints=len(self._state), path=str(self._path))
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.warning("rpc_throttle_load_failed", path=str(self._path), error=str(exc)[:80])

    def persist(self) -> None:
        """Atomically write the tracker state to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {ep: asdict(state) for ep, state in self._state.items()},
                indent=2,
            )
            tmp = self._path.with_suffix(".json.tmp")
            tmp.write_text(payload)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("rpc_throttle_save_failed", path=str(self._path), error=str(exc)[:80])

    # ── State access ──────────────────────────────────────────────────

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

    # ── Recording ─────────────────────────────────────────────────────

    def record_response(
        self,
        endpoint: str,
        method: str,
        status: int,
        headers: Mapping[str, str] | None = None,
        now: float | None = None,
    ) -> None:
        """Record the outcome of an RPC call and its rate-limit headers.

        Called after every HTTP response (success or 429). Updates the
        advertised budget and — for 429 responses — arms an exact
        Retry-After window (falling back to an exponential window when
        no Retry-After header is present).

        Args:
            endpoint: RPC endpoint URL.
            method: JSON-RPC method name.
            status: HTTP status code.
            headers: Response headers (rate-limit headers are parsed).
            now: Epoch time override (tests).
        """
        state = self._entry(endpoint)
        now = time.time() if now is None else now
        state.updated_at = now
        state.last_status = status

        info = extract_rate_limit(headers or {})
        if info["limit"] is not None:
            state.limit = info["limit"]
        if info["remaining"] is not None:
            state.remaining = info["remaining"]
        if info["reset_at"] is not None:
            state.reset_at = info["reset_at"]

        if status == 429:
            state.rate_limit_hits += 1
            wait = info["retry_after"]
            if wait is not None and wait > 0:
                state.backoff_until = now + wait
                logger.warning(
                    "rpc_throttle_retry_after",
                    endpoint=endpoint[:60],
                    method=method,
                    wait_s=round(wait, 1),
                )
            else:
                # No Retry-After header: exponential window scaled by the
                # number of consecutive 429s (cap at 5 min).
                state.backoff_until = now + min(300, 2 ** min(state.rate_limit_hits, 8))
                logger.warning(
                    "rpc_throttle_429_backoff",
                    endpoint=endpoint[:60],
                    method=method,
                    hits=state.rate_limit_hits,
                )
        elif status == 200 and state.rate_limit_hits > 0:
            # A clean success closes the 429 streak (half-open probe style).
            state.rate_limit_hits = 0

    # ── Wait computation ──────────────────────────────────────────────

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
        if state.limit and state.remaining is not None and state.reset_at is not None:
            if state.consumed_pct() >= RPC_PREEMPTIVE_THRESHOLD_PCT:
                seconds_left = max(0.0, state.reset_at - now)
                if seconds_left > 0 and state.remaining > 0:
                    pace = seconds_left / state.remaining
                    if pace > 0:
                        return min(pace, RPC_THROTTLE_MAX_WAIT)

        # 3. Per-method budget (rolling window enforcement).
        rate = RPC_METHOD_REQ_PER_SEC.get(method, RPC_METHOD_DEFAULT_RATE)
        if rate <= 0:
            return 0.0
        window = max(1.0, 1.0 / rate)
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


# Module-level tracker shared across the codebase (persists to disk).
RATE_TRACKER = RPCRateTracker()


async def throttle_before(endpoint: str, method: str) -> None:
    """Block until a request to ``endpoint`` for ``method`` may proceed.

    Waits for the exact Retry-After/backoff window when one is armed, or
    paces pre-emptively from the advertised budget / per-method rate.
    Uses the module-level :data:`RATE_TRACKER` so all RPC paths share one
    view of each endpoint.

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
    """
    import asyncio

    wait = RATE_TRACKER.wait_seconds_for(endpoint, method)
    if wait > 0:
        logger.debug(
            "rpc_throttle_wait",
            endpoint=endpoint[:60],
            method=method,
            wait_s=round(wait, 2),
        )
        await asyncio.sleep(wait)
    RATE_TRACKER.mark_sent(endpoint, method)


def record_rpc_response(
    endpoint: str,
    method: str,
    status: int,
    headers: Mapping[str, str] | None = None,
) -> None:
    """Record a response in the shared tracker (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.
        method: JSON-RPC method name.
        status: HTTP status code.
        headers: Response headers (rate-limit headers are parsed).
    """
    RATE_TRACKER.record_response(endpoint, method, status, headers)


__all__ = [
    "EndpointState",
    "RATE_TRACKER",
    "RPCRateTracker",
    "extract_rate_limit",
    "record_rpc_response",
    "throttle_before",
]
