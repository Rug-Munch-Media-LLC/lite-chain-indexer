"""
Per-account (provider) rate caps — concurrency + requests/sec per provider.

ADDITIVE on top of the per-endpoint throttle (``rpc/throttle.py``) and the
per-provider token bucket (``rpc/ratelimit.py``). Those pace ONE endpoint;
this layer paces the ACCOUNT that owns a set of endpoints.

Why it exists: providers ban the *account*, not a single URL. Hitting many
of a provider's endpoints at once (20 chains, several workers) exhausts the
account budget and the key gets banned — every endpoint behind it dies at
once. We therefore cap:

  * **concurrency** — at most ``RPC_PROVIDER_MAX_CONCURRENCY`` in-flight
    requests per provider, and
  * **rate** — at most ``RPC_PROVIDER_RPS`` requests/sec per provider,

keyed by the ACCOUNT identity: ``provider_id`` + ``key_id``
(:func:`account_key_for_endpoint`). Every URL of one API key shares a budget,
but two DIFFERENT keys on the same host (e.g. two KeyedProvider accounts) get
independent concurrency/RPS budgets and backoff windows. Key-less providers
group by ``provider_id`` alone. When the budget is exhausted the caller
*queues* (awaits the semaphore / paces) instead of firing.

Provider-level ``Retry-After`` is honored too: a 429 on any endpoint of the
account arms a backoff window for the whole account, so we stop burning the
other endpoints of the same banned provider.

Env:
    RPC_PROVIDER_MAX_CONCURRENCY  concurrent in-flight per provider (4)
    RPC_PROVIDER_RPS              requests/sec per provider (10)
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, cast

from lite_indexer.config import (
    RPC_PROVIDER_MAX_CONCURRENCY,
    RPC_PROVIDER_RPS,
    RPC_THROTTLE_MAX_WAIT,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.ledger import key_id_for_endpoint, provider_id_for_endpoint
from lite_indexer.rpc.throttle import extract_rate_limit

logger = get_logger(__name__)

# Bounded slot wait: fail fast on an orphaned slot, never hang (2026-09-20).
_SLOT_WAIT_CAP_S = 300.0


def account_key_for_endpoint(endpoint: str) -> str:
    """Account identity for cap keying: ``provider_id`` + optional ``key_id``.

    ``*.keyegateway.com`` URLs with the SAME key collapse to one account key, but
    two different keys on the same host get DIFFERENT keys (independent
    budgets). Key-less providers return the bare ``provider_id`` so their
    behaviour is unchanged. The ``key_id`` is a non-secret hash
    (:func:`key_id_for_endpoint`).
    """
    provider_id = provider_id_for_endpoint(endpoint)
    key_id = key_id_for_endpoint(endpoint)
    return f"{provider_id}:{key_id}" if key_id else provider_id


@dataclass
class ProviderState:
    """In-process rate-cap state for one provider account."""

    provider_id: str
    # Concurrency accounting (mirrors the per-provider semaphore).
    in_flight: int = 0
    max_in_flight: int = 0
    # Rolling request timestamps (requests/sec pacing).
    request_times: list[float] = field(default_factory=list)
    # Absolute epoch until which the ACCOUNT must not be called (Retry-After).
    backoff_until: float = 0.0
    rate_limit_hits: int = 0
    last_status: int = 200
    updated_at: float = 0.0

    def in_backoff(self, now: float | None = None) -> bool:
        """Whether the provider account is inside a backoff window."""
        now = time.time() if now is None else now
        return self.backoff_until > now

    def remaining_wait(self, now: float | None = None) -> float:
        """Seconds until the provider backoff window elapses (0 when open)."""
        now = time.time() if now is None else now
        return max(0.0, self.backoff_until - now)


class ProviderRateLimiter:
    """Per-account concurrency + requests/sec limiter.

    State is in-process and safe under asyncio (single-threaded event loop,
    no locks required). ``acquire``/``release`` bracket a dispatched call and
    enforce both caps; ``record_response`` arms/clears the account-level
    ``Retry-After`` window from real 429/200 responses.
    """

    def __init__(
        self,
        rps: float = RPC_PROVIDER_RPS,
        max_concurrency: int = RPC_PROVIDER_MAX_CONCURRENCY,
        max_wait: float = RPC_THROTTLE_MAX_WAIT,
    ) -> None:
        self._rps = max(0.0, float(rps))
        self._max_concurrency = max(0, int(max_concurrency))
        self._max_wait = max(0.0, float(max_wait))
        self._states: dict[str, ProviderState] = {}
        self._semaphores: dict[str, asyncio.Semaphore] = {}

    # ── State access ──────────────────────────────────────────────────

    def _state(self, provider_id: str) -> ProviderState:
        state = self._states.get(provider_id)
        if state is None:
            state = ProviderState(provider_id=provider_id)
            self._states[provider_id] = state
        return state

    def _semaphore(self, provider_id: str) -> asyncio.Semaphore:
        sem = self._semaphores.get(provider_id)
        if sem is None:
            # Zero/negative cap = "unlimited" (Semaphore(0) would deadlock).
            limit = self._max_concurrency if self._max_concurrency > 0 else 1_000_000
            sem = asyncio.Semaphore(limit)
            self._semaphores[provider_id] = sem
        return sem

    def wait_seconds_for(self, provider_id: str, now: float | None = None) -> float:
        """Seconds to wait before the next request for ``provider_id``.

        Priority: (1) account backoff from a provider-level Retry-After,
        (2) rolling requests/sec pacing. Concurrency is enforced separately
        by the semaphore in :meth:`acquire`.

        Args:
            provider_id: Account-level provider id.
            now: Epoch time override (tests).

        Returns:
            Seconds to sleep (0 = call now).
        """
        state = self._state(provider_id)
        now = time.time() if now is None else now

        if state.in_backoff(now):
            return state.remaining_wait(now)

        if self._rps <= 0:
            return 0.0
        window = max(0.01, 1.0 / self._rps)
        recent = [t for t in state.request_times if now - t < window]
        state.request_times = recent
        if not recent:
            return 0.0
        next_slot = recent[-1] + window
        return min(max(0.0, next_slot - now), self._max_wait)

    def allow(self, provider_id: str, now: float | None = None) -> bool:
        """Whether a request to ``provider_id`` may proceed immediately."""
        return not self._state(provider_id).in_backoff(now)

    def mark_sent(self, provider_id: str, now: float | None = None) -> None:
        """Record that a request was dispatched (requests/sec accounting)."""
        state = self._state(provider_id)
        now = time.time() if now is None else now
        state.request_times.append(now)
        state.updated_at = now

    def record_retry_after(self, provider_id: str, wait: float, now: float | None = None) -> None:
        """Arm an account-level backoff window (provider Retry-After)."""
        state = self._state(provider_id)
        now = time.time() if now is None else now
        if wait > 0:
            state.backoff_until = max(state.backoff_until, now + wait)
            state.rate_limit_hits += 1
            state.updated_at = now
            metrics_incr("rpc_provider_backoffs")
            logger.warning(
                "provider_retry_after",
                provider=provider_id,
                wait_s=round(wait, 1),
            )

    def record_response(
        self,
        provider_id: str,
        status: int,
        headers: Mapping[str, str] | None = None,
        now: float | None = None,
    ) -> None:
        """Record a provider response; honor Retry-After at the ACCOUNT level.

        A 429 arms an exact ``Retry-After`` window when advertised, else an
        exponential window scaled by the consecutive-hit count. A 200 closes
        the streak (half-open probe style).
        """
        state = self._state(provider_id)
        now = time.time() if now is None else now
        state.last_status = status
        state.updated_at = now

        if status == 429:
            info = extract_rate_limit(headers or {})
            wait = cast(float | None, info["retry_after"])
            if wait is not None and wait > 0:
                self.record_retry_after(provider_id, wait, now)
            else:
                state.rate_limit_hits += 1
                state.backoff_until = max(
                    state.backoff_until,
                    now + min(300, 2 ** min(state.rate_limit_hits, 8)),
                )
                metrics_incr("rpc_provider_backoffs")
                logger.warning(
                    "provider_429_backoff",
                    provider=provider_id,
                    hits=state.rate_limit_hits,
                )
        elif status == 200 and state.rate_limit_hits > 0:
            state.rate_limit_hits = 0

    def state_for(self, provider_id: str, now: float | None = None) -> dict[str, Any]:
        """Public read-only view of one provider's rate-cap state."""
        state = self._states.get(provider_id)
        if state is None:
            return {}
        now = time.time() if now is None else now
        return {
            "provider_id": state.provider_id,
            "in_flight": state.in_flight,
            "max_in_flight": state.max_in_flight,
            "max_concurrency": self._max_concurrency,
            "rps": self._rps,
            "in_backoff": state.in_backoff(now),
            "backoff_until": state.backoff_until,
            "rate_limit_hits": state.rate_limit_hits,
            "last_status": state.last_status,
        }

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Full state view (for logging / metrics)."""
        return {pid: self.state_for(pid) for pid in self._states}

    def reset(self, provider_id: str | None = None) -> None:
        """Clear limiter state (one provider or all)."""
        if provider_id is None:
            self._states.clear()
            self._semaphores.clear()
        else:
            self._states.pop(provider_id, None)
            self._semaphores.pop(provider_id, None)

    # ── Dispatch gate ─────────────────────────────────────────────────

    async def acquire_slot(self, provider_id: str) -> None:
        """Acquire a provider concurrency slot (queue, never drop).

        Concurrency-only: waits for a free in-flight slot. Rate pacing and
        account backoff are handled by :meth:`wait_seconds_for` /
        :func:`provider_throttle_before` so retries inside a call do not
        re-pace against the provider budget. Bounded: orphaned slots fail fast.
        """
        try:
            await asyncio.wait_for(self._semaphore(provider_id).acquire(), timeout=_SLOT_WAIT_CAP_S)
        except TimeoutError:
            logger.warning("rpc_slot_wait_timeout", provider=provider_id, cap_s=_SLOT_WAIT_CAP_S)
            raise
        state = self._state(provider_id)
        state.in_flight += 1
        state.max_in_flight = max(state.max_in_flight, state.in_flight)

    def release_slot(self, provider_id: str) -> None:
        """Release a concurrency slot acquired via :meth:`acquire_slot`."""
        state = self._state(provider_id)
        state.in_flight = max(0, state.in_flight - 1)
        sem = self._semaphores.get(provider_id)
        if sem is not None:
            sem.release()

    async def acquire(self, provider_id: str) -> None:
        """Acquire a slot AND honor provider rate/backoff (full gate).

        On cancellation the slot is released so a cancelled caller cannot
        leak it.
        """
        await self.acquire_slot(provider_id)
        try:
            wait = self.wait_seconds_for(provider_id)
            if wait > 0:
                logger.debug("provider_rate_wait", provider=provider_id, wait_s=round(wait, 2))
                await asyncio.sleep(wait)
            self.mark_sent(provider_id)
        except BaseException:
            self.release_slot(provider_id)
            raise

    def release(self, provider_id: str) -> None:
        """Release a provider slot acquired via :meth:`acquire`."""
        self.release_slot(provider_id)


# Module-level limiter shared across the codebase.
PROVIDER_LIMITER = ProviderRateLimiter()


@asynccontextmanager
async def provider_slot(provider_id: str) -> AsyncIterator[None]:
    """Async context manager bracketing one provider-capped dispatch.

    Queues when the provider concurrency budget is exhausted AND honors the
    provider requests/sec + backoff budget; never drops the call. Use
    :func:`provider_concurrency_slot` around a retry loop instead, with a
    single :func:`provider_throttle_before` before it, so retries do not
    re-pace the provider budget.
    """
    await PROVIDER_LIMITER.acquire(provider_id)
    try:
        yield
    finally:
        PROVIDER_LIMITER.release(provider_id)


@asynccontextmanager
async def provider_concurrency_slot(provider_id: str) -> AsyncIterator[None]:
    """Async context manager enforcing ONLY the provider concurrency cap."""
    await PROVIDER_LIMITER.acquire_slot(provider_id)
    try:
        yield
    finally:
        PROVIDER_LIMITER.release_slot(provider_id)


async def provider_throttle_before(endpoint: str) -> None:
    """Provider-level rate gate keyed by an endpoint URL (convenience).

    Waits out an account-level Retry-After / requests/sec pacing window and
    records the dispatch. For concurrency enforcement use the
    :func:`provider_slot` context manager, which brackets the actual call.
    """
    account_key = account_key_for_endpoint(endpoint)
    wait = PROVIDER_LIMITER.wait_seconds_for(account_key)
    if wait > 0:
        logger.debug("provider_rate_wait", provider=account_key, wait_s=round(wait, 2))
        await asyncio.sleep(wait)
    PROVIDER_LIMITER.mark_sent(account_key)


def record_provider_response(
    endpoint: str,
    status: int,
    headers: Mapping[str, str] | None = None,
) -> None:
    """Record a response against the endpoint's provider account.

    Convenience wrapper for the RPC layer: derives the provider id from the
    URL so a 429 on one endpoint arms the account-level Retry-After window.

    Args:
        endpoint: RPC endpoint URL.
        status: HTTP status code.
        headers: Response headers (``Retry-After`` is parsed).
    """
    PROVIDER_LIMITER.record_response(account_key_for_endpoint(endpoint), status, headers)


__all__ = [
    "PROVIDER_LIMITER",
    "ProviderRateLimiter",
    "ProviderState",
    "account_key_for_endpoint",
    "provider_concurrency_slot",
    "provider_slot",
    "provider_throttle_before",
    "record_provider_response",
]
