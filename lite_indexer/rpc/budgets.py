"""Per-provider / per-endpoint request budgets with pre-emptive cool-down.

ADDITIVE on top of the existing pacing layers (``rpc/ratelimit.py`` token
bucket, ``rpc/provider_caps.py`` account caps, ``rpc/throttle.py`` header
tracker). Those either pace a shared profile globally or react to a 429
after it happens. This layer attaches an *identity-specific* budget
(rps, max_concurrency, optional daily request budget) and cools the
identity at ``preemptive_pct`` of its daily budget — i.e. BEFORE the
provider's own 429/softban arrives.

Why: selection favoured the lowest-latency endpoint, so a few endpoints
absorbed load and blew their per-IP caps while 30+ sat idle; per-identity
budgets + pre-emptive cool-down make that routable (rpc/pool).

Enforcement is **explicit-only**: the generic keyless default
(``explicit=False``) is a no-op here so OSS behaviour is unchanged and the
existing provider caps remain the single pacer. An identity is enforced
when an operator supplies it via ``RPC_PROVIDER_BUDGETS`` /
``RPC_ENDPOINT_BUDGETS`` (or sets a daily budget for it).

State is in-process (single-threaded event loop, no locks); cool-down
recovers on success and resets at the UTC day rollover.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import (
    DEFAULT_PROVIDER_BUDGET,
    RPC_BUDGET_COOLDOWN_BACKOFF,
    RPC_BUDGET_COOLDOWN_MAX_SECONDS,
    RPC_BUDGET_COOLDOWN_SECONDS,
    RPC_BUDGET_MAX_WAIT,
    RPC_ENDPOINT_BUDGETS,
    RPC_PROVIDER_BUDGETS,
    ProviderBudget,
    resolve_provider_budget,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.ledger import account_id_for_endpoint, key_id_for_endpoint, provider_id_for_endpoint

logger = get_logger(__name__)

# Bounded slot wait: an orphaned slot must fail fast, not hang (2026-09-20).
_SLOT_WAIT_CAP_S = 300.0

# Rolling-window timestamps pruned to this horizon (bounded deques).
_TIMESTAMP_HORIZON_S = 60.0


def account_key_for_budget(endpoint: str) -> str:
    """Per-KEY identity for rps/concurrency keying: ``provider_id`` + key id.

    Rate limits are metered per key, so two keys of one provider get separate
    pacing/concurrency. The *quota* cap (``quota_key_for_budget``) is per
    ACCOUNT instead, so keys never multiply the daily budget.
    """
    provider_id = provider_id_for_endpoint(endpoint)
    key_id = key_id_for_endpoint(endpoint)
    return f"{provider_id}:{key_id}" if key_id else provider_id


def quota_key_for_budget(endpoint: str) -> str:
    """Per-ACCOUNT identity for the daily quota cap.

    Keyed providers meter quota per account, not per key: N keys of one
    account share ONE daily budget (extra keys buy rate, never quota). Uses
    the ledger's :func:`account_id_for_endpoint`, which defaults to the
    provider id.
    """
    return account_id_for_endpoint(endpoint)


def _utc_day(now: float) -> int:
    """UTC day ordinal for ``now`` (day-rollover key)."""
    return datetime.fromtimestamp(now, tz=UTC).toordinal()


@dataclass
class _BudgetState:
    """In-process budget state for one enforced identity."""

    day: int = 0
    used_today: int = 0
    in_flight: int = 0
    max_in_flight: int = 0
    request_times: deque[float] = field(default_factory=deque)
    cooled_until: float = 0.0
    cooldown_level: int = 0
    preemptive_cools: int = 0
    last_status: int = 200
    # One-shot flags: quota events log once per identity per UTC day.
    approach_logged: bool = False
    exhausted_logged: bool = False


class ProviderBudgetTracker:
    """Enforce per-identity rps / concurrency / daily budgets.

    Args:
        provider_budgets: Provider-account id → budget overrides.
        endpoint_budgets: Exact endpoint URL → budget overrides.
        default_budget: Generic keyless default.
        cooldown_seconds: Base pre-emptive cool-down window.
        cooldown_backoff: Multiplier applied per repeat cool-down.
        max_cooldown_seconds: Ceiling for a single cool-down window.
        max_wait: Cap on the per-call rps pacing sleep.
        time_fn: Clock injection (tests).
    """

    def __init__(
        self,
        *,
        provider_budgets: dict[str, ProviderBudget] | None = None,
        endpoint_budgets: dict[str, ProviderBudget] | None = None,
        default_budget: ProviderBudget | None = None,
        cooldown_seconds: float = RPC_BUDGET_COOLDOWN_SECONDS,
        cooldown_backoff: float = RPC_BUDGET_COOLDOWN_BACKOFF,
        max_cooldown_seconds: float = RPC_BUDGET_COOLDOWN_MAX_SECONDS,
        max_wait: float = RPC_BUDGET_MAX_WAIT,
        time_fn: Callable[[], float] = time.time,
    ) -> None:
        self._provider_budgets = dict(provider_budgets) if provider_budgets is not None else dict(RPC_PROVIDER_BUDGETS)
        self._endpoint_budgets = dict(endpoint_budgets) if endpoint_budgets is not None else dict(RPC_ENDPOINT_BUDGETS)
        self._default = default_budget if default_budget is not None else DEFAULT_PROVIDER_BUDGET
        self._cooldown_seconds = max(0.0, float(cooldown_seconds))
        self._cooldown_backoff = max(1.0, float(cooldown_backoff))
        self._max_cooldown = max(self._cooldown_seconds, float(max_cooldown_seconds))
        self._max_wait = max(0.0, float(max_wait))
        self._time = time_fn
        self._states: dict[str, _BudgetState] = {}
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        # Daily counter keyed by ACCOUNT identity → (UTC day, count): a
        # provider daily_budget is a HARD cap shared by all its keys.
        self._daily_used: dict[str, tuple[int, int]] = {}

    # ── Identity / budget resolution ──────────────────────────────────

    def budget_for(self, endpoint: str) -> ProviderBudget:
        """Resolve the effective budget for ``endpoint``."""
        return resolve_provider_budget(
            provider_id_for_endpoint(endpoint),
            endpoint,
            provider_budgets=self._provider_budgets,
            endpoint_budgets=self._endpoint_budgets,
            default=self._default,
        )

    def budget_key(self, endpoint: str) -> str:
        """Per-KEY concurrency/rps key: endpoint-scoped when overridden, else account."""
        if endpoint in self._endpoint_budgets:
            return f"endpoint:{endpoint}"
        return f"account:{account_key_for_budget(endpoint)}"

    def quota_key(self, endpoint: str) -> str:
        """Per-ACCOUNT quota key: all keys of one account share a daily cap.

        An explicit endpoint override keeps its own endpoint-scoped quota
        (the operator named that exact URL); everything else is aggregated
        per account so keys never multiply the budget.
        """
        if endpoint in self._endpoint_budgets:
            return f"quota:endpoint:{endpoint}"
        return f"quota:{quota_key_for_budget(endpoint)}"

    def enforces(self, endpoint: str) -> bool:
        """Whether this identity is actively budget-enforced."""
        budget = self.budget_for(endpoint)
        return budget.explicit or budget.daily_budget > 0

    # ── State ─────────────────────────────────────────────────────────

    def _state(self, endpoint: str, now: float | None = None) -> _BudgetState:
        now = self._time() if now is None else now
        state = self._states.get(endpoint)
        if state is None:
            state = _BudgetState(day=_utc_day(now))
            self._states[endpoint] = state
        elif state.day != _utc_day(now):
            # UTC day rollover: full recovery.
            state.day = _utc_day(now)
            state.used_today = 0
            state.request_times.clear()
            state.cooled_until = 0.0
            state.cooldown_level = 0
        return state

    def _prune(self, state: _BudgetState, now: float) -> None:
        cutoff = now - _TIMESTAMP_HORIZON_S
        while state.request_times and state.request_times[0] < cutoff:
            state.request_times.popleft()

    def _daily_count(self, endpoint: str, now: float) -> int:
        """Current UTC-day dispatch count for the endpoint's ACCOUNT quota."""
        key = self.quota_key(endpoint)
        day = _utc_day(now)
        entry = self._daily_used.get(key)
        if entry is None or entry[0] != day:
            self._daily_used[key] = (day, 0)
            return 0
        return entry[1]

    def _bump_daily(self, endpoint: str, now: float) -> int:
        """Increment + return the UTC-day count for the ACCOUNT quota."""
        key = self.quota_key(endpoint)
        day = _utc_day(now)
        entry = self._daily_used.get(key)
        count = 0 if entry is None or entry[0] != day else entry[1]
        count += 1
        self._daily_used[key] = (day, count)
        return count

    def _semaphore(self, endpoint: str) -> asyncio.Semaphore:
        key = self.budget_key(endpoint)
        sem = self._semaphores.get(key)
        if sem is None:
            limit = self.budget_for(endpoint).max_concurrency
            sem = asyncio.Semaphore(limit if limit > 0 else 1_000_000)
            self._semaphores[key] = sem
        return sem

    # ── Accounting ────────────────────────────────────────────────────

    def record_dispatch(self, endpoint: str, now: float | None = None) -> None:
        """Record one dispatched request; may trigger the pre-emptive cool.

        Enforces the daily budget as a HARD cap: once the identity's UTC-day
        count reaches ``daily_budget`` it is unavailable for the rest of the
        day. Below that, crossing ``preemptive_pct`` arms the pre-emptive
        cool-down (route around it before the provider 429s).
        """
        if not self.enforces(endpoint):
            return
        now = self._time() if now is None else now
        state = self._state(endpoint, now)
        self._prune(state, now)
        state.used_today += 1
        state.request_times.append(now)
        budget = self.budget_for(endpoint)
        if budget.daily_budget <= 0:
            return
        used = self._bump_daily(endpoint, now)
        if used >= budget.daily_budget:
            if not state.exhausted_logged:
                state.exhausted_logged = True
                metrics_incr("rpc_budget_daily_exhausted")
                logger.warning(
                    "rpc_budget_daily_exhausted",
                    endpoint=endpoint[:60],
                    used_today=used,
                    daily_budget=budget.daily_budget,
                )
            return
        threshold = budget.daily_budget * budget.preemptive_pct / 100.0
        if used >= threshold:
            if not state.approach_logged:
                state.approach_logged = True
                metrics_incr("rpc_budget_daily_approaching")
                logger.warning(
                    "rpc_budget_daily_approaching",
                    endpoint=endpoint[:60],
                    used_today=used,
                    daily_budget=budget.daily_budget,
                    pct=round(100.0 * used / budget.daily_budget, 1),
                )
            if not self.is_cooled(endpoint, now):
                self._cool(endpoint, state, budget, now)

    def _cool(self, endpoint: str, state: _BudgetState, budget: ProviderBudget, now: float) -> None:
        """Arm a pre-emptive cool-down with per-repeat backoff."""
        duration = min(
            self._cooldown_seconds * (self._cooldown_backoff**state.cooldown_level),
            self._max_cooldown,
        )
        state.cooled_until = max(state.cooled_until, now + duration)
        state.cooldown_level += 1
        state.preemptive_cools += 1
        metrics_incr("rpc_budget_preemptive_cools")
        logger.warning(
            "rpc_budget_preemptive_cool",
            endpoint=endpoint[:60],
            used_today=state.used_today,
            daily_budget=budget.daily_budget,
            cooldown_s=round(duration, 1),
        )

    def record_response(self, endpoint: str, status: int, now: float | None = None) -> None:
        """Record a provider response; a 429 cools the identity defensively."""
        if not self.enforces(endpoint):
            return
        now = self._time() if now is None else now
        state = self._state(endpoint, now)
        state.last_status = status
        if status == 429:
            self._cool(endpoint, state, self.budget_for(endpoint), now)

    def record_success(self, endpoint: str, now: float | None = None) -> None:
        """Recover: a success decays the cool-down level and can clear it."""
        state = self._states.get(endpoint)
        if state is None:
            return
        state.last_status = 200
        if state.cooldown_level > 0:
            state.cooldown_level -= 1
        if state.cooldown_level == 0:
            state.cooled_until = 0.0

    # ── Gates ─────────────────────────────────────────────────────────

    def is_cooled(self, endpoint: str, now: float | None = None) -> bool:
        """Whether the identity is inside a pre-emptive cool-down window."""
        if not self.enforces(endpoint):
            return False
        now = self._time() if now is None else now
        state = self._state(endpoint, now)
        return state.cooled_until > now

    def budget_available(self, endpoint: str, now: float | None = None) -> bool:
        """Whether the identity still has budget.

        False when pre-emptively cooled OR when the hard daily cap is
        reached (the identity stays unavailable until the UTC day rolls
        over).
        """
        if not self.enforces(endpoint):
            return True
        now = self._time() if now is None else now
        budget = self.budget_for(endpoint)
        if budget.daily_budget > 0 and self._daily_count(endpoint, now) >= budget.daily_budget:
            return False
        return not self.is_cooled(endpoint, now)

    def wait_seconds(self, endpoint: str, now: float | None = None) -> float:
        """Seconds to pace before the next request (rps budget only).

        A cooled identity returns 0 here: the caller (selection) should route
        around it rather than sleep out the whole cool-down window.
        """
        budget = self.budget_for(endpoint)
        if not self.enforces(endpoint) or budget.rps <= 0:
            return 0.0
        now = self._time() if now is None else now
        if self.is_cooled(endpoint, now):
            return 0.0
        state = self._state(endpoint, now)
        self._prune(state, now)
        window = 1.0 / budget.rps
        recent = [t for t in state.request_times if now - t < window]
        state.request_times = deque(recent)
        if not recent:
            return 0.0
        next_slot = recent[-1] + window
        return min(max(0.0, next_slot - now), self._max_wait)

    def used_today(self, endpoint: str, now: float | None = None) -> int:
        """Requests recorded for the identity during the current UTC day."""
        now = self._time() if now is None else now
        return self._daily_count(endpoint, now)

    # ── Concurrency slot ──────────────────────────────────────────────

    async def acquire_slot(self, endpoint: str) -> None:
        """Acquire a concurrency slot for the identity (bounded wait, never drops)."""
        try:
            await asyncio.wait_for(self._semaphore(endpoint).acquire(), timeout=_SLOT_WAIT_CAP_S)
        except TimeoutError:
            logger.warning("rpc_slot_wait_timeout", endpoint=endpoint[:50], cap_s=_SLOT_WAIT_CAP_S)
            raise
        state = self._state(endpoint)
        state.in_flight += 1
        state.max_in_flight = max(state.max_in_flight, state.in_flight)

    def release_slot(self, endpoint: str) -> None:
        """Release a slot acquired via :meth:`acquire_slot`."""
        state = self._state(endpoint)
        state.in_flight = max(0, state.in_flight - 1)
        sem = self._semaphores.get(self.budget_key(endpoint))
        if sem is not None:
            sem.release()

    # ── Introspection ─────────────────────────────────────────────────

    def snapshot(self) -> dict[str, dict[str, Any]]:
        """Read-only view of every tracked identity (for logging/metrics)."""
        return {
            endpoint: {
                "used_today": self._daily_count(endpoint, self._time()),
                "in_flight": state.in_flight,
                "max_in_flight": state.max_in_flight,
                "cooldown_level": state.cooldown_level,
                "cooled_until": state.cooled_until,
                "preemptive_cools": state.preemptive_cools,
                "last_status": state.last_status,
            }
            for endpoint, state in self._states.items()
        }

    def reset(self, endpoint: str | None = None) -> None:
        """Clear tracker state (one identity or all)."""
        if endpoint is None:
            self._states.clear()
            self._semaphores.clear()
            self._daily_used.clear()
        else:
            self._states.pop(endpoint, None)
            self._semaphores.pop(self.budget_key(endpoint), None)
            self._daily_used.pop(self.quota_key(endpoint), None)


# Module-level tracker shared across the RPC layer.
BUDGET_TRACKER = ProviderBudgetTracker()


@asynccontextmanager
async def budget_slot(endpoint: str) -> AsyncIterator[None]:
    """Bracket one dispatch with the identity's concurrency budget.

    A no-op for non-enforced identities so the generic path pays nothing.
    """
    if not BUDGET_TRACKER.enforces(endpoint):
        yield
        return
    await BUDGET_TRACKER.acquire_slot(endpoint)
    try:
        yield
    finally:
        BUDGET_TRACKER.release_slot(endpoint)


def budget_for_endpoint(endpoint: str) -> ProviderBudget:
    """Effective budget for an endpoint (module singleton)."""
    return BUDGET_TRACKER.budget_for(endpoint)


def budget_available(endpoint: str, now: float | None = None) -> bool:
    """Selection gate: False when the identity is pre-emptively cooled."""
    return BUDGET_TRACKER.budget_available(endpoint, now)


def budget_wait_seconds(endpoint: str, now: float | None = None) -> float:
    """Pacing gate: seconds to wait before the next request."""
    return BUDGET_TRACKER.wait_seconds(endpoint, now)


def record_budget_dispatch(endpoint: str, now: float | None = None) -> None:
    """Record a dispatch against the identity budget."""
    BUDGET_TRACKER.record_dispatch(endpoint, now)


def record_budget_response(endpoint: str, status: int, now: float | None = None) -> None:
    """Record a response against the identity budget (429 → cool)."""
    BUDGET_TRACKER.record_response(endpoint, status, now)


def record_budget_success(endpoint: str, now: float | None = None) -> None:
    """Record a success against the identity budget (recovery)."""
    BUDGET_TRACKER.record_success(endpoint, now)


def reset_budget_state() -> None:
    """Reset the module-level tracker (tests / shutdown)."""
    BUDGET_TRACKER.reset()


__all__ = [
    "BUDGET_TRACKER",
    "ProviderBudgetTracker",
    "account_key_for_budget",
    "budget_available",
    "budget_for_endpoint",
    "budget_slot",
    "budget_wait_seconds",
    "quota_key_for_budget",
    "record_budget_dispatch",
    "record_budget_response",
    "record_budget_success",
    "reset_budget_state",
]
