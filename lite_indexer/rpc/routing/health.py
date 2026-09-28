"""Endpoint health tracking — circuit breaker, EWMA latency, staleness gate.

Per-endpoint circuit breaker (open / half-open / closed) plus the
EWMA latency / windowed error-rate / in-flight / block-height signals
used by P2C selection and the staleness gate. All state is in-process
and safe under asyncio (single-threaded event loop, no locks required).

Split out of the former monolithic ``rpc/routing.py``.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config import (
    CIRCUIT_COOLDOWN_SECONDS,
    CIRCUIT_DESPERATION_PROBE_INTERVAL,
    CIRCUIT_FAILURE_THRESHOLD,
    CIRCUIT_WINDOW_SECONDS,
    RPC_STALE_TIP_BLOCKS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)
from lite_indexer.rpc.routing.seams import damping as damping_value  # noqa: E402

# ── Per-Endpoint Circuit Breaker ────────────────────────────────────────

class RpcCircuitBreaker:
    """Per-endpoint circuit breaker (open / half-open / closed).

    An endpoint is tripped *open* when it records
    :data:`CIRCUIT_FAILURE_THRESHOLD` failures within
    :data:`CIRCUIT_WINDOW_SECONDS`. While open it is skipped entirely
    (``allow`` returns False). After :data:`CIRCUIT_COOLDOWN_SECONDS` it
    half-opens: one probe call is allowed; success closes the circuit,
    another failure re-opens it.

    This is complementary to the exponential backoff in the pool —
    backoff slows retries, the breaker stops wasting calls on dead
    endpoints entirely.
    """

    def __init__(
        self,
        failure_threshold: int = CIRCUIT_FAILURE_THRESHOLD,
        window_seconds: float = CIRCUIT_WINDOW_SECONDS,
        cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS,
        desperation_probe_interval: float = CIRCUIT_DESPERATION_PROBE_INTERVAL,
    ) -> None:
        self._threshold = failure_threshold
        self._window = window_seconds
        self._cooldown = cooldown_seconds
        self._desperation_interval = desperation_probe_interval
        # endpoint -> (state, opened_at, failures)
        self._state: dict[str, tuple[str, float, list[float]]] = {}
        # chain-pool key -> last desperation probe epoch. Used after a
        # pool-wide outage so the least-banned endpoint is probed at most
        # once per interval instead of every cycle hammering all endpoints.
        self._last_desperation_probe: dict[str, float] = {}

    def _entry(self, endpoint: str) -> tuple[str, float, list[float]]:
        state = self._state.get(endpoint)
        if state is None:
            state = ("closed", 0.0, [])
            self._state[endpoint] = state
        return state

    def state(self, endpoint: str) -> str:
        """Return the breaker state: ``closed``, ``open``, or ``half_open``.

        Args:
            endpoint: RPC endpoint URL.
        """
        state, opened_at, _ = self._entry(endpoint)
        if state == "open" and time.time() - opened_at >= self._cooldown:
            return "half_open"
        return state

    def allow(self, endpoint: str) -> bool:
        """Whether a call to this endpoint may proceed now.

        Closed → allowed. Half-open → exactly one probe allowed (the
        caller flips it to "open" on failure via :meth:`record_failure`).
        Open → denied until the cooldown elapses.

        Args:
            endpoint: RPC endpoint URL.

        Returns:
            True when the call may proceed.
        """
        st = self.state(endpoint)
        if st == "closed":
            return True
        if st == "half_open":
            # Allow one probe, but flip to a probing state so concurrent
            # callers don't all hammer it: we mark it as "open" with a
            # zero cooldown; record_success/failure resolve it.
            self._state[endpoint] = ("probe", 0.0, self._entry(endpoint)[2])
            return True
        return False

    def force_probe(self, endpoint: str) -> bool:
        """Bypass the ban/open state for a single request (desperation probe).

        Used after a pool-wide outage: every endpoint is open/banned and
        the cooldown may be far in the future. This lets the caller make
        exactly one request to the least-banned endpoint without waiting
        for the cooldown to elapse. The request outcome resolves the
        circuit exactly like a normal half-open probe: success closes,
        failure re-opens.

        Args:
            endpoint: RPC endpoint URL.

        Returns:
            Always True — the probe may proceed (state is armed for a
            single request; concurrent callers see it as ``probe``).
        """
        self._state[endpoint] = ("probe", 0.0, self._entry(endpoint)[2])
        return True

    def desperation_probe_due(self, pool_key: str, now: float | None = None) -> bool:
        """Whether a pool-wide desperation probe may fire for ``pool_key``.

        After every endpoint in a chain's pool is open/banned, callers
        check this before force-probing so the least-banned endpoint is
        probed at most once per :data:`CIRCUIT_DESPERATION_PROBE_INTERVAL`
        instead of being hammered every cycle.

        Args:
            pool_key: Stable key for the chain pool (typically the chain
                name, e.g. ``"ethereum"``).
            now: Epoch time override (tests).

        Returns:
            True when the interval has elapsed since the last probe.
        """
        now = time.time() if now is None else now
        return now - self._last_desperation_probe.get(pool_key, 0.0) >= self._desperation_interval

    def mark_desperation_probe(self, pool_key: str, now: float | None = None) -> None:
        """Record that a desperation probe fired for ``pool_key``.

        Args:
            pool_key: Stable key for the chain pool.
            now: Epoch time override (tests).
        """
        self._last_desperation_probe[pool_key] = time.time() if now is None else now

    def record_success(self, endpoint: str) -> None:
        """Close the circuit after a successful call/probe.

        Args:
            endpoint: RPC endpoint URL.
        """
        self._state[endpoint] = ("closed", 0.0, [])

    def record_failure(self, endpoint: str) -> None:
        """Record a failure; trip the circuit open when the threshold is met.

        Args:
            endpoint: RPC endpoint URL.
        """
        st, opened_at, failures = self._entry(endpoint)
        now = time.time()
        failures = [f for f in failures if now - f < self._window]
        failures.append(now)

        if st == "probe" or len(failures) >= self._threshold:
            self._state[endpoint] = ("open", now, failures)
            logger.warning("rpc_circuit_opened", endpoint=endpoint[:60], failures=len(failures))
            return
        self._state[endpoint] = ("closed", 0.0, failures)

    def is_open(self, endpoint: str) -> bool:
        """Whether calls to this endpoint should currently be skipped.

        Args:
            endpoint: RPC endpoint URL.
        """
        return self.state(endpoint) == "open"

    def least_banned_endpoint(self, endpoints: list[str]) -> str:
        """Return the endpoint least penalized by the breaker.

        Picks the endpoint whose circuit state is most recoverable:
        closed > half_open > open, then by the earliest ``opened_at``
        (so an open endpoint whose cooldown elapses soonest wins).

        Args:
            endpoints: Candidate endpoint URLs.

        Returns:
            The least-banned endpoint URL.
        """
        if not endpoints:
            raise ValueError("least_banned_endpoint requires a non-empty endpoint list")

        def _rank(ep: str) -> tuple[int, float]:
            st, opened_at, _ = self._entry(ep)
            state_rank = {"closed": 0, "half_open": 1, "probe": 1, "open": 2}.get(st, 0)
            return (state_rank, opened_at)

        return min(endpoints, key=_rank)

# Module-level breaker shared across the codebase.
CIRCUIT_BREAKER = RpcCircuitBreaker()

def is_circuit_open(endpoint: str) -> bool:
    """Check the shared breaker for an endpoint (convenience wrapper).

    The breaker comes from :mod:`lite_indexer.rpc.routing.seams` so tests
    and operators can inject a replacement.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        True when the circuit is open (skip this endpoint).
    """
    from lite_indexer.rpc.routing.seams import circuit_breaker

    return circuit_breaker().is_open(endpoint)

# ── EWMA Latency / Staleness Gate ─────────────────────────────────────
#
# An additive smart-routing signal layer on top of the penalty scoring +
# circuit breaker in :mod:`lite_indexer.rpc.pool`. Nothing here bypasses
# the breaker: banned / circuit-open endpoints are still filtered out by
# the pool scoring before P2C selection runs.

# EWMA smoothing factor: new_sample_weight = 0.2, so
# ``ewma = 0.8 * old + 0.2 * new`` per the audit spec.
EWMA_ALPHA = 0.2
# Conservative initial EWMA before the first latency sample — a fresh
# endpoint is not preferred over one with proven low latency.
EWMA_INITIAL_MS = 1000.0
# Window (seconds) for the rolling error-rate computation.
ERROR_WINDOW_SECONDS = 300.0
# Staleness gate: an endpoint lagging more than this many blocks behind
# the pool median block height is excluded until it catches up. Env-tunable
# via RPC_STALE_TIP_BLOCKS (default 5) so a chain whose providers advance at
# different cadences can widen/narrow the eviction window without a deploy.
STALENESS_MAX_BLOCKS = RPC_STALE_TIP_BLOCKS
# Penalty applied in the pool scoring for a stale endpoint. Must clear
# the ``healthy`` cutoff (``MAX_FAILURES * 1000`` = 3000) so stale
# endpoints drop out of rotation entirely; 15000 mirrors the
# health-verification penalty (+10000) and sits below the circuit-open
# (+20000) / hard-ban (+50000) penalties so the failure path still wins.
STALE_ENDPOINT_PENALTY = 15000
# Tip-failure demotion: an endpoint that cannot answer ``eth_blockNumber``
# (null result / transport error) records no block height, so the staleness
# gate above can never see it as lagging — it stays "fresh" forever and can
# be picked for the tip poll, where it stalls the chain. Consecutive tip
# failures demote it from tip rotation until it records a height.
TIP_FAILURE_THRESHOLD = 2
TIP_FAILURE_PENALTY = 15000

@dataclass
class EndpointLatency:
    """Per-endpoint EWMA latency + windowed error-rate tracking.

    This is an additive signal used by P2C selection and the staleness
    gate. It is deliberately independent of :class:`RpcCircuitBreaker`
    and the pool failure tracker — it never trips or resets a circuit.
    """

    endpoint: str
    ewma_ms: float = EWMA_INITIAL_MS
    calls: int = 0
    in_flight: int = 0
    last_block_height: int | None = None
    last_updated_at: float = 0.0
    # Consecutive tip-observation failures since the last successful height
    # record. Non-zero (>= TIP_FAILURE_THRESHOLD) means the endpoint cannot
    # currently serve a fresh tip; it is demoted from tip rotation until it
    # records a height.
    tip_failures: int = 0
    # (timestamp, ok) pairs within ERROR_WINDOW_SECONDS; pruned on write.
    _outcomes: list[tuple[float, bool]] = field(default_factory=list, init=False, repr=False)

    def error_rate(self, now: float | None = None) -> float:
        """Fraction of calls that failed within the error window.

        Args:
            now: Epoch time override (tests).

        Returns:
            ``0.0`` when there is no recent data, else ``errors / calls``.
        """
        now = time.time() if now is None else now
        recent = [ok for ts, ok in self._outcomes if now - ts <= ERROR_WINDOW_SECONDS]
        if not recent:
            return 0.0
        return sum(0 if ok else 1 for ok in recent) / len(recent)

    def damped_score(self) -> float:
        """P2C selection score: EWMA latency damped by in-flight load AND
        recent success ratio.

        ``score = ewma_ms * (1 + in_flight) * (1 + ERROR_RATE_DAMPING * error_rate)``
        — an endpoint already serving many in-flight requests is penalized
        (load spreads), and one with a poor windowed error rate is damped
        even between recorded failures, so P2C stops drawing flaky
        endpoints that recover latency-wise but fail half their calls.

        ``ERROR_RATE_DAMPING`` is read through the
        ``lite_indexer.rpc.routing`` package at call time so that
        monkeypatching / reloading the package takes effect here.
        """

        damping = damping_value()
        base = self.ewma_ms * (1.0 + self.in_flight)
        return base * (1.0 + damping * self.error_rate())

# Module-level latency state shared across the codebase.
_LATENCY: dict[str, EndpointLatency] = {}
# Endpoints currently demoted by the stale-tip gate. Tracked only so the
# demote/recover transition is logged once per episode instead of on every
# pool selection.
_demoted_tips: set[str] = set()

def _endpoint_label(endpoint: str) -> str:
    """Non-secret endpoint identity for logs (domain + hashed key id)."""
    try:
        from lite_indexer.rpc.provider_caps import account_key_for_endpoint

        return account_key_for_endpoint(endpoint)
    except Exception:  # noqa: BLE001 — label must never break selection
        return "unknown"

def _note_tip_demotions(stale: list[str], median: int | None) -> None:
    """Log each endpoint's stale-tip demote / catch-up transition once."""
    stale_set = set(stale)
    for ep in sorted(stale_set - _demoted_tips):
        stats = _stats(ep)
        lag = None
        if median is not None and stats.last_block_height is not None:
            lag = median - stats.last_block_height
        logger.warning(
            "rpc_stale_tip_demoted",
            endpoint=_endpoint_label(ep),
            height=stats.last_block_height,
            pool_median=median,
            lag_blocks=lag,
            threshold=STALENESS_MAX_BLOCKS,
        )
        metrics_incr("rpc_stale_tip_demoted")
    for ep in sorted(_demoted_tips - stale_set):
        logger.info("rpc_stale_tip_recovered", endpoint=_endpoint_label(ep))
        metrics_incr("rpc_stale_tip_recovered")
    _demoted_tips.clear()
    _demoted_tips.update(stale_set)

def _stats(endpoint: str) -> EndpointLatency:
    """Return (creating on first use) the latency stats for ``endpoint``."""
    stats = _LATENCY.get(endpoint)
    if stats is None:
        stats = EndpointLatency(endpoint=endpoint)
        _LATENCY[endpoint] = stats
    return stats

def record_rpc_latency(endpoint: str, latency_ms: float) -> None:
    """Update the per-endpoint EWMA latency (α=0.2).

    Args:
        endpoint: RPC endpoint URL.
        latency_ms: Measured call latency in milliseconds (>= 0).
    """
    stats = _stats(endpoint)
    stats.ewma_ms = (1.0 - EWMA_ALPHA) * stats.ewma_ms + EWMA_ALPHA * max(0.0, latency_ms)
    stats.last_updated_at = time.time()

def record_rpc_outcome(endpoint: str, *, ok: bool) -> None:
    """Record a call outcome for the windowed error rate.

    Does NOT touch the circuit breaker — breaker state is only managed
    by :class:`RpcCircuitBreaker` / the pool failure tracker.

    Args:
        endpoint: RPC endpoint URL.
        ok: Whether the call succeeded.
    """
    stats = _stats(endpoint)
    now = time.time()
    stats.calls += 1
    stats._outcomes.append((now, ok))
    stats._outcomes = [(ts, o) for ts, o in stats._outcomes if now - ts <= ERROR_WINDOW_SECONDS]

def record_rpc_call(endpoint: str, *, ok: bool, latency_ms: float | None = None) -> None:
    """Record latency + outcome for one RPC call (convenience wrapper).

    Args:
        endpoint: RPC endpoint URL.
        ok: Whether the call succeeded.
        latency_ms: Optional measured latency in milliseconds.
    """
    if latency_ms is not None:
        record_rpc_latency(endpoint, latency_ms)
    record_rpc_outcome(endpoint, ok=ok)

def endpoint_ewma_ms(endpoint: str) -> float:
    """Current EWMA latency (ms) for an endpoint (never raises)."""
    return _stats(endpoint).ewma_ms

def endpoint_error_rate(endpoint: str) -> float:
    """Windowed error rate for an endpoint (0.0 when no data)."""
    return _stats(endpoint).error_rate()

def incr_in_flight(endpoint: str) -> None:
    """Increment the in-flight request count for ``endpoint``."""
    _stats(endpoint).in_flight += 1

def decr_in_flight(endpoint: str) -> None:
    """Decrement the in-flight request count (floored at 0)."""
    stats = _stats(endpoint)
    stats.in_flight = max(0, stats.in_flight - 1)

def in_flight_count(endpoint: str) -> int:
    """Current in-flight request count for ``endpoint``."""
    return _stats(endpoint).in_flight

@asynccontextmanager
async def track_in_flight(endpoint: str) -> AsyncIterator[None]:
    """Context manager that tracks in-flight load for ``endpoint``.

    Use around each RPC call so the P2C damped score reflects concurrent
    load::

        async with track_in_flight(endpoint):
            resp = await client.post(endpoint, ...)
    """
    incr_in_flight(endpoint)
    try:
        yield
    finally:
        decr_in_flight(endpoint)

def record_endpoint_block_height(endpoint: str, height: int) -> None:
    """Record the latest block height observed from ``endpoint``.

    Feeds the staleness gate: an endpoint lagging more than
    :data:`STALENESS_MAX_BLOCKS` behind the pool median is excluded from
    rotation until it records a height within the threshold (i.e. it
    catches up). Call this after every successful tip-observation call
    (``eth_blockNumber`` / ``getBlockHeight``).

    The height is kept monotonic (``max``) so a historical/backfill read
    that requests an old block number can never *lower* the endpoint's
    recorded tip and wrongly mark a healthy endpoint stale. A successful
    tip observation also clears the tip-failure counter.

    Args:
        endpoint: RPC endpoint URL.
        height: Latest block height observed from this endpoint.
    """
    if height is None or height <= 0:
        return
    stats = _stats(endpoint)
    stats.last_block_height = max(stats.last_block_height or 0, height)
    stats.tip_failures = 0
    stats.last_updated_at = time.time()

def record_endpoint_tip_failure(endpoint: str) -> int:
    """Record a failed tip observation (null/error ``eth_blockNumber``).

    An endpoint that fails to return a tip records no height, which would
    otherwise leave it permanently exempt from the staleness gate ("no
    data never penalizes"). Counting the failures lets the selector demote
    a tip-incapable endpoint until it can actually serve a fresh tip.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        The endpoint's consecutive tip-failure count after this record.
    """
    stats = _stats(endpoint)
    stats.tip_failures += 1
    stats.last_updated_at = time.time()
    return stats.tip_failures

def is_endpoint_tip_failed(endpoint: str) -> bool:
    """Whether ``endpoint`` is currently demoted for repeated tip failures."""
    return _stats(endpoint).tip_failures >= TIP_FAILURE_THRESHOLD

def tip_failed_endpoints(endpoints: list[str]) -> list[str]:
    """All pool endpoints currently demoted for repeated tip failures."""
    return [ep for ep in endpoints if is_endpoint_tip_failed(ep)]

def endpoint_block_height(endpoint: str) -> int | None:
    """Last recorded block height for an endpoint, or None."""
    return _stats(endpoint).last_block_height

def _pool_median_height(endpoints: list[str]) -> int | None:
    """Median of recorded block heights across the pool (None if none)."""
    heights: list[int] = []
    for ep in endpoints:
        height = _stats(ep).last_block_height
        if height is not None:
            heights.append(height)
    if not heights:
        return None
    heights.sort()
    mid = len(heights) // 2
    if len(heights) % 2:
        return heights[mid]
    return (heights[mid - 1] + heights[mid]) // 2

def staleness_blocks(endpoint: str, endpoints: list[str]) -> int | None:
    """How many blocks ``endpoint`` lags behind the pool median height.

    Returns None when either the endpoint or the pool has no recorded
    block height — the gate is inactive in that case (absence of data
    never penalizes an endpoint).

    Args:
        endpoint: RPC endpoint URL.
        endpoints: Full pool candidate list (for the median).

    Returns:
        ``median - endpoint_height`` or None when the gate is inactive.
    """
    height = _stats(endpoint).last_block_height
    median = _pool_median_height(endpoints)
    if height is None or median is None:
        return None
    return median - height

def is_endpoint_stale(endpoint: str, endpoints: list[str]) -> bool:
    """Whether ``endpoint`` lags >STALENESS_MAX_BLOCKS behind the median.

    Args:
        endpoint: RPC endpoint URL.
        endpoints: Full pool candidate list (for the median).

    Returns:
        True when the endpoint is stale and should be excluded.
    """
    lag = staleness_blocks(endpoint, endpoints)
    return lag is not None and lag > STALENESS_MAX_BLOCKS

def stale_endpoints(endpoints: list[str]) -> list[str]:
    """All pool endpoints lagging >STALENESS_MAX_BLOCKS behind the median.

    Args:
        endpoints: Pool candidate URLs.

    Returns:
        Stale endpoint URLs (empty when no height data is recorded).
    """
    median = _pool_median_height(endpoints)
    if median is None:
        return []
    stale: list[str] = []
    for ep in endpoints:
        height = _stats(ep).last_block_height
        if height is not None and median - height > STALENESS_MAX_BLOCKS:
            stale.append(ep)
    _note_tip_demotions(stale, median)
    return stale

def latency_snapshot() -> dict[str, dict[str, Any]]:
    """Per-endpoint EWMA/error-rate/in-flight snapshot (for logging)."""
    return {
        ep: {
            "ewma_ms": round(s.ewma_ms, 1),
            "error_rate": round(s.error_rate(), 4),
            "in_flight": s.in_flight,
            "calls": s.calls,
            "last_block_height": s.last_block_height,
        }
        for ep, s in _LATENCY.items()
    }

def stale_demoted_count() -> int:
    """Number of endpoints currently demoted by the stale-tip gate."""
    return len(_demoted_tips)

def reset_latency_state() -> None:
    """Clear all latency/height tracking (tests, operator reset)."""
    _LATENCY.clear()
    _demoted_tips.clear()
