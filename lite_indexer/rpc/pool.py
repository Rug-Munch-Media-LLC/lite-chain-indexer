"""
RPC endpoint pool — best-endpoint selection, failure tracking, backoff.

Provides load-balanced endpoint selection, exponential backoff on
failures, reactive rate-limit handling after 429 responses, and a
per-endpoint circuit breaker (open after a failure burst) layered on
top of the classic scoring. 429 rate limits trigger backoff only — they
do NOT trip the circuit breaker or hard-ban (see
:func:`record_rate_limit` / :func:`handle_rate_limit`).
"""

from __future__ import annotations

import asyncio
import time

from lite_indexer.config import (
    BACKOFF_CAP,
    ENDPOINT_COOLDOWN,
    MAX_FAILURES,
    RATE_LIMIT_WAIT_CAP,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc.ban_state import (  # re-exported for pool API compat
    ban_endpoint,  # noqa: F401
    clear_429_streak,
    clear_ban_state,  # noqa: F401
    is_endpoint_banned,
    is_endpoint_hard_banned,  # noqa: F401
    is_endpoint_soft_banned,  # noqa: F401
    load_ban_state,  # noqa: F401
    record_429_streak,
    unban_endpoint,
)
from lite_indexer.rpc.budgets import budget_available

logger = get_logger(__name__)

# Penalty applied to an endpoint whose per-identity budget is pre-emptively
# cooled. Matches the circuit-open penalty: the endpoint drops out of the
# healthy set so load flows to a budget-available peer instead of piling on
# the one about to be 429'd.
BUDGET_EXHAUSTED_PENALTY = 20000

# Fairness/rotation counters: how many times each endpoint has been chosen.
# Selection prefers the least-used half of the healthy set (so no endpoint
# absorbs a chain) and uses P2C only as the latency tiebreak within it.
_SELECTION_COUNTS: dict[str, int] = {}


def _logs_capability_excluded(endpoint: str) -> bool:
    """Whether ``endpoint`` is permanently unable to serve broad getLogs.

    A capability refusal must not count toward failure streaks / bans /
    circuit-open (it is a fixed property of the endpoint for the method, not
    an abuse signal). Lazy import avoids an import cycle.
    """
    from lite_indexer.rpc.capabilities import is_logs_capability_excluded

    return is_logs_capability_excluded(endpoint)


def _logs_capability_reprobe_due(endpoint: str) -> bool:
    """Whether a stale capability exclusion may be re-probed.

    Keeps a locally synthesized ``logs_capable=False`` from permanently
    removing endpoint capacity: once the verdict ages past
    ``CAPABILITY_REPROBE_INTERVAL`` the endpoint re-enters the probe set.
    """
    from lite_indexer.rpc.capabilities import is_logs_capability_reprobe_due

    return is_logs_capability_reprobe_due(endpoint)


def selection_counts() -> dict[str, int]:
    """Copy of the per-endpoint selection counters (tests/telemetry)."""
    return dict(_SELECTION_COUNTS)


def reset_selection_state() -> None:
    """Clear the fairness counters (tests / shutdown)."""
    _SELECTION_COUNTS.clear()


# ── Ban / streak state lives in rpc.ban_state (extracted 2026-09-19) ──

# Recent-failure timestamps per endpoint (feeds backoff + scoring).
_endpoint_failures: dict[str, list[float]] = {}


def pool_fully_down(endpoints: list[str]) -> bool:
    """Whether every endpoint in a pool is HARD-banned or circuit-open.

    This is the pool-wide *outage* signal that triggers the desperation
    probe. Only endpoints that are guaranteed to fail count as down:

      - hard-banned (HTTP 403 / provider blacklist — probing deepens it)
      - circuit-open (repeated genuine failures)

    A pool whose endpoints are merely 429-streak SOFT-banned is
    rate-limited, NOT down. Counting those as down stopped the fetch path
    entirely and left one desperation probe per interval against an
    already-throttled endpoint — the base/celo catch-up deadlock
    (2026-09-19). Use :func:`pool_rate_limited` for that state and keep
    crawling instead.

    Args:
        endpoints: Pool candidate URLs (may be empty).

    Returns:
        True when the pool is non-empty and every endpoint is hard-banned
        or circuit-open.
    """
    if not endpoints:
        return False
    from lite_indexer.rpc.routing import is_circuit_open

    return all(is_endpoint_hard_banned(ep) or is_circuit_open(ep) for ep in endpoints)


def pool_rate_limited(endpoints: list[str]) -> bool:
    """Whether the pool is rate-limited (soft-banned throughout) but not down.

    True means every endpoint is currently banned or circuit-open, but at
    least one is a transient 429-streak soft-ban rather than a hard ban —
    so the fetcher must keep rotating the least-penalized endpoints at a
    bounded pace (crawl forward) instead of raising and making zero
    progress.

    Args:
        endpoints: Pool candidate URLs (may be empty).

    Returns:
        True when not :func:`pool_fully_down` but every endpoint is banned
        or circuit-open.
    """
    if not endpoints:
        return False
    if pool_fully_down(endpoints):
        return False
    from lite_indexer.rpc.routing import is_circuit_open

    return all(is_endpoint_banned(ep) or is_circuit_open(ep) for ep in endpoints)


def least_banned_endpoint(endpoints: list[str]) -> str:
    """Return the endpoint with the lowest combined pool penalty.

    Mirrors the :func:`get_best_endpoint` scoring (hard ban + circuit
    open + recent failures + list position) but deterministically picks
    the single least-penalized endpoint instead of rotating among the
    healthy subset. Used by the desperation probe to choose which
    endpoint to force-probe after a pool-wide outage.

    Args:
        endpoints: Pool candidate URLs.

    Returns:
        The least-banned endpoint URL.
    """
    if not endpoints:
        raise ValueError("least_banned_endpoint requires a non-empty endpoint list")

    from lite_indexer.rpc.routing import is_circuit_open

    now = time.time()
    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        penalty = len(failures) * 1000 + i
        if is_endpoint_banned(ep):
            penalty += 50000
        if is_circuit_open(ep):
            penalty += 20000
        scored.append((penalty, ep))
    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def probe_eligible_endpoints(endpoints: list[str]) -> list[str]:
    """Endpoints the desperation probe is ALLOWED to dial.

    The probe exists to detect recovery, so it must never pick an endpoint
    that is guaranteed to fail: an abuse-banned endpoint (HTTP 403 /
    provider blacklist, where probing deepens the ban) or a
    capability-excluded endpoint (structurally cannot serve broad
    eth_getLogs). Circuit-open endpoints stay eligible — that is exactly
    what ``force_probe`` overrides, and so do 429-streak SOFT-BANNED
    endpoints: their ban is transient by definition, and re-testing them is
    the whole point of the probe. Excluding soft-bans made an all-banned
    celo pool permanently unable to recover (2026-09-19).

    Args:
        endpoints: Pool candidate URLs.

    Stale capability exclusions stay eligible: ``logs_capable=False`` is a
    local verdict, so after ``CAPABILITY_REPROBE_INTERVAL`` the endpoint is
    re-tested rather than removed forever. Without this a chain whose whole
    log pool is capability-excluded deadlocks (no eligible probe) instead of
    self-healing within a bounded time.

    Returns:
        Subset that is neither abuse-banned nor freshly logs-incapable (may
        be empty; the caller must then wait out the ban instead of probing).
    """
    eligible: list[str] = []
    for ep in endpoints:
        if is_endpoint_hard_banned(ep):
            continue
        if _logs_capability_excluded(ep) and not _logs_capability_reprobe_due(ep):
            continue
        eligible.append(ep)
    return eligible


def force_probe_endpoint(endpoint: str) -> bool:
    """Bypass the circuit ban for a single request to ``endpoint``.

    Used by the desperation probe: after a pool-wide outage the
    least-banned endpoint is force-probed (once per
    :data:`CIRCUIT_DESPERATION_PROBE_INTERVAL`) even though its circuit
    is open. A successful probe closes the circuit (via
    :func:`record_endpoint_success`); a failure re-opens it.

    Args:
        endpoint: RPC endpoint URL to probe.

    Returns:
        True — the probe may proceed.
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    return CIRCUIT_BREAKER.force_probe(endpoint)


def desperation_probe_due(endpoints: list[str], now: float | None = None) -> bool:
    """Whether a pool-wide desperation probe may fire for ``endpoints``.

    Args:
        endpoints: Pool candidate URLs (pool key = chain name is not
            available here, so the first endpoint doubles as the pool
            key; callers that know the chain should prefer the breaker's
            own method with the chain name).
        now: Epoch time override (tests).

    Returns:
        True when the desperation interval has elapsed since the last
        probe for this pool.
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    if not endpoints:
        return False
    return CIRCUIT_BREAKER.desperation_probe_due(endpoints[0], now=now)


def mark_desperation_probe(endpoints: list[str], now: float | None = None) -> None:
    """Record that a desperation probe fired for ``endpoints``.

    Args:
        endpoints: Pool candidate URLs.
        now: Epoch time override (tests).
    """
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    if not endpoints:
        return
    CIRCUIT_BREAKER.mark_desperation_probe(endpoints[0], now=now)


# ── Best Endpoint Selection ──────────────────────────────────────────


def get_best_endpoint(endpoints: list[str]) -> str:
    """Return the healthiest endpoint with load-balanced rotation.

    Scoring:
      - Hard ban (403/429): +50000 penalty (skip — provider rate-limited)
      - Circuit open: +20000 penalty (skip — breaker tripped)
      - Stale (>3 blocks behind pool median height): +15000 penalty
        (skip — lagging endpoint; excluded until it catches up)
      - Failed verification: +10000 penalty (worst)
      - Each recent failure adds 1000 penalty
      - Original list position adds 1..N penalty (prefers earlier tiers)
      - Bottom 30% of healthy list is ignored for rotation diversity
      - Top healthy endpoints are picked via power-of-two-choices: two
        candidates are drawn and the lower ``ewma_ms * (1 + in_flight)``
        damped-latency score wins (see
        :func:`lite_indexer.rpc.routing.select_p2c`)

    Fallthrough: if all endpoints are in cooldown, clear and retry the first.
    """
    now = time.time()

    from lite_indexer.rpc.routing import (
        STALE_ENDPOINT_PENALTY,
        TIP_FAILURE_PENALTY,
        is_circuit_open,
        is_endpoint_tip_failed,
        stale_endpoints,
    )

    # Staleness gate: endpoints lagging >3 blocks behind the pool median
    # block height are excluded until they record a fresh height. With no
    # recorded heights the set is empty and the gate is inactive.
    stale = set(stale_endpoints(endpoints))

    scored: list[tuple[int, str]] = []
    for i, ep in enumerate(endpoints):
        failures = [t for t in _endpoint_failures.get(ep, []) if now - t < ENDPOINT_COOLDOWN]
        _endpoint_failures[ep] = failures

        penalty = len(failures) * 1000 + i

        # Hard ban: a 403/429 endpoint is provider-blacklisted for a
        # while — skip it entirely, not just deprioritized.
        if is_endpoint_banned(ep):
            penalty += 50000

        # Circuit breaker: an open circuit fully skips the endpoint.
        if is_circuit_open(ep):
            penalty += 20000

        # Staleness gate: a lagging endpoint is skipped until it catches
        # up (records a block height within STALENESS_MAX_BLOCKS of the
        # pool median).
        if ep in stale:
            penalty += STALE_ENDPOINT_PENALTY

        # Tip-failure demotion: an endpoint that failed repeated
        # eth_blockNumber observations records no height, so the stale gate
        # cannot see it lag. Exclude it from tip rotation until it serves a
        # fresh tip, or a dead tip-incapable endpoint keeps holding the tip.
        if is_endpoint_tip_failed(ep):
            penalty += TIP_FAILURE_PENALTY

        # Per-identity budget gate: an endpoint pre-emptively cooled at its
        # daily-budget threshold is skipped (before the provider 429s it).
        if not budget_available(ep):
            penalty += BUDGET_EXHAUSTED_PENALTY

        # Add verification penalty (unverified endpoints deprioritized)
        from lite_indexer.rpc.health import is_endpoint_healthy

        if not is_endpoint_healthy(ep):
            penalty += 10000

        scored.append((penalty, ep))

    scored.sort(key=lambda x: x[0])

    healthy = [ep for penalty, ep in scored if penalty < MAX_FAILURES * 1000]

    if not healthy:
        # Pool-wide outage. Do NOT clear failure state — that reset was
        # the stall: every cycle re-hammered endpoints[0] (which is still
        # banned/circuit-open) and deepened the bans. Return the
        # least-banned endpoint instead; the fetch path's desperation
        # probe gate decides whether it is dialed now or on the next
        # probe tick.
        logger.warning("all_endpoints_down", count=len(endpoints))
        return least_banned_endpoint(endpoints)

    # Fair-spread rotation over the healthy subset (P2C within the
    # least-used half): no endpoint absorbs a chain.
    return _select_fair(healthy)


def _select_fair(healthy: list[str]) -> str:
    """Fair-spread selection: least-used half of the healthy set, P2C tiebreak.

    The global load-distribution fix: instead of always handing the chain to
    the lowest-latency (fastest) endpoint until it 429s, candidates are the
    least-recently-chosen half of the healthy pool, and P2C picks within that
    window by damped latency. The chosen endpoint's counter increments, so it
    rotates out of the window and no endpoint can absorb a chain. P2C remains
    the tiebreak, so latency still matters among equally-used peers.
    """
    if len(healthy) == 1:
        chosen = healthy[0]
    else:
        ordered = sorted(enumerate(healthy), key=lambda item: (_SELECTION_COUNTS.get(item[1], 0), item[0]))
        window = max(2, (len(ordered) + 1) // 2)
        candidates = [ep for _i, ep in ordered[:window]]
        from lite_indexer.rpc.routing import select_p2c

        chosen = select_p2c(candidates)
    _SELECTION_COUNTS[chosen] = _SELECTION_COUNTS.get(chosen, 0) + 1
    return chosen


# ── Failure Tracking ────────────────────────────────────────────────


def record_endpoint_failure(endpoint: str) -> None:
    """Record a failure for this endpoint (feeds backoff + circuit breaker).

    Use for genuine failures (timeouts, connection errors, bad responses).
    For 429 rate limits use :func:`record_rate_limit` instead — they must
    NOT trip the circuit breaker. A capability-excluded endpoint (known to
    structurally refuse broad ``eth_getLogs``) is ignored entirely: the
    refusal is a permanent method property, not an abuse signal.
    """
    if _logs_capability_excluded(endpoint):
        logger.debug("endpoint_failure_ignored_logs_capability", endpoint=endpoint[:60])
        return
    _endpoint_failures.setdefault(endpoint, []).append(time.time())
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_failure(endpoint)


def record_rate_limit(endpoint: str) -> None:
    """Record a 429 rate limit for backoff WITHOUT tripping the circuit.

    429 means the provider is temporarily throttling us — the correct
    reaction is backoff: the recent-failure timestamp deprioritizes the
    endpoint in :func:`get_best_endpoint` scoring and drives the
    exponential sleep in :func:`apply_endpoint_backoff` /
    :func:`handle_rate_limit`. It must NOT trip the per-endpoint circuit
    breaker (which fully skips the endpoint for a cooldown) or hard-ban it
    (``ENDPOINT_BAN_*`` exponential window) — rate limits are transient and
    the endpoint usually recovers within seconds; banning it wastes healthy
    capacity that free-tier providers expect us to retry after backoff.

    Args:
        endpoint: RPC endpoint URL that returned 429.
    """
    _endpoint_failures.setdefault(endpoint, []).append(time.time())
    metrics_incr("rpc_rate_limits")

    # Escalating soft-ban on consecutive 429s (never resets mid-streak):
    # streak 1-2 → plain backoff; streak >=3 → the endpoint sits out for
    # minutes so parallel chunk workers stop aggregating into a permanent
    # 429 storm. Success anywhere clears the streak (record_endpoint_success).
    record_429_streak(endpoint)


def record_endpoint_success(endpoint: str) -> None:
    """Record a success for this endpoint (closes its circuit + unban)."""
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_success(endpoint)
    unban_endpoint(endpoint)
    clear_429_streak(endpoint)


async def apply_endpoint_backoff(url: str) -> None:
    """Apply exponential backoff if this endpoint has recent failures."""
    recent_failures = [t for t in _endpoint_failures.get(url, []) if time.time() - t < ENDPOINT_COOLDOWN]
    if recent_failures:
        backoff = 2 ** len(recent_failures)
        await asyncio.sleep(min(backoff, BACKOFF_CAP))


async def handle_rate_limit(url: str) -> None:
    """Handle a 429: backoff only — no circuit trip, no hard ban.

    429 means the provider explicitly throttled this endpoint. Unlike
    genuine failures, rate limits are transient: the endpoint usually
    recovers in seconds, so we record the failure for the pool penalty and
    sleep with exponential backoff, but we do NOT trip the circuit breaker
    and do NOT hard-ban (both would skip the endpoint entirely and waste
    healthy capacity after the throttle lifts).

    Args:
        url: RPC endpoint URL that returned 429.
    """
    record_rate_limit(url)
    wait = min(2 ** len(_endpoint_failures.get(url, [])), RATE_LIMIT_WAIT_CAP)
    logger.debug("rpc_rate_limited", url=url[:50], backoff=wait)
    await asyncio.sleep(wait)
