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
import json
import time

from lite_indexer.config import (
    BACKOFF_CAP,
    ENDPOINT_BAN_BASE_SECONDS,
    ENDPOINT_BAN_CAP_SECONDS,
    ENDPOINT_COOLDOWN,
    MAX_FAILURES,
    RATE_LIMIT_WAIT_CAP,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
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


def selection_counts() -> dict[str, int]:
    """Copy of the per-endpoint selection counters (tests/telemetry)."""
    return dict(_SELECTION_COUNTS)


def reset_selection_state() -> None:
    """Clear the fairness counters (tests / shutdown)."""
    _SELECTION_COUNTS.clear()


# ── State ──────────────────────────────────────────────────────────────
_endpoint_failures: dict[str, list[float]] = {}
# Consecutive-429 tracking (WIN: never get banned from public RPCs).
# A provider that 429s occasionally is throttling us briefly; a provider
# that 429s CONSECUTIVELY is dying/dead — retrying every few seconds is
# how accounts get banned. Escalate to a soft-ban window on streaks.
_endpoint_429_streaks: dict[str, int] = {}
_RATE_LIMIT_STREAK_BAN_THRESHOLD = 3
_RATE_LIMIT_STREAK_BAN_BASE_S = 300  # 5 min first soft-ban, doubles per repeat
# Hard bans for 403/429 responses: url -> banned_until (epoch seconds).
# These providers explicitly rate-limit / blacklist the endpoint for a
# while — hammering them only deepens the ban, so they get an
# exponentially growing "do not touch" window instead of the plain
# ENDPOINT_COOLDOWN used for generic failures.
_endpoint_bans: dict[str, float] = {}
# url -> ban count (feeds the exponential backoff curve).
_endpoint_ban_level: dict[str, int] = {}

# D2 (2026-09-14): persist ban windows + 429 streaks so a restart does not
# reset them and immediately re-hammer the pool.
_BAN_KIND = "ban"


def _persist_ban(endpoint: str) -> None:
    """Write-through the endpoint's ban/streak state (or delete when clear)."""
    try:
        from lite_indexer.rpc import cache_db
    except Exception:  # noqa: BLE001
        return
    banned_until = _endpoint_bans.get(endpoint, 0.0)
    streak = int(_endpoint_429_streaks.get(endpoint, 0))
    now = time.time()
    active = banned_until > now
    if not active and streak == 0:
        cache_db.state_delete(_BAN_KIND, endpoint)
        return
    payload = json.dumps(
        {
            "until": float(banned_until) if active else 0.0,
            "level": int(_endpoint_ban_level.get(endpoint, 0)),
            "streak": streak,
        }
    )
    cache_db.state_put(_BAN_KIND, endpoint, payload, ttl=max(0.0, banned_until - now) + 600.0)


def load_ban_state() -> int:
    """Load persisted ban windows + 429 streaks (daemon startup)."""
    try:
        from lite_indexer.rpc import cache_db

        stored = cache_db.state_load(_BAN_KIND)
    except Exception:  # noqa: BLE001
        return 0
    now = time.time()
    loaded = 0
    for endpoint, text in stored.items():
        try:
            data = json.loads(text)
            until = float(data.get("until", 0.0) or 0.0)
            if until > now:
                _endpoint_bans[endpoint] = until
                _endpoint_ban_level[endpoint] = int(data.get("level", 0) or 0)
                _endpoint_429_streaks[endpoint] = int(data.get("streak", 0) or 0)
                loaded += 1
        except (ValueError, TypeError):
            continue
    if loaded:
        logger.info("endpoint_bans_loaded", records=loaded)
    return loaded


def clear_ban_state() -> None:
    """Drop the in-memory ban/streak state (tests / shutdown)."""
    _endpoint_bans.clear()
    _endpoint_ban_level.clear()
    _endpoint_429_streaks.clear()


# ── Hard Ban (403/429) ───────────────────────────────────────────────


def ban_endpoint(endpoint: str, reason: str = "") -> None:
    """Ban an endpoint that returned 403/429 for an exponential duration.

    Each consecutive ban doubles the window: base * 2^level, capped at
    ``ENDPOINT_BAN_CAP_SECONDS``. A single success calls
    :func:`unban_endpoint` to clear the ban.

    A capability-excluded endpoint (known to structurally refuse broad
    ``eth_getLogs``) is never banned: the refusal is a permanent property of
    the endpoint for that method, not an abuse signal.

    Args:
        endpoint: RPC endpoint URL.
        reason: Human-readable reason (e.g. ``http_429``) for logging.
    """
    if _logs_capability_excluded(endpoint):
        logger.debug("endpoint_ban_skipped_logs_capability", endpoint=endpoint[:60], reason=reason)
        return
    level = _endpoint_ban_level.get(endpoint, 0)
    duration = min(ENDPOINT_BAN_BASE_SECONDS * (2**level), ENDPOINT_BAN_CAP_SECONDS)
    _endpoint_bans[endpoint] = time.time() + duration
    _endpoint_ban_level[endpoint] = level + 1
    metrics_incr("endpoints_banned")
    _persist_ban(endpoint)
    logger.warning(
        "endpoint_banned",
        endpoint=endpoint[:60],
        reason=reason,
        level=level,
        duration_s=duration,
    )


def unban_endpoint(endpoint: str) -> None:
    """Clear a hard ban after a successful call (ban level resets)."""
    if endpoint in _endpoint_bans:
        _endpoint_bans.pop(endpoint, None)
        _endpoint_ban_level.pop(endpoint, None)
        _persist_ban(endpoint)
        logger.debug("endpoint_unbanned", endpoint=endpoint[:60])


def is_endpoint_banned(endpoint: str) -> bool:
    """Whether an endpoint is currently under a 403/429 hard ban.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        True while the ban window is active (the endpoint should be
        skipped entirely).
    """
    banned_until = _endpoint_bans.get(endpoint)
    if banned_until is None:
        return False
    if time.time() >= banned_until:
        # Ban window elapsed — clear it lazily.
        _endpoint_bans.pop(endpoint, None)
        return False
    return True


def pool_fully_down(endpoints: list[str]) -> bool:
    """Whether every endpoint in a pool is banned or circuit-open.

    This is the pool-wide outage signal that triggers the desperation
    probe: all endpoints are currently unusable, so the fetch path must
    not hammer the whole pool every cycle. Instead it force-probes the
    least-banned endpoint at most once per desperation interval.

    Args:
        endpoints: Pool candidate URLs (may be empty).

    Returns:
        True when the pool is non-empty and every endpoint is banned or
        circuit-open.
    """
    if not endpoints:
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
    # streak 1-2 → plain backoff (existing behavior); streak >=3 → the
    # endpoint sits out for minutes (doubling per repeat) so parallel
    # chunk workers stop aggregating into a permanent 429 storm. Success
    # anywhere clears the streak (see record_endpoint_success).
    streak = _endpoint_429_streaks.get(endpoint, 0) + 1
    _endpoint_429_streaks[endpoint] = streak
    _persist_ban(endpoint)
    if streak >= _RATE_LIMIT_STREAK_BAN_THRESHOLD:
        ban_s = min(
            _RATE_LIMIT_STREAK_BAN_BASE_S * (2 ** (streak - _RATE_LIMIT_STREAK_BAN_THRESHOLD)),
            3600,
        )
        # Apply the streak-computed window directly (not ban_endpoint's own
        # exponential) so the logged duration is the enforced duration.
        _endpoint_bans[endpoint] = time.time() + ban_s
        metrics_incr("endpoints_banned")
        logger.warning(
            "rpc_rate_limit_streak_softban",
            endpoint=endpoint[:60],
            streak=streak,
            banned_seconds=ban_s,
        )


def record_endpoint_success(endpoint: str) -> None:
    """Record a success for this endpoint (closes its circuit + unban)."""
    from lite_indexer.rpc.routing import CIRCUIT_BREAKER

    CIRCUIT_BREAKER.record_success(endpoint)
    unban_endpoint(endpoint)
    _endpoint_429_streaks.pop(endpoint, None)
    _persist_ban(endpoint)


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
