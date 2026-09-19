"""Endpoint ban state — 403/429 bans, 429-streak soft-bans, persistence.

Owns the per-endpoint ban/streak state extracted from ``rpc.pool``:
exponential 403/429 ban windows, consecutive-429 streak soft-bans, the
hard-ban (provider blacklist) flag, and write-through persistence via
``rpc.cache_db``. Selection/scoring stays in ``rpc.pool``.
"""

from __future__ import annotations

import json
import time

from lite_indexer.config import ENDPOINT_BAN_BASE_SECONDS, ENDPOINT_BAN_CAP_SECONDS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Consecutive-429 tracking: occasional 429s are brief throttling;
# CONSECUTIVE 429s mean the provider is dying — escalate to a soft-ban.
_endpoint_429_streaks: dict[str, int] = {}
_RATE_LIMIT_STREAK_BAN_THRESHOLD = 3
_RATE_LIMIT_STREAK_BAN_BASE_S = 300  # 5 min first soft-ban, doubles per repeat
# Hard cap on the streak soft-ban: 3600s once turned a transient 429
# burst into a 1h self-sustaining deadlock past the provider's recovery.
_RATE_LIMIT_STREAK_BAN_CAP_S = 300

# 403/429 bans: url -> banned_until (epoch seconds). These providers
# blacklist the endpoint — hammering deepens the ban, so they get an
# exponentially growing "do not touch" window, not plain ENDPOINT_COOLDOWN.
_endpoint_bans: dict[str, float] = {}
# url -> ban count (feeds the exponential backoff curve).
_endpoint_ban_level: dict[str, int] = {}
# urls banned for EXPLICIT ABUSE (HTTP 403 / blacklist), not a transient
# 429 streak: the recovery probe must never dial these (probing deepens
# the blacklist), while soft-bans are exactly what the probe re-tests.
_endpoint_hard_bans: set[str] = set()

# Persist ban windows + 429 streaks so a restart does not re-hammer the pool.
_BAN_KIND = "ban"


def _persist_ban(endpoint: str) -> None:
    """Write-through the endpoint's ban/streak state (delete when clear)."""
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
            "hard": endpoint in _endpoint_hard_bans,
        }
    )
    cache_db.state_put(_BAN_KIND, endpoint, payload, ttl=max(0.0, banned_until - now) + 600.0)


def load_ban_state() -> int:
    """Load persisted ban windows + 429 streaks (daemon startup)."""  # noqa: D200
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
                if data.get("hard"):
                    _endpoint_hard_bans.add(endpoint)
                loaded += 1
        except (ValueError, TypeError):
            logger.debug("endpoint_bans_record_invalid")
            continue
    if loaded:
        logger.info("endpoint_bans_loaded", records=loaded)
    return loaded


def clear_ban_state() -> None:
    """Drop the in-memory ban/streak state (tests / shutdown)."""
    _endpoint_bans.clear()
    _endpoint_ban_level.clear()
    _endpoint_429_streaks.clear()
    _endpoint_hard_bans.clear()


def ban_endpoint(endpoint: str, reason: str = "") -> None:
    """Ban an endpoint that returned 403/429 for an exponential duration.

    Each consecutive ban doubles the window (capped). A capability-excluded
    endpoint is never banned: the refusal is a permanent method property.
    """
    from lite_indexer.rpc.pool import _logs_capability_excluded

    if _logs_capability_excluded(endpoint):
        logger.debug("endpoint_ban_skipped_logs_capability", endpoint=endpoint[:60], reason=reason)
        return
    level = _endpoint_ban_level.get(endpoint, 0)
    duration = min(ENDPOINT_BAN_BASE_SECONDS * (2**level), ENDPOINT_BAN_CAP_SECONDS)
    _endpoint_bans[endpoint] = time.time() + duration
    _endpoint_ban_level[endpoint] = level + 1
    if reason.startswith("http_403"):
        _endpoint_hard_bans.add(endpoint)
    metrics_incr("endpoints_banned")
    _persist_ban(endpoint)
    logger.warning("endpoint_banned", endpoint=endpoint[:60], reason=reason, level=level, duration_s=duration)


def unban_endpoint(endpoint: str) -> None:
    """Clear a hard ban after a successful call (ban level resets)."""
    if endpoint in _endpoint_bans:
        _endpoint_bans.pop(endpoint, None)
        _endpoint_ban_level.pop(endpoint, None)
        _endpoint_hard_bans.discard(endpoint)
        _persist_ban(endpoint)
        logger.debug("endpoint_unbanned", endpoint=endpoint[:60])


def is_endpoint_banned(endpoint: str) -> bool:
    """Whether an endpoint is currently under a 403/429 ban (active window)."""
    banned_until = _endpoint_bans.get(endpoint)
    if banned_until is None:
        return False
    if time.time() >= banned_until:
        # Ban window elapsed — clear it lazily.
        _endpoint_bans.pop(endpoint, None)
        return False
    return True


def is_endpoint_hard_banned(endpoint: str) -> bool:
    """Whether ``endpoint`` is banned for explicit abuse (HTTP 403), not 429.

    Hard-banned endpoints must never be dialed by the recovery probe (a
    blacklist deepens when probed); soft-bans stay probe-eligible.
    """
    if endpoint not in _endpoint_hard_bans:
        return False
    if not is_endpoint_banned(endpoint):
        # Ban window elapsed — the hard flag is stale.
        _endpoint_hard_bans.discard(endpoint)
        return False
    return True


def is_endpoint_soft_banned(endpoint: str) -> bool:
    """Whether ``endpoint`` is under a transient 429-streak soft-ban.

    Rate-limiting, not an outage (window capped at 300s) — must NOT be
    treated as pool-down (the base/celo deadlock, 2026-09-19).
    """
    return is_endpoint_banned(endpoint) and not is_endpoint_hard_banned(endpoint)


def record_429_streak(endpoint: str) -> float | None:
    """Record one more consecutive 429; soft-ban on streaks >= threshold.

    Streak >=3 sits the endpoint out (doubling per repeat, capped) so
    parallel chunk workers stop aggregating into a 429 storm.

    Returns: soft-ban window in seconds, or None when under the threshold.
    """
    streak = _endpoint_429_streaks.get(endpoint, 0) + 1
    _endpoint_429_streaks[endpoint] = streak
    _persist_ban(endpoint)
    if streak < _RATE_LIMIT_STREAK_BAN_THRESHOLD:
        return None
    ban_s = min(
        _RATE_LIMIT_STREAK_BAN_BASE_S * (2 ** (streak - _RATE_LIMIT_STREAK_BAN_THRESHOLD)),
        _RATE_LIMIT_STREAK_BAN_CAP_S,
    )
    # Apply the streak-computed window directly (not ban_endpoint's own
    # exponential) so the logged duration is the enforced duration.
    _endpoint_bans[endpoint] = time.time() + ban_s
    metrics_incr("endpoints_banned")
    logger.warning("rpc_rate_limit_streak_softban", endpoint=endpoint[:60], streak=streak, banned_seconds=ban_s)
    return ban_s


def clear_429_streak(endpoint: str) -> None:
    _endpoint_429_streaks.pop(endpoint, None)  # clear streak after a success
    _persist_ban(endpoint)
