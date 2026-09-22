"""BigQuery account registry with quota-aware rotation.

Accounts come from ``BIGQUERY_ACCOUNTS`` (JSON list of
``{"project": ..., "creds": "/path/key.json"}``) or the legacy
``BIGQUERY_PROJECT_ID``/``GOOGLE_APPLICATION_CREDENTIALS`` pair (+``ALT_``).
An account that rejects with the free-quota error is marked exhausted
until the next calendar month (the free tier resets monthly) and skipped;
healthy accounts rotate least-recently-used. Google is a *fallback* bulk
source — quota exhaustion must never be silent.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

DEFAULT_STATE = "/var/data/chain-indexer/state/bigquery_accounts.json"


@dataclass(frozen=True)
class BqAccount:
    """One query account: project id + service-account key path."""

    project: str
    creds: str | None = None


def load_accounts() -> list[BqAccount]:
    """Accounts from ``BIGQUERY_ACCOUNTS`` JSON, or the legacy env pair(s)."""
    raw = os.environ.get("BIGQUERY_ACCOUNTS", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            return [BqAccount(str(e["project"]), e.get("creds")) for e in parsed if e.get("project")]
        except (ValueError, KeyError, TypeError) as exc:
            logger.warning("bigquery_accounts_bad_json", error=str(exc)[:120])
    accounts = [BqAccount(
        os.environ.get("BIGQUERY_PROJECT_ID") or "gen-lang-client-0708984505",
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    )]
    alt = os.environ.get("BIGQUERY_ALT_PROJECT_ID")
    if alt:
        accounts.append(BqAccount(alt, os.environ.get("BIGQUERY_ALT_CREDENTIALS")))
    return accounts


def _state_path() -> Path:
    return Path(os.environ.get("BIGQUERY_STATE_FILE") or DEFAULT_STATE)


def _load_state() -> dict:
    try:
        return json.loads(_state_path().read_text())
    except (OSError, ValueError) as exc:
        logger.debug("bigquery_state_unreadable", error=str(exc)[:100])
        return {}


def _save_state(state: dict) -> None:
    path = _state_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=1))
    except OSError as exc:
        logger.warning("bigquery_state_write_failed", error=str(exc)[:100])


def _next_month_reset(now: float | None = None) -> str:
    """ISO ts when the monthly free tier resets (first of next month, UTC)."""
    ts = datetime.fromtimestamp(now or time.time(), tz=UTC)
    year, month = (ts.year + 1, 1) if ts.month == 12 else (ts.year, ts.month + 1)
    return datetime(year, month, 1, tzinfo=UTC).isoformat()


def mark_exhausted(project: str, now: float | None = None) -> None:
    """Remember a quota rejection until the next monthly reset."""
    state = _load_state()
    entry = state.setdefault(project, {})
    entry["exhausted_until"] = _next_month_reset(now)
    _save_state(state)
    logger.warning("bigquery_account_exhausted", project=project, until=entry["exhausted_until"][:10])


def mark_used(project: str, now: float | None = None) -> None:
    """Record a successful use (drives least-recently-used rotation)."""
    state = _load_state()
    state.setdefault(project, {})["last_used"] = now or time.time()
    _save_state(state)


def ordered_accounts(now: float | None = None) -> list[BqAccount]:
    """Accounts to try, in order: healthy LRU first, exhausted last.

    Concrete quota rejections also mark exhausted-explicit for the current
    month, so the next call re-orders before touching a burnt account.
    """
    ts = now or time.time()
    state = _load_state()
    healthy: list[tuple[float, int, BqAccount]] = []
    burnt: list[BqAccount] = []
    for idx, account in enumerate(load_accounts()):
        entry = state.get(account.project) or {}
        until = entry.get("exhausted_until")
        if until and until > datetime.fromtimestamp(ts, tz=UTC).isoformat():
            burnt.append(account)
            continue
        healthy.append((float(entry.get("last_used") or 0), idx, account))
    healthy.sort(key=lambda item: (item[0], item[1]))
    return [a for _t, _i, a in healthy] + burnt
