"""
SQLite persistence for webhook subscriptions and the delivery log.

Schema (file default ``/var/data/chain-indexer/webhooks.db``)::

    subscriptions(id TEXT PK, url TEXT, subjects JSON, secret TEXT,
                  created_at TEXT, last_success TEXT, failure_count INTEGER)
    delivery_log(id TEXT PK, subscription_id TEXT, subject TEXT,
                 timestamp TEXT, status TEXT, status_code INTEGER,
                 duration_ms INTEGER)

Design notes:

* **Lazy connect.** The connection is opened on first use, so merely
  constructing a :class:`WebhookStore` (e.g. the process-wide delivery
  singleton) costs nothing and never creates files.
* **Thread-safe.** SQLite connections are single-threaded by default;
  the delivery service (NATS event loop) and the FastAPI router
  (uvicorn worker) may touch the store from different threads, so every
  operation is serialized under a lock.
* **WAL journal.** Readers never block the writer and vice-versa —
  the delivery log is append-mostly while the API serves list queries.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path

from lite_indexer.config import WEBHOOK_DB_PATH
from lite_indexer.webhooks.models import DeliveryAttempt, WebhookSubscription

_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    subjects JSON NOT NULL,
    secret TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_success TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS delivery_log (
    id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    status TEXT NOT NULL,
    status_code INTEGER,
    duration_ms INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_delivery_log_subscription
    ON delivery_log(subscription_id, timestamp);
"""


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


class WebhookStore:
    """SQLite-backed CRUD for webhook subscriptions + delivery attempts."""

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path = Path(db_path or WEBHOOK_DB_PATH)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None

    # ── Connection management ────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        """Open (or return) the shared connection, creating tables once."""
        if self._conn is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_SCHEMA)
            conn.commit()
            self._conn = conn
        return self._conn

    def close(self) -> None:
        """Close the underlying connection (idempotent)."""
        with self._lock:
            conn, self._conn = self._conn, None
            if conn is not None:
                conn.close()

    # ── Subscription CRUD ────────────────────────────────────────────

    def create_subscription(
        self,
        url: str,
        subjects: list[str],
        secret: str = "",
    ) -> WebhookSubscription:
        """Persist a new subscription and return the stored record."""
        sub = WebhookSubscription(
            id=_new_id(),
            url=url,
            subjects=list(subjects),
            secret=secret,
            created_at=_now_iso(),
            last_success=None,
            failure_count=0,
        )
        with self._lock:
            conn = self._connect()
            conn.execute(
                "INSERT INTO subscriptions (id, url, subjects, secret, created_at, last_success, failure_count)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    sub.id,
                    sub.url,
                    json.dumps(sub.subjects),
                    sub.secret,
                    sub.created_at,
                    sub.last_success,
                    sub.failure_count,
                ),
            )
            conn.commit()
        return sub

    def delete_subscription(self, subscription_id: str) -> bool:
        """Remove a subscription; returns False when it did not exist."""
        with self._lock:
            conn = self._connect()
            cursor = conn.execute("DELETE FROM subscriptions WHERE id = ?", (subscription_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_subscription(self, subscription_id: str) -> WebhookSubscription | None:
        """Return one subscription or None when unknown."""
        with self._lock:
            conn = self._connect()
            row = conn.execute("SELECT * FROM subscriptions WHERE id = ?", (subscription_id,)).fetchone()
        return self._row_to_subscription(row)

    def list_subscriptions(self) -> list[WebhookSubscription]:
        """Return all registered subscriptions, oldest first."""
        with self._lock:
            conn = self._connect()
            rows = conn.execute("SELECT * FROM subscriptions ORDER BY created_at ASC").fetchall()
        subscriptions: list[WebhookSubscription] = []
        for row in rows:
            sub = self._row_to_subscription(row)
            if sub is not None:
                subscriptions.append(sub)
        return subscriptions

    def matching_subscriptions(self, subject: str) -> list[WebhookSubscription]:
        """Return subscriptions whose subject patterns match ``subject``.

        A subscription matches when ANY of its patterns matches the
        NATS-style wildcard rules (``*`` = one token, ``>`` = rest).
        """
        return [sub for sub in self.list_subscriptions() if any(_subject_matches(p, subject) for p in sub.subjects)]

    # ── Delivery log ─────────────────────────────────────────────────

    def record_attempt(
        self,
        subscription_id: str,
        subject: str,
        status: str,
        status_code: int | None,
        duration_ms: int,
    ) -> DeliveryAttempt:
        """Record a delivery attempt and bump the failure counter."""
        attempt = DeliveryAttempt(
            id=_new_id(),
            subscription_id=subscription_id,
            subject=subject,
            timestamp=_now_iso(),
            status=status,
            status_code=status_code,
            duration_ms=duration_ms,
        )
        with self._lock:
            conn = self._connect()
            conn.execute(
                "INSERT INTO delivery_log (id, subscription_id, subject, timestamp, status, status_code, duration_ms)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt.id,
                    attempt.subscription_id,
                    attempt.subject,
                    attempt.timestamp,
                    attempt.status,
                    attempt.status_code,
                    attempt.duration_ms,
                ),
            )
            conn.execute(
                "UPDATE subscriptions SET failure_count = failure_count + 1 WHERE id = ?",
                (subscription_id,),
            )
            conn.commit()
        return attempt

    def record_success(
        self,
        subscription_id: str,
        subject: str,
        status_code: int,
        duration_ms: int,
    ) -> DeliveryAttempt:
        """Record a successful delivery and reset the failure counter."""
        attempt = DeliveryAttempt(
            id=_new_id(),
            subscription_id=subscription_id,
            subject=subject,
            timestamp=_now_iso(),
            status="success",
            status_code=status_code,
            duration_ms=duration_ms,
        )
        with self._lock:
            conn = self._connect()
            conn.execute(
                "INSERT INTO delivery_log (id, subscription_id, subject, timestamp, status, status_code, duration_ms)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    attempt.id,
                    attempt.subscription_id,
                    attempt.subject,
                    attempt.timestamp,
                    attempt.status,
                    attempt.status_code,
                    attempt.duration_ms,
                ),
            )
            conn.execute(
                "UPDATE subscriptions SET last_success = ?, failure_count = 0 WHERE id = ?",
                (_now_iso(), subscription_id),
            )
            conn.commit()
        return attempt

    def delivery_log(self, subscription_id: str, limit: int = 100) -> list[DeliveryAttempt]:
        """Return the most recent delivery attempts for a subscription."""
        with self._lock:
            conn = self._connect()
            rows = conn.execute(
                "SELECT * FROM delivery_log WHERE subscription_id = ? ORDER BY timestamp DESC LIMIT ?",
                (subscription_id, max(1, min(int(limit), 1000))),
            ).fetchall()
        return [self._row_to_attempt(row) for row in rows]

    # ── Row mapping ──────────────────────────────────────────────────

    @staticmethod
    def _row_to_subscription(row: sqlite3.Row | None) -> WebhookSubscription | None:
        if row is None:
            return None
        try:
            subjects = json.loads(row["subjects"] or "[]")
        except json.JSONDecodeError:
            subjects = []
        return WebhookSubscription(
            id=row["id"],
            url=row["url"],
            subjects=list(subjects) if isinstance(subjects, list) else [],
            secret=row["secret"] or "",
            created_at=row["created_at"] or "",
            last_success=row["last_success"],
            failure_count=int(row["failure_count"] or 0),
        )

    @staticmethod
    def _row_to_attempt(row: sqlite3.Row) -> DeliveryAttempt:
        return DeliveryAttempt(
            id=row["id"],
            subscription_id=row["subscription_id"],
            subject=row["subject"],
            timestamp=row["timestamp"] or "",
            status=row["status"] or "",
            status_code=row["status_code"],
            duration_ms=int(row["duration_ms"] or 0),
        )


def _subject_matches(pattern: str, subject: str) -> bool:
    """Match a NATS-style subject pattern (``*`` / ``>``) against a subject.

    ``*`` matches exactly one token, ``>`` matches the remaining tokens
    (must be the final pattern token). Unknown subjects never match.
    """
    pattern_tokens = pattern.split(".")
    subject_tokens = subject.split(".")
    for pt, st in zip(pattern_tokens, subject_tokens, strict=False):
        if pt == ">":
            return True
        if pt == "*":
            continue
        if pt != st:
            return False
    return len(pattern_tokens) == len(subject_tokens)


__all__ = ["WebhookStore"]
