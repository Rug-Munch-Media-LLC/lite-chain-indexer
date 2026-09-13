"""DLQ SQLite persistence for webhook delivery (queue sub-module).

Split out of the former monolithic ``webhooks/delivery.py``. These methods
live on :class:`WebhookDelivery` via the ``QueueMixin``.
"""

from __future__ import annotations

import sqlite3
import threading

from lite_indexer.webhooks.models import DlqEntry

from ._base import DeliveryMixinBase
from .helpers import _DLQ_POLL_BATCH, _DLQ_SCHEMA


class QueueMixin(DeliveryMixinBase):
    """DLQ SQLite access (same DB file, own connection)."""

    _dlq_conn: sqlite3.Connection | None
    _dlq_lock: threading.Lock

    def _dlq_connect(self) -> sqlite3.Connection:
        """Open (or return) the DLQ connection, creating the table once."""
        if self._dlq_conn is None:
            from lite_indexer.webhooks.storage import WebhookStore  # noqa: PLC0415

            store: WebhookStore = self._store
            store.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(store.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_DLQ_SCHEMA)
            conn.commit()
            self._dlq_conn = conn
        return self._dlq_conn

    def _dlq_insert(self, entry: DlqEntry) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "INSERT OR REPLACE INTO webhook_dlq (id, subscription_id, subject, payload, event_type, chain,"
                " timestamp, attempts, max_attempts, next_attempt_at, last_error, last_status_code, status, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entry.id,
                    entry.subscription_id,
                    entry.subject,
                    entry.payload,
                    entry.event_type,
                    entry.chain,
                    entry.timestamp,
                    entry.attempts,
                    entry.max_attempts,
                    entry.next_attempt_at,
                    entry.last_error,
                    entry.last_status_code,
                    entry.status,
                    entry.created_at,
                ),
            )
            conn.commit()

    def _get_dlq_entry(self, entry_id: str) -> DlqEntry | None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            row = conn.execute("SELECT * FROM webhook_dlq WHERE id = ?", (entry_id,)).fetchone()
        return self._row_to_dlq(row)

    def _claim_dlq_entry(self, entry_id: str) -> bool:
        """Mark pending → in_flight; False when another task owns it."""
        with self._dlq_lock:
            conn = self._dlq_connect()
            cursor = conn.execute(
                "UPDATE webhook_dlq SET status = 'in_flight' WHERE id = ? AND status = 'pending'",
                (entry_id,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def _reschedule_dlq(
        self,
        entry_id: str,
        attempts: int,
        next_attempt_at: float,
        status_code: int | None,
        error: str | None,
    ) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "UPDATE webhook_dlq SET attempts = ?, next_attempt_at = ?, status = 'pending',"
                " last_status_code = ?, last_error = ? WHERE id = ?",
                (attempts, next_attempt_at, status_code, error, entry_id),
            )
            conn.commit()

    def _mark_dlq_dead(self, entry_id: str, attempts: int, reason: str) -> None:
        """Persist the terminal dead-letter state (final attempt count kept for audit)."""
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "UPDATE webhook_dlq SET status = 'dead', attempts = ?, last_error = ? WHERE id = ?",
                (attempts, reason, entry_id),
            )
            conn.commit()

    def _delete_dlq_entry(self, entry_id: str) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute("DELETE FROM webhook_dlq WHERE id = ?", (entry_id,))
            conn.commit()

    def _dlq_due_rows(self, now: float) -> list[sqlite3.Row]:
        with self._dlq_lock:
            conn = self._dlq_connect()
            rows = conn.execute(
                "SELECT id FROM webhook_dlq WHERE status = 'pending' AND next_attempt_at <= ?"
                " ORDER BY next_attempt_at ASC LIMIT ?",
                (now, _DLQ_POLL_BATCH),
            ).fetchall()
        return list(rows)

    @staticmethod
    def _row_to_dlq(row: sqlite3.Row | None) -> DlqEntry | None:
        if row is None:
            return None
        return DlqEntry(
            id=str(row["id"]),
            subscription_id=str(row["subscription_id"]),
            subject=str(row["subject"]),
            payload=bytes(row["payload"] or b""),
            event_type=str(row["event_type"] or ""),
            chain=str(row["chain"] or ""),
            timestamp=int(row["timestamp"] or 0),
            attempts=int(row["attempts"] or 0),
            max_attempts=int(row["max_attempts"] or 8),
            next_attempt_at=float(row["next_attempt_at"] or 0.0),
            status=str(row["status"] or "pending"),
            last_error=row["last_error"],
            last_status_code=row["last_status_code"],
            created_at=str(row["created_at"] or ""),
        )
