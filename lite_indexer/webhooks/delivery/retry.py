"""Retry / backoff logic for failed webhook deliveries (retry sub-module).

Split out of the former monolithic ``webhooks/delivery.py``. These methods
live on :class:`WebhookDelivery` via the ``RetryMixin``.
"""

from __future__ import annotations

import asyncio
import random
import time
from datetime import UTC, datetime

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.webhooks.models import DlqEntry, WebhookSubscription

from ._base import DeliveryMixinBase

logger = get_logger(__name__)


class RetryMixin(DeliveryMixinBase):
    """DLQ retry worker + Svix-schedule backoff for failed deliveries."""

    def _start_worker(self) -> None:
        """Start the DLQ retry worker once (idempotent)."""
        if self._worker_task is None and not self._worker_running:
            self._worker_running = True
            self._worker_task = asyncio.create_task(self._dlq_worker())

    async def _dlq_worker(self) -> None:
        """Background poller: drain due DLQ entries every poll interval."""
        try:
            stop_event = self._stop_event
            poll_interval = self._poll_interval
            while not stop_event.is_set():
                self._process_due_dlq()
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                except TimeoutError:
                    continue
        finally:
            self._worker_running = False

    def _process_due_dlq(self) -> None:
        """Schedule retries for every DLQ entry whose time has come."""
        due = self._dlq_due_rows(time.time())
        for row in due:
            asyncio.create_task(self._retry_entry(str(row["id"])))

    def _enqueue_dlq(
        self,
        sub: WebhookSubscription,
        subject: str,
        payload: bytes,
        event_type: str,
        chain: str,
        webhook_id: str,
        timestamp: int,
        *,
        attempts: int,
        status_code: int | None,
        error: str | None,
    ) -> None:
        """Park a failed message in the DLQ and schedule its next attempt.

        The DLQ row id IS the ``webhook-id`` idempotency key, so every
        retry re-sends the identical signature input (id, timestamp,
        raw body) that the subscriber saw on the initial attempt.
        """
        schedule = self._schedule
        delay = self._jittered(schedule[min(attempts, len(schedule) - 1)])
        entry = DlqEntry(
            id=webhook_id,
            subscription_id=sub.id,
            subject=subject,
            payload=payload,
            event_type=event_type,
            chain=chain,
            timestamp=timestamp,
            attempts=attempts,
            max_attempts=self.max_attempts,
            next_attempt_at=time.time() + delay,
            status="pending",
            last_error=error,
            last_status_code=status_code,
            created_at=datetime.now(UTC).isoformat(),
        )
        self._dlq_insert(entry)
        metrics_incr("webhook_dlq_enqueued")
        if not self._worker_running:
            asyncio.create_task(self._retry_later(entry.id, delay))

    async def _retry_later(self, entry_id: str, delay: float) -> None:
        """Single-shot timer path used when the worker is not running."""
        if delay > 0:
            await asyncio.sleep(delay)
        await self._retry_entry(entry_id)

    async def _retry_entry(self, entry_id: str) -> None:
        """Attempt one DLQ entry; reschedule, dead-letter, or resolve it."""
        entry = self._get_dlq_entry(entry_id)
        if entry is None or entry.status != "pending":
            return
        if not self._claim_dlq_entry(entry_id):
            return  # another task/worker is already handling it
        store = self._store
        sub = store.get_subscription(entry.subscription_id)
        if sub is None:
            self._mark_dlq_dead(entry_id, entry.attempts, "subscription removed")
            return
        if sub.health == "disabled":
            self._mark_dlq_dead(entry_id, entry.attempts, "endpoint disabled at 50 consecutive failures")
            return

        delivered, status_code, duration_ms, error = await self._attempt_post(
            sub, entry.id, entry.timestamp, entry.subject, entry.event_type, entry.chain, entry.payload
        )
        if delivered:
            store.record_success(sub.id, entry.subject, status_code or 0, duration_ms)
            self._delete_dlq_entry(entry_id)
            metrics_incr("webhook_deliveries")
            logger.debug("webhook_dlq_delivered", subscription_id=sub.id, subject=entry.subject)
            return

        attempts = entry.attempts + 1
        status = "failed" if status_code is not None else "error"
        store.record_attempt(sub.id, entry.subject, status, status_code, duration_ms)
        self._log_failure(sub, entry.subject, attempts=attempts, status_code=status_code, error=error)

        if attempts >= entry.max_attempts:
            # Retries exhausted — terminal dead-letter, kept as audit trail.
            store.record_attempt(sub.id, entry.subject, "deadletter", status_code, duration_ms)
            self._mark_dlq_dead(entry_id, attempts, error or f"http {status_code}")
            metrics_incr("webhook_dlq_dead")
            logger.warning("webhook_deadlettered", subscription_id=sub.id, subject=entry.subject, webhook_id=entry.id)
            return

        schedule = self._schedule
        delay = self._jittered(schedule[min(attempts, len(schedule) - 1)])
        self._reschedule_dlq(entry_id, attempts, time.time() + delay, status_code, error)
        if not self._worker_running:
            asyncio.create_task(self._retry_later(entry_id, delay))

    def _jittered(self, delay: float) -> float:
        """Apply ±20% jitter to a non-zero backoff (thundering-herd guard)."""
        if delay <= 0 or not self._jitter:
            return delay
        return delay * random.uniform(0.8, 1.2)
