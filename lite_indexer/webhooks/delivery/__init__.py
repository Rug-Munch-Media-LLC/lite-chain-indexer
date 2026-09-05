"""Webhook delivery package — split from the former 864-line
``webhooks/delivery.py`` into focused sub-modules:

* :mod:`.sender` — NATS fan-out + initial webhook POST delivery
* :mod:`.retry` — DLQ retry worker + Svix-schedule backoff
* :mod:`.queue` — DLQ SQLite persistence
* :mod:`.helpers` — subject parsing, signing, subscriber-side verification

Public API is unchanged: ``WebhookDelivery``, ``verify_signature``,
``REPLAY_WINDOW_SECONDS`` (plus the private helpers tests rely on).
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from typing import Any

import httpx

from lite_indexer.config import (
    NATS_URL,
    WEBHOOK_ENABLED,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_TOTAL_TIMEOUT_SECONDS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.webhooks.models import DeliveryAttempt, WebhookSubscription
from lite_indexer.webhooks.storage import WebhookStore

from .helpers import (
    _DELIVERY_SUBJECTS,
    _DLQ_POLL_INTERVAL,
    _SVIX_RETRY_DELAYS,
    REPLAY_WINDOW_SECONDS,
    _parse_subject,
    _sign,
    verify_signature,
)
from .queue import QueueMixin
from .retry import RetryMixin
from .sender import SenderMixin

logger = get_logger(__name__)


class WebhookDelivery(SenderMixin, RetryMixin, QueueMixin):
    """Subscribes to NATS subjects and POSTs events to registered webhooks.

    Args:
        url: NATS server URL. Defaults to ``NATS_URL``.
        enabled: Master switch. Defaults to the ``WEBHOOK_ENABLED`` config.
        store: Persistence backend. Defaults to a fresh ``WebhookStore``
            at ``WEBHOOK_DB_PATH``.
        max_retries: Retry attempts after the initial POST. Defaults to
            the Svix 8-attempt schedule (7 retries after the initial).
        timeout_seconds: Per-request HTTP timeout. Defaults to
            ``WEBHOOK_TIMEOUT_SECONDS``.
        total_timeout_seconds: Wall-clock budget for the initial attempt
            before the message is parked in the DLQ. Defaults to
            ``WEBHOOK_TOTAL_TIMEOUT_SECONDS``.
        backoffs: Backoff schedule (seconds) between attempts. Defaults
            to the Svix schedule (0s/5s/5m/30m/2h/5h/10h/10h). An
            explicit schedule disables jitter unless ``jitter=True``.
        jitter: Apply ±20% jitter to non-zero backoffs. ``None`` means
            jitter the default Svix schedule only.
        poll_interval: DLQ worker poll cadence (seconds).
        transport: Injectable httpx transport (tests use a mock).
        clock: Injectable monotonic clock (tests).
    """

    # Process-wide singleton so the per-chain daemon tasks share ONE
    # NATS subscription (otherwise events would be delivered N times).
    _shared: WebhookDelivery | None = None

    def __init__(
        self,
        url: str | None = None,
        enabled: bool | None = None,
        store: WebhookStore | None = None,
        max_retries: int | None = None,
        timeout_seconds: float | None = None,
        total_timeout_seconds: float | None = None,
        backoffs: tuple[float, ...] | None = None,
        jitter: bool | None = None,
        poll_interval: float = _DLQ_POLL_INTERVAL,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        self._url = url or NATS_URL
        self.enabled = WEBHOOK_ENABLED if enabled is None else enabled
        self._store = store or WebhookStore()
        self._schedule = backoffs or _SVIX_RETRY_DELAYS
        if backoffs is None:
            self._jitter = True if jitter is None else jitter
        else:
            self._jitter = False if jitter is None else jitter
        self.max_retries = max(0, (len(self._schedule) - 1) if max_retries is None else max_retries)
        self.max_attempts = self.max_retries + 1
        self.timeout_seconds = WEBHOOK_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
        self.total_timeout_seconds = (
            WEBHOOK_TOTAL_TIMEOUT_SECONDS if total_timeout_seconds is None else total_timeout_seconds
        )
        self._poll_interval = max(0.1, poll_interval)
        self._transport = transport
        self._clock = clock

        self._nc: Any | None = None
        self._subscriptions: list[Any] = []
        self._started = False
        self._http_client: httpx.AsyncClient | None = None
        self._close_lock = asyncio.Lock()

        # DLQ retry machinery (persisted in the same SQLite file).
        self._dlq_conn: sqlite3.Connection | None = None
        self._dlq_lock = threading.Lock()
        self._stop_event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._worker_running = False

    # ── Shared instance ──────────────────────────────────────────────

    @classmethod
    def shared(cls) -> WebhookDelivery:
        """Return the process-wide delivery instance (created lazily).

        One instance per process is what guarantees a single NATS
        subscription even when the daemon runs N ``LiteIndexer`` chain
        tasks. Tests inject a private instance instead.
        """
        if cls._shared is None:
            cls._shared = cls()
        return cls._shared

    # ── Lifecycle ────────────────────────────────────────────────────

    @property
    def started(self) -> bool:
        """True when the NATS subscription is active."""
        return self._started

    async def start(self) -> bool:
        """Connect to NATS, subscribe to the event subjects, start the DLQ worker.

        Idempotent and best-effort: a missing NATS server logs a warning
        and returns False — the delivery service simply has nothing to
        consume until NATS comes back (the publisher reconnects lazily,
        and this method is retried on the next sync cycle). The DLQ
        retry worker starts regardless so messages parked before the
        restart are still drained.
        """
        if not self.enabled or self._started:
            return self._started
        try:
            import nats  # local import keeps the core package light

            self._nc = await nats.connect(
                self._url,
                connect_timeout=2,
                max_reconnect_attempts=0,
            )
        except Exception as exc:  # noqa: BLE001 — start is best-effort
            self._nc = None
            logger.warning("webhook_delivery_start_failed", url=self._url, error=str(exc)[:120])
            # Pending DLQ retries must still drain even without NATS.
            self._start_worker()
            return False

        for subject in _DELIVERY_SUBJECTS:
            try:
                sub = await self._nc.subscribe(subject, cb=self._on_message)
                self._subscriptions.append(sub)
            except Exception as exc:  # noqa: BLE001 — per-subject best-effort
                logger.warning("webhook_subscribe_failed", subject=subject, error=str(exc)[:120])

        self._started = True
        self._start_worker()
        logger.info("webhook_delivery_started", url=self._url, subjects=list(_DELIVERY_SUBJECTS))
        return True

    async def stop(self) -> None:
        """Stop the DLQ worker, close NATS + HTTP + DLQ connections (idempotent)."""
        async with self._close_lock:
            self._started = False
            self._stop_event.set()
            task, self._worker_task = self._worker_task, None
            self._worker_running = False
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            nc, self._nc = self._nc, None
            self._subscriptions = []
            if nc is not None:
                try:
                    await nc.close()
                except Exception as exc:  # noqa: BLE001 — close is best-effort
                    logger.debug("webhook_nats_close_failed", error=str(exc)[:80])
            client, self._http_client = self._http_client, None
            if client is not None:
                try:
                    await client.aclose()
                except Exception as exc:  # noqa: BLE001 — close is best-effort
                    logger.debug("webhook_http_close_failed", error=str(exc)[:80])
            conn, self._dlq_conn = self._dlq_conn, None
            if conn is not None:
                try:
                    conn.close()
                except Exception as exc:  # noqa: BLE001 — close is best-effort
                    logger.debug("webhook_dlq_close_failed", error=str(exc)[:80])

    # ── Subscription management ──────────────────────────────────────

    async def subscribe(
        self,
        webhook_url: str,
        subjects: list[str],
        secret: str | None = None,
    ) -> str:
        """Register a webhook subscription.

        Args:
            webhook_url: HTTPS endpoint to POST events to.
            subjects: NATS subject patterns this webhook wants.
            secret: HMAC-SHA256 signing secret (optional).

        Returns:
            The new ``subscription_id``.
        """
        sub = self._store.create_subscription(webhook_url, list(subjects), secret or "")
        metrics_incr("webhook_registrations")
        logger.info("webhook_subscribed", subscription_id=sub.id, url=webhook_url, subjects=sub.subjects)
        return sub.id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unregister a webhook subscription. Returns False when unknown."""
        removed = self._store.delete_subscription(subscription_id)
        if removed:
            logger.info("webhook_unsubscribed", subscription_id=subscription_id)
        return removed

    def list_subscriptions(self) -> list[WebhookSubscription]:
        """Return all registered subscriptions (oldest first)."""
        return self._store.list_subscriptions()

    def delivery_log(self, subscription_id: str, limit: int = 100) -> list[DeliveryAttempt]:
        """Return recent delivery attempts for a subscription."""
        return self._store.delivery_log(subscription_id, limit)


__all__ = [
    "REPLAY_WINDOW_SECONDS",
    "WebhookDelivery",
    "verify_signature",
    # private helpers re-exported for backward compatibility (tests import them)
    "_parse_subject",
    "_sign",
]
