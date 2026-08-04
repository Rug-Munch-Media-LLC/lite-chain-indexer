"""
Webhook delivery — subscribes to NATS subjects and POSTs events to
registered webhooks, turning the indexer's real-time stream into a push
platform. Goldsky charges for webhooks; this ships them free.

Flow::

    NATS subject ──▶ WebhookDelivery._on_message ──▶ deliver(subject, payload)
        │                                              │
        │                                              ▼
        │                              POST matching webhook(s)
        │                              • HMAC-SHA256 signature (X-LCI-Signature)
        │                              • X-LCI-Event / X-LCI-Chain headers
        │                              • retries with exponential backoff
        │                                (1s, 4s, 16s by default)
        │                              • bounded total budget (30s default)
        ▼
    delivery_log (SQLite)

Design rules (mirroring the NATS publisher):

1. **Best-effort, never raises.** ``start()`` and ``deliver()`` swallow
   and log every failure — a dead NATS server or a flaky webhook must
   never block indexing.
2. **Process-wide singleton.** The daemon runs one ``LiteIndexer`` per
   chain; the delivery service must subscribe to ``chain.>`` / ``alerts.>``
   exactly once or every event would be delivered N times. Use
   :meth:`WebhookDelivery.shared`.
3. **Bounded retries.** At most ``WEBHOOK_MAX_RETRIES`` retries with
   exponential backoff, and a hard total budget
   (``WEBHOOK_TOTAL_TIMEOUT_SECONDS``, default 30s) per delivery.
4. **Signed.** Every POST carries ``X-LCI-Signature: sha256=<hmac>``
   computed over the raw payload bytes with the subscription secret so
   consumers can verify authenticity.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
from typing import Any

import httpx

from lite_indexer.config import (
    NATS_URL,
    WEBHOOK_BACKOFF_SECONDS,
    WEBHOOK_ENABLED,
    WEBHOOK_MAX_RETRIES,
    WEBHOOK_TIMEOUT_SECONDS,
    WEBHOOK_TOTAL_TIMEOUT_SECONDS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.webhooks.models import DeliveryAttempt, WebhookSubscription
from lite_indexer.webhooks.storage import WebhookStore

logger = get_logger(__name__)

# NATS subjects captured by the delivery service (same wildcard set the
# JetStream stream auto-provisions and the publisher writes to).
_DELIVERY_SUBJECTS: tuple[str, ...] = ("chain.>", "alerts.>")

# HTTP statuses treated as success. Anything >= 400 is retried.
_SUCCESS_STATUS_LIMIT = 400


def _parse_subject(subject: str) -> tuple[str, str]:
    """Extract (event_type, chain) from a NATS subject.

    ``chain.ethereum.dex.swap`` → ``("swap", "ethereum")``
    ``alerts.whale.solana``    → ``("whale", "solana")``
    """
    parts = subject.split(".")
    if parts and parts[0] == "alerts" and len(parts) >= 3:
        return parts[1], parts[2]
    if parts and parts[0] == "chain" and len(parts) >= 3:
        return parts[-1], parts[1]
    return subject, ""


def _sign(secret: str, payload: bytes) -> str:
    """HMAC-SHA256 signature over the raw payload (hex digest)."""
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


class WebhookDelivery:
    """Subscribes to NATS subjects and POSTs events to registered webhooks.

    Args:
        url: NATS server URL. Defaults to ``NATS_URL``.
        enabled: Master switch. Defaults to the ``WEBHOOK_ENABLED`` config.
        store: Persistence backend. Defaults to a fresh ``WebhookStore``
            at ``WEBHOOK_DB_PATH``.
        max_retries: Retry attempts after the initial POST. Defaults to
            ``WEBHOOK_MAX_RETRIES``.
        timeout_seconds: Per-request HTTP timeout. Defaults to
            ``WEBHOOK_TIMEOUT_SECONDS``.
        total_timeout_seconds: Total wall-clock budget for one delivery
            including retries. Defaults to ``WEBHOOK_TOTAL_TIMEOUT_SECONDS``.
        backoffs: Backoff schedule (seconds) between retries.
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
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Any = time.monotonic,
    ) -> None:
        self._url = url or NATS_URL
        self.enabled = WEBHOOK_ENABLED if enabled is None else enabled
        self._store = store or WebhookStore()
        self.max_retries = max(0, WEBHOOK_MAX_RETRIES if max_retries is None else max_retries)
        self.timeout_seconds = WEBHOOK_TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
        self.total_timeout_seconds = (
            WEBHOOK_TOTAL_TIMEOUT_SECONDS if total_timeout_seconds is None else total_timeout_seconds
        )
        self._backoffs = backoffs or WEBHOOK_BACKOFF_SECONDS
        self._transport = transport
        self._clock = clock

        self._nc: Any | None = None
        self._subscriptions: list[Any] = []
        self._started = False
        self._http_client: httpx.AsyncClient | None = None
        self._close_lock = asyncio.Lock()

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
        """Connect to NATS and subscribe to the event subjects.

        Idempotent and best-effort: a missing NATS server logs a warning
        and returns False — the delivery service simply has nothing to
        consume until NATS comes back (the publisher reconnects lazily,
        and this method is retried on the next sync cycle).
        """
        if not self.enabled or self._started:
            return self._started
        try:
            import nats  # local import keeps the core package light

            self._nc = await nats.connect(
                self._url,
                timeout=2.0,
                max_reconnect_attempts=0,
            )
        except Exception as exc:  # noqa: BLE001 — start is best-effort
            self._nc = None
            logger.warning("webhook_delivery_start_failed", url=self._url, error=str(exc)[:120])
            return False

        for subject in _DELIVERY_SUBJECTS:
            try:
                sub = await self._nc.subscribe(subject, cb=self._on_message)
                self._subscriptions.append(sub)
            except Exception as exc:  # noqa: BLE001 — per-subject best-effort
                logger.warning("webhook_subscribe_failed", subject=subject, error=str(exc)[:120])

        self._started = True
        logger.info("webhook_delivery_started", url=self._url, subjects=list(_DELIVERY_SUBJECTS))
        return True

    async def stop(self) -> None:
        """Close the NATS subscription and the HTTP client (idempotent)."""
        async with self._close_lock:
            self._started = False
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

    # ── NATS message handler ─────────────────────────────────────────

    async def _on_message(self, msg: Any) -> None:
        """NATS subscription callback — fan the event out to webhooks."""
        try:
            await self.deliver(msg.subject, msg.data)
        except Exception as exc:  # noqa: BLE001 — delivery must never raise
            logger.warning("webhook_on_message_failed", subject=str(getattr(msg, "subject", "")), error=str(exc)[:120])

    # ── Delivery ─────────────────────────────────────────────────────

    async def deliver(self, subject: str, payload: bytes) -> list[bool]:
        """POST ``payload`` to every subscription matching ``subject``.

        Called by the NATS message handler. For each matching webhook:

        * headers ``X-LCI-Event`` / ``X-LCI-Chain`` / ``X-LCI-Signature``
          (HMAC-SHA256 over the raw payload),
        * up to ``max_retries`` retries with exponential backoff
          (1s, 4s, 16s by default),
        * a hard total budget of ``total_timeout_seconds`` (default 30s).

        Returns a list of per-webhook success flags. Never raises —
        every failure is logged and recorded in the delivery log.
        """
        matches = self._store.matching_subscriptions(subject)
        if not matches:
            return []

        results: list[bool] = []
        for sub in matches:
            results.append(await self._deliver_one(sub, subject, payload))
        return results

    async def _deliver_one(self, sub: WebhookSubscription, subject: str, payload: bytes) -> bool:
        event_type, chain = _parse_subject(subject)
        headers = {
            "X-LCI-Event": event_type,
            "X-LCI-Chain": chain,
            "X-LCI-Signature": f"sha256={_sign(sub.secret, payload)}",
            "Content-Type": "application/json",
        }

        deadline = self._clock() + self.total_timeout_seconds
        last_status: int | None = None
        last_error: str | None = None

        for attempt in range(1, self.max_retries + 2):  # initial + retries
            now = self._clock()
            if now >= deadline:
                await self._record_dropped(sub, subject)
                break
            remaining = deadline - now
            timeout = min(self.timeout_seconds, remaining)

            start = self._clock()
            try:
                resp = await self._http().post(sub.url, content=payload, headers=headers, timeout=timeout)
                duration_ms = int((self._clock() - start) * 1000)
                if resp.status_code < _SUCCESS_STATUS_LIMIT:
                    self._store.record_success(sub.id, subject, resp.status_code, duration_ms)
                    metrics_incr("webhook_deliveries")
                    logger.debug(
                        "webhook_delivered",
                        subscription_id=sub.id,
                        subject=subject,
                        status_code=resp.status_code,
                    )
                    return True
                last_status = resp.status_code
                self._store.record_attempt(sub.id, subject, "failed", resp.status_code, duration_ms)
                logger.debug(
                    "webhook_http_status",
                    subscription_id=sub.id,
                    subject=subject,
                    status_code=resp.status_code,
                    attempt=attempt,
                )
            except Exception as exc:  # noqa: BLE001 — transport errors are retried
                last_error = str(exc)[:120]
                duration_ms = int((self._clock() - start) * 1000)
                self._store.record_attempt(sub.id, subject, "error", None, duration_ms)
                logger.debug(
                    "webhook_attempt_error",
                    subscription_id=sub.id,
                    subject=subject,
                    attempt=attempt,
                    error=last_error,
                )

            # Backoff before the next attempt (skip when it would blow
            # the total budget — the deadline check above handles it).
            if attempt <= self.max_retries:
                backoff = self._backoffs[min(attempt - 1, len(self._backoffs) - 1)] if self._backoffs else 0.0
                if self._clock() + backoff < deadline:
                    await asyncio.sleep(backoff)

        # All attempts exhausted — log once, do NOT retry further.
        metrics_incr("webhook_delivery_failures")
        logger.warning(
            "webhook_delivery_failed",
            subscription_id=sub.id,
            url=sub.url,
            subject=subject,
            attempts=self.max_retries + 1,
            last_status=last_status,
            error=last_error,
        )
        return False

    async def _record_dropped(self, sub: WebhookSubscription, subject: str) -> None:
        """Record a delivery abandoned when the total budget expired."""
        self._store.record_attempt(sub.id, subject, "dropped", None, 0)
        logger.debug(
            "webhook_budget_exhausted",
            subscription_id=sub.id,
            subject=subject,
            total_timeout_s=self.total_timeout_seconds,
        )

    # ── HTTP client ──────────────────────────────────────────────────

    def _http(self) -> httpx.AsyncClient:
        """Lazily-created shared httpx client (closed by :meth:`stop`)."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds),
                transport=self._transport,
            )
        return self._http_client


__all__ = ["WebhookDelivery"]
