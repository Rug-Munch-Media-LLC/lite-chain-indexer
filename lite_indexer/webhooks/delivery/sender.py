"""Webhook sending / delivery functions (sender sub-module).

Split out of the former monolithic ``webhooks/delivery.py``. These methods
live on :class:`WebhookDelivery` via the ``SenderMixin``.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.webhooks.models import (
    HEALTH_BREAK,
    HEALTH_DISABLE,
    HEALTH_WARN,
    DeliveryAttempt,
    WebhookSubscription,
)

from ._base import DeliveryMixinBase
from .helpers import (
    _SUCCESS_STATUS_LIMIT,
    _build_signature_header,
    _parse_subject,
)

logger = get_logger(__name__)


class SenderMixin(DeliveryMixinBase):
    """NATS message fan-out and initial webhook POST delivery."""

    async def _on_message(self, msg: Any) -> None:
        """NATS subscription callback — fan the event out to webhooks."""
        try:
            await self.deliver(msg.subject, msg.data)
        except Exception as exc:  # noqa: BLE001 — delivery must never raise
            logger.warning("webhook_on_message_failed", subject=str(getattr(msg, "subject", "")), error=str(exc)[:120])

    async def deliver(self, subject: str, payload: bytes) -> list[bool]:
        """POST ``payload`` to every subscription matching ``subject``.

        Called by the NATS message handler. For each matching webhook:

        * Standard Webhooks headers (``webhook-id`` / ``webhook-timestamp``
          / ``webhook-signature: v1,<base64>``) plus ``X-LCI-Event`` /
          ``X-LCI-Chain``,
        * one immediate POST; failures are parked in the DLQ and retried
          on the Svix schedule (5s/5m/30m/2h/5h/10h/10h after the initial
          attempt, ±20% jitter) by the background worker,
        * endpoint health short-circuit for broken/disabled endpoints.

        Returns a list of per-webhook success flags (True = delivered on
        the first POST). Never raises — every failure is logged and
        recorded in the delivery log / DLQ.
        """
        matches = self._store.matching_subscriptions(subject)
        if not matches:
            return []

        results: list[bool] = []
        for sub in matches:
            results.append(await self._deliver_one(sub, subject, payload))
        return results

    async def _deliver_one(self, sub: WebhookSubscription, subject: str, payload: bytes) -> bool:
        """Initial delivery attempt for one webhook (DLQ on failure)."""
        clock = self._clock
        deadline = clock() + self.total_timeout_seconds
        if clock() >= deadline:
            await self._record_dropped(sub, subject)
            return False

        if sub.health in ("broken", "disabled"):
            logger.warning("webhook_health_skip", subscription_id=sub.id, url=sub.url, health=sub.health)
            return False

        event_type, chain = _parse_subject(subject)
        webhook_id = uuid.uuid4().hex
        timestamp = int(time.time())

        delivered, status_code, duration_ms, error = await self._attempt_post(
            sub, webhook_id, timestamp, subject, event_type, chain, payload
        )
        if delivered:
            self._store.record_success(sub.id, subject, status_code or 0, duration_ms)
            metrics_incr("webhook_deliveries")
            logger.debug("webhook_delivered", subscription_id=sub.id, subject=subject, status_code=status_code)
            return True

        status = "failed" if status_code is not None else "error"
        self._store.record_attempt(sub.id, subject, status, status_code, duration_ms)
        self._log_failure(sub, subject, attempts=1, status_code=status_code, error=error)
        self._enqueue_dlq(
            sub,
            subject,
            payload,
            event_type,
            chain,
            webhook_id,
            timestamp,
            attempts=1,
            status_code=status_code,
            error=error,
        )
        return False

    async def _attempt_post(
        self,
        sub: WebhookSubscription,
        webhook_id: str,
        timestamp: int,
        subject: str,
        event_type: str,
        chain: str,
        payload: bytes,
    ) -> tuple[bool, int | None, int, str | None]:
        """POST once with the Standard Webhooks headers.

        Returns ``(delivered, status_code, duration_ms, error)``. Never
        raises — transport errors are captured in the tuple.
        """
        headers = {
            "webhook-id": webhook_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": _build_signature_header(
                sub.secret, sub.additional_secrets, webhook_id, timestamp, payload
            ),
            "X-LCI-Event": event_type,
            "X-LCI-Chain": chain,
            "Content-Type": "application/json",
        }
        start = self._clock()
        try:
            resp = await self._http().post(sub.url, content=payload, headers=headers, timeout=self.timeout_seconds)
            duration_ms = int((self._clock() - start) * 1000)
            if resp.status_code < _SUCCESS_STATUS_LIMIT:
                return True, resp.status_code, duration_ms, None
            return False, resp.status_code, duration_ms, None
        except Exception as exc:  # noqa: BLE001 — transport errors are retried
            duration_ms = int((self._clock() - start) * 1000)
            return False, None, duration_ms, str(exc)[:120]

    async def _record_dropped(self, sub: WebhookSubscription, subject: str) -> None:
        """Record a delivery abandoned when the total budget expired."""
        self._store.record_attempt(sub.id, subject, "dropped", None, 0)
        logger.debug(
            "webhook_budget_exhausted",
            subscription_id=sub.id,
            subject=subject,
            total_timeout_s=self.total_timeout_seconds,
        )

    def _log_failure(
        self, sub: WebhookSubscription, subject: str, *, attempts: int, status_code: int | None, error: str | None
    ) -> None:
        """Log a failed attempt and health-threshold crossings."""
        logger.warning(
            "webhook_attempt_failed",
            subscription_id=sub.id,
            url=sub.url,
            subject=subject,
            attempts=attempts,
            status_code=status_code,
            error=error,
        )
        count = sub.failure_count + 1  # the store already incremented
        if count == HEALTH_WARN:
            logger.warning("webhook_health_warn", subscription_id=sub.id, consecutive_failures=count)
        elif count == HEALTH_BREAK:
            logger.warning("webhook_health_break", subscription_id=sub.id, consecutive_failures=count)
        elif count == HEALTH_DISABLE:
            logger.warning("webhook_health_disabled", subscription_id=sub.id, consecutive_failures=count)

    def _http(self) -> httpx.AsyncClient:
        """Lazily-created shared httpx client (closed by ``stop``)."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_seconds),
                transport=self._transport,
            )
        return self._http_client


__all__ = ["SenderMixin", "DeliveryAttempt"]
