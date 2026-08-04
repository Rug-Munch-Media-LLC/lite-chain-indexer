"""
Webhook domain models — subscription and delivery-attempt records.

These are plain dataclasses (not ORM models / not Pydantic): the
webhook layer is deliberately dependency-light. ``lite_indexer`` core
must import without FastAPI installed, so the API request/response
schemas live in ``lite_indexer.webhooks.router`` (imported only by the
``api`` package).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class WebhookSubscription:
    """A registered webhook destination.

    Attributes:
        id: Stable subscription id (UUID hex).
        url: HTTPS endpoint events are POSTed to.
        subjects: NATS subject patterns this webhook wants (``*`` and
            ``>`` wildcards supported), e.g. ``chain.ethereum.transfer``
            or ``alerts.whale.>``.
        secret: HMAC-SHA256 signing secret (empty string = unsigned).
        created_at: ISO-8601 UTC registration timestamp.
        last_success: ISO-8601 UTC timestamp of the last successful
            delivery, or ``None`` when never successful.
        failure_count: Consecutive delivery failures since the last
            success (reset on success).
    """

    id: str
    url: str
    subjects: list[str] = field(default_factory=list)
    secret: str = ""
    created_at: str = ""
    last_success: str | None = None
    failure_count: int = 0


@dataclass
class DeliveryAttempt:
    """One POST attempt (or final failure) recorded in the delivery log.

    Attributes:
        id: Stable attempt id (UUID hex).
        subscription_id: Owning subscription.
        subject: NATS subject that triggered the delivery.
        timestamp: ISO-8601 UTC attempt timestamp.
        status: ``success`` | ``failed`` (non-2xx) | ``error``
            (network/transport error) | ``dropped`` (budget exhausted).
        status_code: HTTP status returned by the endpoint (None when
            the attempt never completed a request).
        duration_ms: Wall-clock duration of the attempt in ms.
    """

    id: str
    subscription_id: str
    subject: str
    timestamp: str
    status: str
    status_code: int | None = None
    duration_ms: int = 0


__all__ = ["WebhookSubscription", "DeliveryAttempt"]
