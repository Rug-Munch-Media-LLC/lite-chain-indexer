"""
Webhook domain models — subscription, delivery-attempt, and DLQ records.

These are plain dataclasses (not ORM models / not Pydantic): the
webhook layer is deliberately dependency-light. ``lite_indexer`` core
must import without FastAPI installed, so the API request/response
schemas live in ``lite_indexer.webhooks.router`` (imported only by the
``api`` package).

Delivery health
---------------

Endpoint health is DERIVED from ``WebhookSubscription.failure_count``
(consecutive failures — the counter resets on any success):

* ``healthy``  (0-5)   — deliveries proceed normally.
* ``warn``     (6-19)  — deliveries proceed; every failure logs a
  warning so operators see an endpoint degrading.
* ``broken``   (20-49) — circuit break: live-event intake short-circuits
  (no POST). The DLQ retry worker keeps retrying so the endpoint
  recovers automatically the moment it accepts a message again.
* ``disabled`` (50+)   — intake and retries stop and queued messages are
  dead-lettered. Re-enabling requires re-registering the subscription.

The thresholds mirror the delivery health contract in
``lite_indexer.webhooks.delivery`` (warn=6, break=20, disable=50).
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Endpoint health thresholds (consecutive delivery failures).
HEALTH_WARN = 6
HEALTH_BREAK = 20
HEALTH_DISABLE = 50


@dataclass
class WebhookSubscription:
    """A registered webhook destination.

    Attributes:
        id: Stable subscription id (UUID hex).
        url: HTTPS endpoint events are POSTed to.
        subjects: NATS subject patterns this webhook wants (``*`` and
            ``>`` wildcards supported), e.g. ``chain.ethereum.transfer``
            or ``alerts.whale.>``.
        secret: Primary HMAC-SHA256 signing secret (empty string =
            unsigned).
        additional_secrets: Extra signing secrets used during rotation —
            every retry signs with the primary secret AND each of these
            so the ``webhook-signature`` header carries one ``v1,<sig>``
            token per secret (subscribers verify any of them).
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
    additional_secrets: list[str] = field(default_factory=list)
    created_at: str = ""
    last_success: str | None = None
    failure_count: int = 0

    @property
    def health(self) -> str:
        """Endpoint health derived from consecutive failures.

        ``healthy`` | ``warn`` | ``broken`` | ``disabled``. Because the
        state is derived from the persisted ``failure_count`` it survives
        restarts, and any successful delivery resets it to ``healthy``.
        """
        if self.failure_count >= HEALTH_DISABLE:
            return "disabled"
        if self.failure_count >= HEALTH_BREAK:
            return "broken"
        if self.failure_count >= HEALTH_WARN:
            return "warn"
        return "healthy"


@dataclass
class DeliveryAttempt:
    """One POST attempt (or final failure) recorded in the delivery log.

    Attributes:
        id: Stable attempt id (UUID hex).
        subscription_id: Owning subscription.
        subject: NATS subject that triggered the delivery.
        timestamp: ISO-8601 UTC attempt timestamp.
        status: ``success`` | ``failed`` (non-2xx) | ``error``
            (network/transport error) | ``dropped`` (budget exhausted) |
            ``deadletter`` (retries exhausted, parked in the DLQ).
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


@dataclass
class DlqEntry:
    """One message parked in the dead-letter queue awaiting retry.

    A message is enqueued after its initial POST fails and re-attempted
    on the Svix 8-attempt schedule (5s/5m/30m/2h/5h/10h/10h after the
    first attempt, jittered). Once ``attempts`` reaches ``max_attempts``
    the entry is marked ``dead`` and stays in the table as an audit
    trail of undeliverable messages.

    Attributes:
        id: The message's ``webhook-id`` — the idempotency key. It is
            generated once per NATS event and is STABLE across every
            retry so subscribers can dedupe by it.
        subscription_id: Owning subscription.
        subject: NATS subject that triggered the delivery.
        payload: Raw request body bytes (re-sent verbatim on retry).
        event_type: Parsed subject event type (re-sent in ``X-LCI-Event``).
        chain: Parsed subject chain (re-sent in ``X-LCI-Chain``).
        timestamp: ``webhook-timestamp`` (unix seconds) — stable across
            retries so the subscriber's replay-window check still passes.
        attempts: Failed attempts so far (1 = the initial POST failed).
        max_attempts: Total attempts before dead-lettering (Svix: 8).
        next_attempt_at: Epoch seconds when the next attempt is due.
        status: ``pending`` | ``in_flight`` | ``dead``.
        last_error / last_status_code: Reason for the last failure.
        created_at: ISO-8601 UTC enqueue timestamp.
    """

    id: str
    subscription_id: str
    subject: str
    payload: bytes
    event_type: str
    chain: str
    timestamp: int
    attempts: int
    max_attempts: int
    next_attempt_at: float
    status: str = "pending"
    last_error: str | None = None
    last_status_code: int | None = None
    created_at: str = ""


__all__ = [
    "WebhookSubscription",
    "DeliveryAttempt",
    "DlqEntry",
    "HEALTH_WARN",
    "HEALTH_BREAK",
    "HEALTH_DISABLE",
]
