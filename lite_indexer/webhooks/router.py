"""
FastAPI router for webhook management.

Endpoints:

* ``POST   /api/v1/webhooks``         — register a webhook
* ``DELETE /api/v1/webhooks/{id}``    — unregister
* ``GET    /api/v1/webhooks``         — list subscriptions
* ``GET    /api/v1/webhooks/{id}/logs`` — delivery log

Security:

* Registration is per-IP rate limited (``WEBHOOK_MAX_PER_IP``, applied
  in ``api/app.py`` where the slowapi ``Limiter`` lives).
* URLs must be ``https`` (rejected with 422 otherwise).
* Subjects must be non-empty, whitespace-free NATS patterns.

This module is imported ONLY by the ``api`` package (FastAPI optional
extra), so the core ``lite_indexer`` package never requires fastapi.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, field_validator

from lite_indexer.config import WEBHOOK_MAX_PER_IP
from lite_indexer.webhooks.auth import require_webhook_api_key
from lite_indexer.webhooks.delivery import WebhookDelivery
from lite_indexer.webhooks.models import DeliveryAttempt, WebhookSubscription
from lite_indexer.webhooks.ssrf import is_safe_webhook_url
from lite_indexer.webhooks.storage import WebhookStore

router = APIRouter(tags=["webhooks"])


# NATS subject: dot-separated tokens of word chars, '-', '_', plus the
# wildcard tokens * / > (each wildcard occupies a full token).
_SUBJECT_TOKEN = re.compile(r"^[A-Za-z0-9_.*>-]+$")


class WebhookRegisterRequest(BaseModel):
    """Register-webhook request body."""

    url: str = Field(description="HTTPS endpoint events are POSTed to")
    subjects: list[str] = Field(
        description="NATS subject patterns (supports * and > wildcards), e.g. chain.ethereum.transfer"
    )
    secret: str | None = Field(
        default=None,
        description="HMAC-SHA256 signing secret (optional, never returned in responses)",
    )

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("webhook URL must be https")
        # SSRF protection (audit SEC-3a): reject private/loopback/link-local
        # targets and non-standard ports. Re-checked at send time too.
        if not is_safe_webhook_url(value):
            raise ValueError("webhook URL host is not allowed")
        return value

    @field_validator("subjects")
    @classmethod
    def _validate_subjects(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one subject is required")
        for subject in value:
            if not subject or " " in subject or not _SUBJECT_TOKEN.fullmatch(subject):
                raise ValueError(f"invalid subject pattern: {subject!r}")
        return value


class WebhookOut(BaseModel):
    """Subscription as returned by the API (secret never included)."""

    id: str
    url: str
    subjects: list[str]
    created_at: str
    last_success: str | None = None
    failure_count: int = 0

    @classmethod
    def from_subscription(cls, sub: WebhookSubscription) -> WebhookOut:
        return cls(
            id=sub.id,
            url=sub.url,
            subjects=sub.subjects,
            created_at=sub.created_at,
            last_success=sub.last_success,
            failure_count=sub.failure_count,
        )


# ── Shared backend ───────────────────────────────────────────────────
# The delivery service is the process-wide singleton (one NATS
# subscription); the router shares its SQLite store so registrations
# made here are immediately picked up by the live delivery loop.

_delivery: WebhookDelivery | None = None
_store: WebhookStore | None = None


def get_delivery() -> WebhookDelivery:
    """Return the shared delivery service (lazy, patchable in tests)."""
    global _delivery
    if _delivery is None:
        _delivery = WebhookDelivery.shared()
    return _delivery


def get_store() -> WebhookStore:
    """Return the shared store (lazy, patchable in tests)."""
    global _store
    if _store is None:
        _store = get_delivery()._store  # noqa: SLF001 — same process store
    return _store


@router.post(
    "/api/v1/webhooks",
    response_model=WebhookOut,
    status_code=201,
    summary="Register a webhook",
)
async def register_webhook(request: Request, body: WebhookRegisterRequest) -> WebhookOut:
    """Register a new webhook subscription.

    The endpoint must be ``https`` and the subjects must be NATS subject
    patterns (``*`` / ``>`` wildcards allowed). Returns the created
    subscription (the secret is never echoed back).
    """
    delivery = get_delivery()
    subscription_id = await delivery.subscribe(body.url, body.subjects, body.secret)
    sub = delivery._store.get_subscription(subscription_id)  # noqa: SLF001
    if sub is None:  # pragma: no cover — just created, cannot be missing
        raise HTTPException(status_code=500, detail="subscription not persisted")
    return WebhookOut.from_subscription(sub)


@router.delete(
    "/api/v1/webhooks/{subscription_id}",
    status_code=204,
    summary="Unregister a webhook",
)
async def unregister_webhook(subscription_id: str, _: None = Depends(require_webhook_api_key)) -> None:
    """Remove a webhook subscription and stop delivering to it."""
    removed = await get_delivery().unsubscribe(subscription_id)
    if not removed:
        raise HTTPException(status_code=404, detail="webhook subscription not found")
    return None


@router.get(
    "/api/v1/webhooks",
    response_model=list[WebhookOut],
    summary="List webhook subscriptions",
)
async def list_webhooks() -> list[WebhookOut]:
    """Return all registered webhook subscriptions (oldest first)."""
    return [WebhookOut.from_subscription(sub) for sub in get_delivery().list_subscriptions()]


@router.get(
    "/api/v1/webhooks/{subscription_id}/logs",
    response_model=list[DeliveryAttempt],
    summary="Webhook delivery log",
)
async def webhook_logs(
    subscription_id: str,
    limit: int = Query(default=100, ge=1, le=1000, description="Max log entries"),
    _: None = Depends(require_webhook_api_key),
) -> list[DeliveryAttempt]:
    """Return the most recent delivery attempts for a subscription."""
    delivery = get_delivery()
    if delivery._store.get_subscription(subscription_id) is None:  # noqa: SLF001
        raise HTTPException(status_code=404, detail="webhook subscription not found")
    return delivery.delivery_log(subscription_id, limit)


__all__ = ["router", "get_delivery", "get_store", "WebhookOut", "WebhookRegisterRequest", "WEBHOOK_MAX_PER_IP"]
