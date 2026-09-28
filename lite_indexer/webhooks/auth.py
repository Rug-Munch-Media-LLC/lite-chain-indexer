"""Auth for the webhook management routes (fail-closed).

Two gates live here:

* :func:`require_webhook_api_key` — guards ``DELETE /webhooks/{id}`` and
  ``GET /webhooks/{id}/logs``. Compared with ``secrets.compare_digest``.
* :func:`require_registration_auth` — guards ``POST /webhooks``. That route
  used to have NO auth at all, so any caller could point the indexer's
  signed egress at a URL of their choosing.

Both fail closed. An unset key is a misconfiguration, not an invitation:
the request is refused with 503 naming the env var. The single documented
escape is ``API_ALLOW_UNAUTHENTICATED_WRITE=1`` for the local dev loop,
which :func:`assert_webhook_auth_configured` also checks at startup.
"""

from __future__ import annotations

import secrets

from fastapi import Header, HTTPException, status

from lite_indexer.config.webhooks import ALLOW_UNAUTHENTICATED_WRITE, WEBHOOK_API_KEY

#: Env var operators must set to run the local dev loop unauthenticated.
DEV_ESCAPE_ENV = "API_ALLOW_UNAUTHENTICATED_WRITE"

#: Env var operators must set to run the local dev loop unauthenticated.


class WebhookAuthConfigError(RuntimeError):
    """Raised when a webhook management route is mounted with no key."""


def assert_webhook_auth_configured() -> None:
    """Refuse to start when the webhook management routes have no key.

    Raises:
        WebhookAuthConfigError: key unset and no dev escape set.
    """
    if WEBHOOK_API_KEY or ALLOW_UNAUTHENTICATED_WRITE:
        return
    raise WebhookAuthConfigError(
        "webhook management routes mounted without an auth secret: set "
        f"WEBHOOK_API_KEY, or set {DEV_ESCAPE_ENV}=1 for local development. "
        "Unset no longer means open."
    )


def _unconfigured_detail() -> str:
    """The 503 body naming the env var an operator must set."""
    return (
        "webhook management API is misconfigured: WEBHOOK_API_KEY is unset. "
        f"Set it, or set {DEV_ESCAPE_ENV}=1 for local development."
    )


def _require_key(provided: str | None, header: str) -> None:
    """Shared constant-time check for a configured webhook secret."""
    if not provided:
        if ALLOW_UNAUTHENTICATED_WRITE:
            return
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_unconfigured_detail(),
        )
    if not secrets.compare_digest(provided, header or ""):
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


def require_webhook_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce the shared secret on management routes."""
    _require_key(WEBHOOK_API_KEY, x_api_key or "")


def require_registration_auth(x_api_key: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce the shared secret on webhook registration.

    Registration is a write: it creates a persistent subscription the
    indexer will POST signed chain data to for as long as it is live.
    """
    _require_key(WEBHOOK_API_KEY, x_api_key or "")


__all__ = [
    "DEV_ESCAPE_ENV",
    "WebhookAuthConfigError",
    "assert_webhook_auth_configured",
    "require_registration_auth",
    "require_webhook_api_key",
]
