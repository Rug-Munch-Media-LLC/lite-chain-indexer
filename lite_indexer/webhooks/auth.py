"""Auth for webhook management routes (audit SEC-3b, 2026-09-19).

Minimal shared-secret gate matching the repo's env-config convention:
when ``WEBHOOKS_API_KEY`` is set, ``DELETE /api/v1/webhooks/{id}`` and
``GET /api/v1/webhooks/{id}/logs`` REQUIRE a matching ``X-API-Key``
header. When unset, the routes stay open for local development and a
startup warning (``webhooks_unauthenticated``) is logged once.
"""

from __future__ import annotations

import os

from fastapi import Header, HTTPException

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

WEBHOOKS_API_KEY: str | None = os.environ.get("WEBHOOKS_API_KEY") or None

if WEBHOOKS_API_KEY is None:  # pragma: no cover — dev-only path
    logger.warning("webhooks_unauthenticated", detail="WEBHOOKS_API_KEY unset; webhook management routes are open")


def require_webhook_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce the shared secret on management routes."""
    if WEBHOOKS_API_KEY is not None and x_api_key != WEBHOOKS_API_KEY:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


__all__ = ["WEBHOOKS_API_KEY", "require_webhook_api_key"]
