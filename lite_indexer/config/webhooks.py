"""Webhook delivery configuration (split from config/indexing)."""

from __future__ import annotations

import os

from lite_indexer.config.indexing import DATA_DIR

# ── Webhook Delivery (push platform) ────────────────────────────────────
# POST every NATS event to registered webhooks (HMAC-SHA256 signed,
# bounded retries). Requires NATS_ENABLED=1 — the delivery service is a
# NATS subscriber. Default ON.
WEBHOOK_ENABLED: bool = os.environ.get("WEBHOOK_ENABLED", "1") != "0"

# SQLite persistence for subscriptions + delivery log.
WEBHOOK_DB_PATH: str = os.environ.get("WEBHOOK_DB_PATH", str(DATA_DIR / "webhooks.db"))

# Retry attempts after the initial POST (backoff 1s, 4s, 16s).
WEBHOOK_MAX_RETRIES: int = int(os.environ.get("WEBHOOK_MAX_RETRIES", "3"))

# Per-request HTTP timeout and total delivery budget (seconds).
WEBHOOK_TIMEOUT_SECONDS: float = float(os.environ.get("WEBHOOK_TIMEOUT_SECONDS", "10.0"))
WEBHOOK_TOTAL_TIMEOUT_SECONDS: float = float(os.environ.get("WEBHOOK_TOTAL_TIMEOUT_SECONDS", "30.0"))

# Backoff sequence between retries (seconds), matching .env.example.
WEBHOOK_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 4.0, 16.0)

# Per-IP cap on webhook registrations (API layer).
WEBHOOK_MAX_PER_IP: int = int(os.environ.get("WEBHOOK_MAX_PER_IP", "10"))

# ── Webhook API auth (fail-closed) ──────────────────────────────────────
# Shared secret guarding the webhook MANAGEMENT routes. Registration used
# to be completely unauthenticated, which handed any caller a live SSRF
# egress (the indexer POSTs signed events to whatever URL is registered).
# Registration is now gated on the same secret as delete/logs. Unset is a
# misconfiguration, not an invitation — see api/webhook_auth.py, where the
# dev escape is API_ALLOW_UNAUTHENTICATED_WRITE (one flag for the whole
# write surface) and WEBHOOK_API_KEY unset keeps registration reachable
# only under that escape.
WEBHOOK_API_KEY: str = os.environ.get("WEBHOOK_API_KEY") or ""

# Allow the unauthenticated local dev loop (no secrets at all). Off by
# default; never set on a shared deployment.
ALLOW_UNAUTHENTICATED_WRITE: bool = os.environ.get("API_ALLOW_UNAUTHENTICATED_WRITE") == "1"
