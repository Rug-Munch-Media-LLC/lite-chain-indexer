"""Shared helpers for webhook delivery — subject parsing, signing, verification.

Split out of the former monolithic ``webhooks/delivery.py``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time

# NATS subjects captured by the delivery service (same wildcard set the
# JetStream stream auto-provisions and the publisher writes to).
_DELIVERY_SUBJECTS: tuple[str, ...] = ("chain.>", "alerts.>")

# HTTP statuses treated as success. Anything >= 400 is retried.
_SUCCESS_STATUS_LIMIT = 400

# Standard Webhooks replay window (seconds). Subscribers must reject
# messages whose webhook-timestamp is older than this.
REPLAY_WINDOW_SECONDS = 300

# Svix-style retry schedule: delay before EACH of the 8 attempts
# (0s = immediate initial POST, then 5s / 5m / 30m / 2h / 5h / 10h / 10h
# between subsequent attempts). Jitter is applied to every non-zero delay.
_SVIX_RETRY_DELAYS: tuple[float, ...] = (
    0.0,
    5.0,
    300.0,
    1800.0,
    7200.0,
    18000.0,
    36000.0,
    36000.0,
)

# How often the DLQ retry worker scans for due entries.
_DLQ_POLL_INTERVAL = 1.0

# Max due entries drained per poll (bounds task creation on a backlog).
_DLQ_POLL_BATCH = 100

_DLQ_SCHEMA = """
CREATE TABLE IF NOT EXISTS webhook_dlq (
    id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload BLOB NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    chain TEXT NOT NULL DEFAULT '',
    timestamp INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 1,
    max_attempts INTEGER NOT NULL DEFAULT 8,
    next_attempt_at REAL NOT NULL,
    last_error TEXT,
    last_status_code INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dlq_due ON webhook_dlq(status, next_attempt_at);
"""


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
    """HMAC-SHA256 signature over the raw payload (hex digest).

    Legacy helper kept for compatibility with existing consumers/tests.
    The wire format uses :func:`_sign_v1` (Standard Webhooks).
    """
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _sign_v1(secret: str, webhook_id: str, timestamp: int, payload: bytes) -> str:
    """Standard Webhooks v1 signature: base64 HMAC-SHA256.

    Signs the raw bytes of ``{webhook-id}.{webhook-timestamp}.{raw-body}``
    and base64-encodes the digest. The timestamp is the unix-seconds
    ``webhook-timestamp`` header value (stable across retries), so the
    signed message is identical on every attempt of one event.
    """
    to_sign = f"{webhook_id}.{timestamp}.".encode() + payload
    digest = hmac.new(secret.encode("utf-8"), to_sign, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _build_signature_header(
    secret: str,
    additional_secrets: list[str],
    webhook_id: str,
    timestamp: int,
    payload: bytes,
) -> str:
    """Build the ``webhook-signature`` header value.

    One ``v1,<base64>`` token per secret (primary first), space-separated.
    During rotation the old secret lives in ``additional_secrets`` so the
    message stays verifiable by both keys until subscribers migrate.
    """
    tokens = [f"v1,{_sign_v1(secret, webhook_id, timestamp, payload)}"]
    for extra in additional_secrets:
        tokens.append(f"v1,{_sign_v1(extra, webhook_id, timestamp, payload)}")
    return " ".join(tokens)


def verify_signature(
    payload: bytes,
    webhook_id: str,
    timestamp: str,
    signature_header: str,
    secrets: list[str],
    *,
    now: int | None = None,
    max_age_seconds: int = REPLAY_WINDOW_SECONDS,
) -> bool:
    """Subscriber-side reference verifier (Standard Webhooks v1).

    Use this on the consumer side to validate an incoming delivery:

    1. **Replay window** — rejects when ``abs(now - timestamp)`` exceeds
       ``max_age_seconds`` (default 300s). The sender stamps retries with
       the ORIGINAL timestamp, so a retry arriving minutes later still
       passes as long as it is inside the window.
    2. **Constant-time compare** — every ``v1,<base64>`` token in the
       header is compared against the locally computed digest with
       :func:`hmac.compare_digest`; the check is satisfied when ANY
       configured secret matches ANY token (multi-token = rotation).

    Returns True only when the window check AND a signature match pass.
    """
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    current = int(time.time()) if now is None else now
    if abs(current - ts) > max_age_seconds:
        return False
    tokens = [token for token in signature_header.split(" ") if token.startswith("v1,")]
    if not tokens:
        return False
    for secret in secrets:
        expected = _sign_v1(secret, webhook_id, ts, payload)
        for token in tokens:
            candidate = token[len("v1,") :]
            if hmac.compare_digest(candidate, expected):
                return True
    return False


__all__ = [
    "REPLAY_WINDOW_SECONDS",
    "verify_signature",
]
