"""
Webhook delivery — subscribes to NATS subjects and POSTs events to
registered webhooks, turning the indexer's real-time stream into a push
platform. Goldsky charges for webhooks; this ships them free.

Flow::

    NATS subject ──▶ WebhookDelivery._on_message ──▶ deliver(subject, payload)
        │                                              │
        │                                              ▼
        │                              POST matching webhook(s)
        │                              • Standard Webhooks signature
        │                                (webhook-id / webhook-timestamp /
        │                                 webhook-signature: v1,<b64>)
        │                              • X-LCI-Event / X-LCI-Chain headers
        │                              • Svix 8-attempt retry schedule
        │                                (0s/5s/5m/30m/2h/5h/10h/10h, jittered)
        │                              • SQLite DLQ for in-flight retries
        │                              • endpoint health warn-6 / break-20 /
        │                                disable-50
        ▼
    delivery_log + webhook_dlq (SQLite)

Wire format (Standard Webhooks v1)
----------------------------------

Every POST carries::

    webhook-id:        <stable message id>            (idempotency key)
    webhook-timestamp: <unix seconds>                 (stable across retries)
    webhook-signature: v1,<base64 HMAC-SHA256> [v1,<b64> ...]

The v1 signature is HMAC-SHA256 over the raw bytes of
``{webhook-id}.{webhook-timestamp}.{raw-body}``. When a subscription has
rotation secrets configured, one ``v1,<sig>`` token is emitted per secret
so subscribers can verify against the old or the new secret.

Subscriber-side verification (reference: :func:`verify_signature`)
------------------------------------------------------------------

1. **Replay window.** Reject the message when
   ``abs(now - webhook-timestamp) > 300s`` (the timestamp is stable
   across retries, so late retries still pass).
2. **Constant-time compare.** Split ``webhook-signature`` on spaces,
   keep the ``v1,`` tokens, and compare each candidate against the
   locally-computed digest with ``hmac.compare_digest`` (never ``==`` —
   the header is attacker-controlled and must not leak timing).
3. **Idempotency.** Cache processed ``webhook-id`` values and ignore
   repeats — the same id is re-sent on every retry.

Design rules (mirroring the NATS publisher):

1. **Best-effort, never raises.** ``start()`` and ``deliver()`` swallow
   and log every failure — a dead NATS server or a flaky webhook must
   never block indexing.
2. **Process-wide singleton.** The daemon runs one ``LiteIndexer`` per
   chain; the delivery service must subscribe to ``chain.>`` / ``alerts.>``
   exactly once or every event would be delivered N times. Use
   :meth:`WebhookDelivery.shared`.
3. **Persisted retries + DLQ.** A failed POST is parked in the SQLite
   ``webhook_dlq`` table and retried on the Svix schedule
   (5s/5m/30m/2h/5h/10h/10h after the initial attempt, ±20% jitter).
   After 8 failed attempts the entry is dead-lettered (``status='dead'``)
   and stays as an audit record. A background worker (started by
   :meth:`WebhookDelivery.start`) drains due entries, so retries survive
   process restarts.
4. **Endpoint health.** Consecutive failures drive health:
   warn ≥ 6 (logged), circuit-break ≥ 20 (live intake short-circuits),
   disable ≥ 50 (retries stop, queued messages dead-lettered). A
   successful retry resets the counter and recovers the endpoint.
5. **Signed.** Every POST carries the Standard Webhooks headers above.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import random
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime
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
from lite_indexer.webhooks.models import (
    HEALTH_BREAK,
    HEALTH_DISABLE,
    HEALTH_WARN,
    DeliveryAttempt,
    DlqEntry,
    WebhookSubscription,
)
from lite_indexer.webhooks.storage import WebhookStore

logger = get_logger(__name__)

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


class WebhookDelivery:
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
        deadline = self._clock() + self.total_timeout_seconds
        if self._clock() >= deadline:
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

    # ── DLQ (dead-letter queue) ──────────────────────────────────────

    def _start_worker(self) -> None:
        """Start the DLQ retry worker once (idempotent)."""
        if self._worker_task is None and not self._worker_running:
            self._worker_running = True
            self._worker_task = asyncio.create_task(self._dlq_worker())

    async def _dlq_worker(self) -> None:
        """Background poller: drain due DLQ entries every poll interval."""
        try:
            while not self._stop_event.is_set():
                self._process_due_dlq()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    continue
        finally:
            self._worker_running = False

    def _process_due_dlq(self) -> None:
        """Schedule retries for every DLQ entry whose time has come."""
        due = self._dlq_due_rows(time.time())
        for row in due:
            asyncio.create_task(self._retry_entry(str(row["id"])))

    def _enqueue_dlq(
        self,
        sub: WebhookSubscription,
        subject: str,
        payload: bytes,
        event_type: str,
        chain: str,
        webhook_id: str,
        timestamp: int,
        *,
        attempts: int,
        status_code: int | None,
        error: str | None,
    ) -> None:
        """Park a failed message in the DLQ and schedule its next attempt.

        The DLQ row id IS the ``webhook-id`` idempotency key, so every
        retry re-sends the identical signature input (id, timestamp,
        raw body) that the subscriber saw on the initial attempt.
        """
        delay = self._jittered(self._schedule[min(attempts, len(self._schedule) - 1)])
        entry = DlqEntry(
            id=webhook_id,
            subscription_id=sub.id,
            subject=subject,
            payload=payload,
            event_type=event_type,
            chain=chain,
            timestamp=timestamp,
            attempts=attempts,
            max_attempts=self.max_attempts,
            next_attempt_at=time.time() + delay,
            status="pending",
            last_error=error,
            last_status_code=status_code,
            created_at=datetime.now(UTC).isoformat(),
        )
        self._dlq_insert(entry)
        metrics_incr("webhook_dlq_enqueued")
        if not self._worker_running:
            asyncio.create_task(self._retry_later(entry.id, delay))

    async def _retry_later(self, entry_id: str, delay: float) -> None:
        """Single-shot timer path used when the worker is not running."""
        if delay > 0:
            await asyncio.sleep(delay)
        await self._retry_entry(entry_id)

    async def _retry_entry(self, entry_id: str) -> None:
        """Attempt one DLQ entry; reschedule, dead-letter, or resolve it."""
        entry = self._get_dlq_entry(entry_id)
        if entry is None or entry.status != "pending":
            return
        if not self._claim_dlq_entry(entry_id):
            return  # another task/worker is already handling it
        sub = self._store.get_subscription(entry.subscription_id)
        if sub is None:
            self._mark_dlq_dead(entry_id, entry.attempts, "subscription removed")
            return
        if sub.health == "disabled":
            self._mark_dlq_dead(entry_id, entry.attempts, "endpoint disabled at 50 consecutive failures")
            return

        delivered, status_code, duration_ms, error = await self._attempt_post(
            sub, entry.id, entry.timestamp, entry.subject, entry.event_type, entry.chain, entry.payload
        )
        if delivered:
            self._store.record_success(sub.id, entry.subject, status_code or 0, duration_ms)
            self._delete_dlq_entry(entry_id)
            metrics_incr("webhook_deliveries")
            logger.debug("webhook_dlq_delivered", subscription_id=sub.id, subject=entry.subject)
            return

        attempts = entry.attempts + 1
        status = "failed" if status_code is not None else "error"
        self._store.record_attempt(sub.id, entry.subject, status, status_code, duration_ms)
        self._log_failure(sub, entry.subject, attempts=attempts, status_code=status_code, error=error)

        if attempts >= entry.max_attempts:
            # Retries exhausted — terminal dead-letter, kept as audit trail.
            self._store.record_attempt(sub.id, entry.subject, "deadletter", status_code, duration_ms)
            self._mark_dlq_dead(entry_id, attempts, error or f"http {status_code}")
            metrics_incr("webhook_dlq_dead")
            logger.warning("webhook_deadlettered", subscription_id=sub.id, subject=entry.subject, webhook_id=entry.id)
            return

        delay = self._jittered(self._schedule[min(attempts, len(self._schedule) - 1)])
        self._reschedule_dlq(entry_id, attempts, time.time() + delay, status_code, error)
        if not self._worker_running:
            asyncio.create_task(self._retry_later(entry_id, delay))

    def _jittered(self, delay: float) -> float:
        """Apply ±20% jitter to a non-zero backoff (thundering-herd guard)."""
        if delay <= 0 or not self._jitter:
            return delay
        return delay * random.uniform(0.8, 1.2)

    # ── DLQ SQLite access (same DB file, own connection) ─────────────

    def _dlq_connect(self) -> sqlite3.Connection:
        """Open (or return) the DLQ connection, creating the table once."""
        if self._dlq_conn is None:
            self._store.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self._store.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(_DLQ_SCHEMA)
            conn.commit()
            self._dlq_conn = conn
        return self._dlq_conn

    def _dlq_insert(self, entry: DlqEntry) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "INSERT OR REPLACE INTO webhook_dlq (id, subscription_id, subject, payload, event_type, chain,"
                " timestamp, attempts, max_attempts, next_attempt_at, last_error, last_status_code, status, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entry.id,
                    entry.subscription_id,
                    entry.subject,
                    entry.payload,
                    entry.event_type,
                    entry.chain,
                    entry.timestamp,
                    entry.attempts,
                    entry.max_attempts,
                    entry.next_attempt_at,
                    entry.last_error,
                    entry.last_status_code,
                    entry.status,
                    entry.created_at,
                ),
            )
            conn.commit()

    def _get_dlq_entry(self, entry_id: str) -> DlqEntry | None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            row = conn.execute("SELECT * FROM webhook_dlq WHERE id = ?", (entry_id,)).fetchone()
        return self._row_to_dlq(row)

    def _claim_dlq_entry(self, entry_id: str) -> bool:
        """Mark pending → in_flight; False when another task owns it."""
        with self._dlq_lock:
            conn = self._dlq_connect()
            cursor = conn.execute(
                "UPDATE webhook_dlq SET status = 'in_flight' WHERE id = ? AND status = 'pending'",
                (entry_id,),
            )
            conn.commit()
            return cursor.rowcount > 0

    def _reschedule_dlq(
        self,
        entry_id: str,
        attempts: int,
        next_attempt_at: float,
        status_code: int | None,
        error: str | None,
    ) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "UPDATE webhook_dlq SET attempts = ?, next_attempt_at = ?, status = 'pending',"
                " last_status_code = ?, last_error = ? WHERE id = ?",
                (attempts, next_attempt_at, status_code, error, entry_id),
            )
            conn.commit()

    def _mark_dlq_dead(self, entry_id: str, attempts: int, reason: str) -> None:
        """Persist the terminal dead-letter state (final attempt count kept for audit)."""
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute(
                "UPDATE webhook_dlq SET status = 'dead', attempts = ?, last_error = ? WHERE id = ?",
                (attempts, reason, entry_id),
            )
            conn.commit()

    def _delete_dlq_entry(self, entry_id: str) -> None:
        with self._dlq_lock:
            conn = self._dlq_connect()
            conn.execute("DELETE FROM webhook_dlq WHERE id = ?", (entry_id,))
            conn.commit()

    def _dlq_due_rows(self, now: float) -> list[sqlite3.Row]:
        with self._dlq_lock:
            conn = self._dlq_connect()
            rows = conn.execute(
                "SELECT id FROM webhook_dlq WHERE status = 'pending' AND next_attempt_at <= ?"
                " ORDER BY next_attempt_at ASC LIMIT ?",
                (now, _DLQ_POLL_BATCH),
            ).fetchall()
        return list(rows)

    @staticmethod
    def _row_to_dlq(row: sqlite3.Row | None) -> DlqEntry | None:
        if row is None:
            return None
        return DlqEntry(
            id=str(row["id"]),
            subscription_id=str(row["subscription_id"]),
            subject=str(row["subject"]),
            payload=bytes(row["payload"] or b""),
            event_type=str(row["event_type"] or ""),
            chain=str(row["chain"] or ""),
            timestamp=int(row["timestamp"] or 0),
            attempts=int(row["attempts"] or 0),
            max_attempts=int(row["max_attempts"] or 8),
            next_attempt_at=float(row["next_attempt_at"] or 0.0),
            status=str(row["status"] or "pending"),
            last_error=row["last_error"],
            last_status_code=row["last_status_code"],
            created_at=str(row["created_at"] or ""),
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


__all__ = ["WebhookDelivery", "verify_signature", "REPLAY_WINDOW_SECONDS"]
