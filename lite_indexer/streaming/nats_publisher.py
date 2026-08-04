"""
NATS publisher — best-effort real-time event publishing with a circuit breaker.

Design goals:

1. **Never block indexing.** Every publish failure (connect error, server
   down, JetStream hiccup) is caught, logged, and swallowed. The indexer
   treats NATS as a fan-out layer, not the durable store.

2. **Circuit breaker.** After ``NATS_CIRCUIT_FAILURE_THRESHOLD`` consecutive
   failures the circuit opens and publishes are dropped fast (no network
   attempt) for ``NATS_CIRCUIT_COOLDOWN_SECONDS``. When the cooldown
   expires the circuit is half-open: the next publish is a probe —
   success closes the circuit, failure reopens it.

3. **JetStream with graceful degradation.** When ``NATS_JETSTREAM_ENABLED``
   (default on) the publisher auto-provisions a stream capturing
   ``chain.>`` and ``alerts.>`` on first connect and publishes through
   JetStream. If the server has JetStream disabled the publisher degrades
   to core NATS publish automatically.

4. **Lazy connection.** No connection is opened until the first publish
   (or ``connect()``), so a missing NATS server costs nothing at startup.

Subjects are derived from :attr:`SUBJECTS`, a per-event-type template with
the chain substituted in, e.g. ``chain.ethereum.dex.swap``.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import nats

from lite_indexer.config import (
    NATS_CIRCUIT_COOLDOWN_SECONDS,
    NATS_CIRCUIT_FAILURE_THRESHOLD,
    NATS_ENABLED,
    NATS_JETSTREAM_ENABLED,
    NATS_STREAM_NAME,
    NATS_URL,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)


class NATSUnavailableError(RuntimeError):
    """Raised internally when the NATS connection is unavailable."""


class _CircuitBreaker:
    """Consecutive-failure circuit breaker with a time-based cooldown.

    States:

      - ``closed``: publishing normally. Each failure increments the
        counter; reaching ``failure_threshold`` opens the circuit.
      - ``open``: publishes are dropped without a network attempt until
        ``cooldown_seconds`` elapses.
      - ``half_open``: cooldown expired; the next publish is a probe.
        Success closes the circuit, failure reopens it.
    """

    def __init__(
        self,
        failure_threshold: int,
        cooldown_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.cooldown_seconds = max(0.0, cooldown_seconds)
        self._clock = clock
        self._consecutive_failures = 0
        self._open_until = 0.0

    @property
    def consecutive_failures(self) -> int:
        """Number of consecutive failures recorded (0 when closed)."""
        return self._consecutive_failures

    @property
    def state(self) -> str:
        """One of ``closed``, ``open`` or ``half_open``."""
        if self._open_until > 0.0:
            if self._clock() < self._open_until:
                return "open"
            # Cooldown expired — the next probe will run on the next
            # publish; report half-open so callers can observe it.
            return "half_open"
        return "closed"

    @property
    def is_open(self) -> bool:
        """True when publishes should be dropped without attempting.

        Also transitions an expired open circuit to half-open so the next
        publish acts as a probe.
        """
        if self._open_until == 0.0:
            return False
        if self._clock() < self._open_until:
            return True
        # Cooldown expired → half-open. Reset so the next publish probes.
        self._open_until = 0.0
        self._consecutive_failures = 0
        return False

    def record_success(self) -> None:
        """Close the circuit after a successful publish."""
        self._consecutive_failures = 0
        self._open_until = 0.0

    def record_failure(self) -> bool:
        """Record a failure; returns True if this failure opened the circuit."""
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.failure_threshold:
            self._open_until = self._clock() + self.cooldown_seconds
            return True
        return False


class NATSPublisher:
    """Publishes indexed events to NATS (JetStream when available).

    Args:
        url: NATS server URL. Defaults to ``NATS_URL`` config.
        enabled: Master switch. Defaults to the ``NATS_ENABLED`` config.
        failure_threshold: Consecutive failures before the circuit opens.
            Defaults to ``NATS_CIRCUIT_FAILURE_THRESHOLD``.
        cooldown_seconds: How long the circuit stays open. Defaults to
            ``NATS_CIRCUIT_COOLDOWN_SECONDS``.
        jetstream_enabled: Publish through JetStream (auto-provisioning a
            stream). Defaults to ``NATS_JETSTREAM_ENABLED``.
        stream_name: JetStream stream name to auto-provision.
            Defaults to ``NATS_STREAM_NAME``.
        connect_timeout: NATS connect timeout in seconds.
        clock: Injectable monotonic clock (for tests).
    """

    # Event type → NATS subject template (chain substituted).
    SUBJECTS: dict[str, str] = {
        "evm_transfer": "chain.{chain}.transfer",
        "dex_swap": "chain.{chain}.dex.swap",
        "deployer_event": "chain.{chain}.deploy",
        "whale_alert": "alerts.whale.{chain}",
    }

    # Subjects captured by the auto-provisioned JetStream stream.
    _STREAM_SUBJECTS: tuple[str, ...] = ("chain.>", "alerts.>")

    def __init__(
        self,
        url: str | None = None,
        enabled: bool | None = None,
        failure_threshold: int | None = None,
        cooldown_seconds: int | None = None,
        jetstream_enabled: bool | None = None,
        stream_name: str | None = None,
        connect_timeout: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._url = url or NATS_URL
        self.enabled = NATS_ENABLED if enabled is None else enabled
        self._failure_threshold = failure_threshold or NATS_CIRCUIT_FAILURE_THRESHOLD
        self._cooldown_seconds = cooldown_seconds or NATS_CIRCUIT_COOLDOWN_SECONDS
        self._jetstream_enabled = NATS_JETSTREAM_ENABLED if jetstream_enabled is None else jetstream_enabled
        self._stream_name = stream_name or NATS_STREAM_NAME
        self._connect_timeout = connect_timeout
        self._clock = clock

        self._breaker = _CircuitBreaker(
            failure_threshold=self._failure_threshold,
            cooldown_seconds=self._cooldown_seconds,
            clock=self._clock,
        )

        self._nc: Any | None = None
        self._js: Any | None = None
        self._stream_ensured = False
        self._close_lock = asyncio.Lock()

    # ── Subject mapping ────────────────────────────────────────────

    @classmethod
    def subject_for(cls, chain: str, event_type: str) -> str | None:
        """Resolve the NATS subject for an (chain, event_type) pair.

        Returns ``None`` for unknown event types so callers can drop them
        without raising.
        """
        template = cls.SUBJECTS.get(event_type)
        if template is None:
            return None
        return template.format(chain=chain)

    @classmethod
    def known_event_types(cls) -> tuple[str, ...]:
        """Return the supported event type keys."""
        return tuple(cls.SUBJECTS)

    # ── Connection management ─────────────────────────────────────

    @property
    def connected(self) -> bool:
        """True when a NATS connection is established."""
        return bool(self._nc is not None and self._nc.is_connected)

    @property
    def circuit_state(self) -> str:
        """Observability: ``closed`` | ``open`` | ``half_open``."""
        return self._breaker.state

    @property
    def consecutive_failures(self) -> int:
        """Observability: consecutive failures recorded."""
        return self._breaker.consecutive_failures

    async def connect(self) -> bool:
        """Open the NATS connection (idempotent) and provision JetStream.

        Returns True when connected. Never raises — failures are logged
        and surfaced via the circuit breaker on publish.
        """
        if not self.enabled:
            return False
        if self._nc is not None and self._nc.is_connected:
            return True
        try:
            self._nc = await nats.connect(
                self._url,
                timeout=self._connect_timeout,
                max_reconnect_attempts=0,
            )
        except Exception as exc:  # noqa: BLE001 — connect is best-effort
            self._nc = None
            logger.warning("nats_connect_failed", url=self._url, error=str(exc)[:120])
            return False

        if self._jetstream_enabled:
            await self._ensure_stream()
        return True

    async def _ensure_stream(self) -> None:
        """Best-effort JetStream stream provisioning (runs once).

        A stream capturing ``chain.>`` / ``alerts.>`` makes publishes
        durable and replayable. When JetStream is disabled on the server,
        or the stream already exists with a different configuration, the
        publisher degrades to core NATS publish.
        """
        if self._stream_ensured or self._nc is None:
            return
        self._stream_ensured = True
        try:
            self._js = self._nc.jetstream()
            await self._js.add_stream(
                name=self._stream_name,
                subjects=list(self._STREAM_SUBJECTS),
                storage="memory",
            )
            logger.info("nats_stream_ensured", stream=self._stream_name)
        except Exception as exc:  # noqa: BLE001 — provisioning is best-effort
            self._js = None
            logger.warning("nats_stream_ensure_failed", error=str(exc)[:120])

    async def close(self) -> None:
        """Close the NATS connection (idempotent, never raises)."""
        async with self._close_lock:
            nc, self._nc = self._nc, None
            self._js = None
            if nc is not None:
                try:
                    await nc.close()
                except Exception as exc:  # noqa: BLE001 — close is best-effort
                    logger.debug("nats_close_failed", error=str(exc)[:80])

    # ── Publishing ────────────────────────────────────────────────

    async def _publish(self, subject: str, payload: bytes) -> None:
        """Publish a payload, connecting lazily on demand."""
        if (self._nc is None or not self._nc.is_connected) and not await self.connect():
            raise NATSUnavailableError(f"not connected to {self._url}")
        js = self._js
        if js is not None:
            await js.publish(subject, payload)
        else:
            nc = self._nc
            if nc is None:
                raise NATSUnavailableError(f"not connected to {self._url}")
            await nc.publish(subject, payload)

    async def publish_event(self, chain: str, event_type: str, data: dict[str, Any]) -> bool:
        """Publish a single event. Returns True when accepted by NATS.

        Best-effort: connection failures, JetStream errors, and an open
        circuit all return False without raising. The payload is JSON with
        the event envelope ``{chain, type, published_at, data}``.
        """
        if not self.enabled:
            return False
        if self._breaker.is_open:
            logger.debug("nats_circuit_open_drop", chain=chain, event_type=event_type)
            return False

        subject = self.subject_for(chain, event_type)
        if subject is None:
            logger.debug("nats_unknown_event_type", event_type=event_type)
            return False

        envelope = {
            "chain": chain,
            "type": event_type,
            "published_at": datetime.now(UTC).isoformat(),
            "data": data,
        }
        try:
            payload = json.dumps(envelope, default=str).encode("utf-8")
        except (TypeError, ValueError) as exc:
            logger.warning("nats_payload_serialize_failed", chain=chain, error=str(exc)[:120])
            return False

        try:
            await self._publish(subject, payload)
        except Exception as exc:  # noqa: BLE001 — publishing is best-effort
            metrics_incr("nats_publish_failures")
            tripped = self._breaker.record_failure()
            if tripped:
                logger.warning(
                    "nats_circuit_opened",
                    failures=self._breaker.consecutive_failures,
                    cooldown_s=self._cooldown_seconds,
                    error=str(exc)[:120],
                )
            else:
                logger.warning(
                    "nats_publish_failed",
                    subject=subject,
                    error=str(exc)[:120],
                )
            return False

        self._breaker.record_success()
        metrics_incr("nats_publishes")
        logger.debug("nats_published", subject=subject, chain=chain, event_type=event_type)
        return True

    async def stream_block_batch(
        self,
        chain: str,
        block: int,
        events: list[tuple[str, dict[str, Any]]],
    ) -> int:
        """Publish a block batch of typed events.

        Args:
            chain: Chain name.
            block: Block/slot number (injected into each event payload).
            events: List of ``(event_type, data)`` tuples. Event types not
                in :attr:`SUBJECTS` are skipped.

        Returns:
            The number of events successfully published.
        """
        if not self.enabled or not events:
            return 0

        published = 0
        for event_type, data in events:
            payload = dict(data)
            payload.setdefault("block", block)
            if await self.publish_event(chain, event_type, payload):
                published += 1
        return published


__all__ = ["NATSPublisher", "NATSUnavailableError"]
