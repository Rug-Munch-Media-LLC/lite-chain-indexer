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
    NATS_STREAM_DISCARD,
    NATS_STREAM_MAX_AGE_SECONDS,
    NATS_STREAM_MAX_BYTES,
    NATS_STREAM_NAME,
    NATS_STREAM_RETENTION,
    NATS_STREAM_STORAGE,
    NATS_URL,
)
from lite_indexer.logging import get_logger
from lite_indexer.streaming.nats_batch import prepare_batch, send_batch
from lite_indexer.streaming.nats_circuit import CircuitBreaker as _CircuitBreaker
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)


class NATSUnavailableError(RuntimeError):
    """Raised internally when the NATS connection is unavailable."""




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
        "launch": "chain.{chain}.launch",
        "jito_bundle": "chain.{chain}.bundle",
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
        batch_timeout: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._url = url or NATS_URL
        self.enabled = NATS_ENABLED if enabled is None else enabled
        self._failure_threshold = failure_threshold or NATS_CIRCUIT_FAILURE_THRESHOLD
        self._cooldown_seconds = cooldown_seconds or NATS_CIRCUIT_COOLDOWN_SECONDS
        self._jetstream_enabled = NATS_JETSTREAM_ENABLED if jetstream_enabled is None else jetstream_enabled
        self._stream_name = stream_name or NATS_STREAM_NAME
        self._connect_timeout = connect_timeout
        self._batch_timeout = batch_timeout
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

    def _stream_config(self) -> dict[str, Any]:
        """Desired JetStream stream config.

        Mirrors the deployed ``lite-chain-indexer-nats`` server
        (compose.nats.yml + compose/nats-server.conf): FILE storage on
        the persisted nats-data volume, limits retention, 3-day / 5GB
        cap, discard old. Keeping this aligned with the server avoids
        NATS err 10058 ("stream name already in use with a different
        configuration") on every connect.
        """
        return {
            "name": self._stream_name,
            "subjects": list(self._STREAM_SUBJECTS),
            "storage": NATS_STREAM_STORAGE,
            "retention": NATS_STREAM_RETENTION,
            "max_age": NATS_STREAM_MAX_AGE_SECONDS,
            "max_bytes": NATS_STREAM_MAX_BYTES,
            "discard": NATS_STREAM_DISCARD,
        }

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
                connect_timeout=int(self._connect_timeout),
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
        or the stream cannot be created/reconciled, the publisher
        degrades to core NATS publish.

        The desired config (see :meth:`_stream_config`) mirrors the
        deployed server. If the stream already exists with a *different*
        configuration, ``add_stream`` fails with NATS err 10058; the
        publisher then reconciles via ``update_stream`` — non-destructive,
        preserves buffered messages and consumers — instead of logging a
        warning on every connect.
        """
        if self._stream_ensured or self._nc is None:
            return
        self._stream_ensured = True
        try:
            self._js = self._nc.jetstream()
            config = self._stream_config()
            try:
                await self._js.add_stream(**config)
                logger.info("nats_stream_ensured", stream=self._stream_name)
            except Exception:  # noqa: BLE001 — reconcile on mismatch
                if await self._reconcile_stream(config):
                    logger.info("nats_stream_reconciled", stream=self._stream_name)
                else:
                    raise
        except Exception as exc:  # noqa: BLE001 — provisioning is best-effort
            self._js = None
            logger.warning("nats_stream_ensure_failed", error=str(exc)[:120])

    async def _reconcile_stream(self, config: dict[str, Any]) -> bool:
        """Align an existing stream to the desired config via update_stream.

        ``add_stream`` returns NATS err 10058 when the stream exists with
        a different configuration. ``update_stream`` is non-destructive:
        buffered messages and consumers are preserved, which is safe for
        this replayable fan-out stream (no delete/recreate needed).

        Returns True when the stream now matches the desired config;
        False when the stream is absent, JetStream is unavailable, or the
        server rejected the update (caller degrades to core NATS).
        """
        if self._js is None:
            return False
        try:
            await self._js.stream_info(self._stream_name)
        except Exception as exc:  # noqa: BLE001 — stream absent / JetStream off
            logger.debug("nats_stream_reconcile_no_info", error=str(exc)[:120])
            return False
        try:
            await self._js.update_stream(**config)
            logger.info(
                "nats_stream_reconciled",
                stream=self._stream_name,
                storage=config.get("storage"),
                max_age_s=config.get("max_age"),
            )
            return True
        except Exception as exc:  # noqa: BLE001 — update rejected
            logger.warning("nats_stream_reconcile_failed", error=str(exc)[:120])
            return False

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
        """Publish a block batch — pipelined sends, one bounded wait.

        Per-event sequential acks could stall a sync window for minutes on
        a degraded server (2026-09-22: 14k sequential acks at 2-10s server
        readloops). Sends are pipelined via :mod:`nats_batch`; best-effort:
        never raises.

        Args:
            chain: Chain name.
            block: Block/slot number (injected into each event payload).
            events: ``(event_type, data)`` tuples; unknown types are skipped.

        Returns:
            Number of events accepted by NATS.
        """
        if not self.enabled or not events:
            return 0
        if self._breaker.is_open:
            logger.debug("nats_circuit_open_drop", chain=chain, event_type="batch")
            return 0
        prepared = prepare_batch(chain, block, events, self.subject_for)
        if not prepared:
            return 0

        try:
            if (self._nc is None or not self._nc.is_connected) and not await self.connect():
                raise NATSUnavailableError(f"not connected to {self._url}")
            ok, failed = await send_batch(self._js, self._nc, prepared, timeout=self._batch_timeout)
        except TimeoutError:
            logger.warning(
                "nats_batch_timeout", chain=chain, block=block,
                events=len(prepared), timeout_s=self._batch_timeout,
            )
            metrics_incr("nats_publish_failures", len(prepared))
            if self._breaker.record_failure():
                logger.warning(
                    "nats_circuit_opened",
                    failures=self._breaker.consecutive_failures,
                    cooldown_s=self._cooldown_seconds,
                )
            return 0
        except Exception as exc:  # noqa: BLE001 — best-effort fan-out
            logger.warning("nats_batch_publish_failed", chain=chain, block=block, error=str(exc)[:120])
            metrics_incr("nats_publish_failures", len(prepared))
            self._breaker.record_failure()
            return 0

        if failed:
            logger.warning("nats_batch_partial", chain=chain, block=block, published=ok, failed=failed)
            metrics_incr("nats_publish_failures", failed)
        if ok:
            self._breaker.record_success()
            metrics_incr("nats_publishes", ok)
        else:
            self._breaker.record_failure()
        return ok


__all__ = ["NATSPublisher", "NATSUnavailableError"]
