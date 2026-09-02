"""Sidecar services mixin — NATS publishing, webhooks, vector ingestion.

Split from indexer/__init__.py (de-godding 2026-08-23). All methods operate
on ``self`` attributes initialized by :class:`LiteIndexer.__init__`.
"""

from __future__ import annotations

import asyncio
from typing import Any

from lite_indexer.indexer._writers import (
    build_nats_payloads,
    maybe_run_vector_ingestion,
)
from lite_indexer.logging import get_logger
from lite_indexer.streaming.nats_publisher import NATSPublisher
from lite_indexer.webhooks.delivery import WebhookDelivery

logger = get_logger(__name__)


class SidecarsMixin:
    """NATS publisher, webhook delivery, and guarded vector ingestion."""

    @property
    def nats_publisher(self) -> NATSPublisher | None:
        """Lazily-created NATS publisher (None when NATS is disabled).

        The publisher is only constructed when ``nats_enabled`` is set
        (env ``NATS_ENABLED=1`` or an explicit constructor override), so
        a disabled deployment never pays the import/attribute cost.
        """
        if not self.nats_enabled:
            return None
        if self._nats_publisher is None:
            self._nats_publisher = NATSPublisher(enabled=True)
        return self._nats_publisher

    @property
    def webhook_delivery(self) -> WebhookDelivery | None:
        """Process-wide webhook delivery service (None when disabled).

        Requires BOTH NATS and webhooks enabled: the delivery service is
        a NATS subscriber, so with NATS disabled there is nothing for it
        to consume. Uses the shared singleton so the per-chain daemon
        tasks never create duplicate NATS subscriptions.
        """
        if not self.nats_enabled or not self.webhook_enabled:
            return None
        if self._webhook_delivery is None:
            self._webhook_delivery = WebhookDelivery.shared()
        return self._webhook_delivery

    async def _maybe_start_webhook_delivery(self) -> None:
        """Best-effort start of the webhook delivery service.

        Called after the NATS fan-out each sync cycle. Idempotent (the
        delivery service guards its own start) and never raises — a dead
        NATS server logs a warning and the next cycle retries.
        """
        delivery = self.webhook_delivery
        if delivery is None:
            return
        try:
            await delivery.start()
        except Exception as exc:  # noqa: BLE001 — delivery is best-effort
            logger.warning("webhook_delivery_start_failed", chain=self.chain, error=str(exc)[:120])

    # ── NATS Real-Time Streaming (best-effort) ────────────────────

    async def _publish_nats_events(
        self,
        chain: str,
        block: int,
        transfers: list[dict[str, Any]],
        dex_events: list[dict[str, Any]],
        deployer_events: list[dict[str, Any]],
        trace_events: list[dict[str, Any]],
    ) -> int:
        """Fan this cycle's events out to NATS after ClickHouse writes.

        Best-effort by design: NATS is a real-time fan-out layer, never the
        durable store. Every failure path (disabled, unknown event type,
        connection error, open circuit) returns a count without raising,
        so NATS downtime can never block or abort the indexing loop. The
        per-event publisher owns retry/circuit-breaker semantics.

        Event routing:
          - transfer rows        → ``evm_transfer`` subject
          - DEX swap rows        → ``dex_swap`` subject
          - whale-flagged swaps  → ``whale_alert`` subject (in addition)
          - deployer rows        → ``deployer_event`` subject
          - internal trace rows  → ``evm_transfer`` subject (type field
            distinguishes ``internal_transfer`` on the wire)

        Returns:
            Number of events accepted by NATS (0 when disabled).
        """
        publisher = self.nats_publisher
        if publisher is None:
            return 0

        payloads = build_nats_payloads(transfers, dex_events, deployer_events, trace_events)

        if not payloads:
            return 0

        try:
            published = await publisher.stream_block_batch(chain, block, payloads)
        except Exception as exc:  # noqa: BLE001 — NATS is best-effort
            logger.warning("nats_batch_publish_failed", chain=chain, error=str(exc)[:120])
            return 0

        if published:
            logger.info(
                "nats_events_published",
                chain=chain,
                block=block,
                published=published,
                total=len(payloads),
            )
        return published

    # ── Vector Ingestion (LanceDB, optional) ─────────────────────────

    async def _run_vector_ingestion_guarded(
        self,
        from_block: int,
        to_block: int,
        summary: dict[str, Any],
    ) -> None:
        """Best-effort LanceDB vector ingestion for the synced block range.

        Never raises and never blocks indexing: when ingestion is disabled,
        the block interval has not elapsed, ClickHouse is unreachable, or the
        pipeline raises, we log and continue. Vector data is enrichment — the
        durable store is ClickHouse, and the sync loop must proceed even if
        embedding / LanceDB / ClickHouse hiccup.

        Args:
            from_block: First block of the synced range.
            to_block: Last block of the synced range.
            summary: Sync summary dict; ingestion stats are merged in on
                success (e.g. ``wallets_indexed`` / ``contracts_indexed``).
        """
        if not self.vector_ingestion_enabled:
            return
        try:
            stats = await self._maybe_run_vector_ingestion(from_block, to_block)
            if stats:
                summary.update(stats)
        except Exception as exc:  # noqa: BLE001 — ingestion must never block indexing
            logger.warning(
                "vector_ingestion_failed",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
                error=str(exc)[:150],
            )

    async def _maybe_run_vector_ingestion(
        self,
        from_block: int,
        to_block: int,
    ) -> dict[str, int] | None:
        """Run the ingestion pipeline when the block interval has elapsed.

        Delegates to :func:`lite_indexer.indexer._writers.maybe_run_vector_ingestion`,
        which reads the freshly synced ``[from_block, to_block]`` range from
        ClickHouse, embeds wallet/contract text via Qwen3-Embedding-4B
        and stores them in LanceDB collections. The
        first cycle always ingests; afterwards ingestion runs every
        ``VECTOR_INGESTION_INTERVAL_BLOCKS`` blocks. Best-effort — returns
        None when the interval has not elapsed or ClickHouse is unavailable.

        Args:
            from_block: First block of the synced range.
            to_block: Last block of the synced range.

        Returns:
            Stats dict (``wallets_indexed`` / ``contracts_indexed``) when
            ingestion ran, or None when skipped.
        """
        stats, store, last = await maybe_run_vector_ingestion(
            self.chain,
            self._vector_store,
            self.ch_writer,
            self._last_vector_ingestion_block,
            from_block,
            to_block,
        )
        if store is not None:
            self._vector_store = store
        if last is not None:
            self._last_vector_ingestion_block = last
        return stats

    # ── Block Range Determination ──────────────────────────────────
