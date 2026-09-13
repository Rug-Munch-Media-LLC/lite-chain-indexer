"""Sidecar services mixin — NATS publishing, webhooks, vector ingestion.

Split from indexer/__init__.py (de-godding 2026-08-23). All methods operate
on ``self`` attributes initialized by :class:`LiteIndexer.__init__`.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    BACKFILL_ENABLED,
    BACKFILL_INCLUDE_REWINDS,
    BACKFILL_MAX_CHUNKS_PER_CYCLE,
    BACKFILL_MAX_CONCURRENCY,
    BACKFILL_MAX_QUEUE_DEPTH,
    MEMORY_RECYCLE_MB,
)
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.indexer._writers import (
    build_nats_payloads,
    maybe_run_vector_ingestion,
)
from lite_indexer.indexer.backfill import (
    BackfillQueue,
    detect_gaps,
    drain_backfill_queue,
)
from lite_indexer.logging import get_logger
from lite_indexer.streaming.nats_publisher import NATSPublisher
from lite_indexer.webhooks.delivery import WebhookDelivery

logger = get_logger(__name__)


class SidecarsMixin(IndexerMixinBase):
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


class BackfillMixin(IndexerMixinBase):
    """Gap detection + freemium backfill sidecar, mixed into ``LiteIndexer``.

    The queue is disk-persisted (idempotent, bounded) and the drain is
    memory-gated so the sidecar never pushes the daemon toward its hard
    recycle ceiling. No executor is configured by default, so gaps are
    collected but not dispatched until a host wires one
    (:func:`lite_indexer.indexer.backfill_executor.build_evm_backfill_executor`).
    """

    def _get_backfill_queue(self) -> BackfillQueue:
        """Lazily build the persisted queue for this chain (idempotent)."""
        queue = self._backfill_queue
        if queue is None:
            queue = BackfillQueue(self.chain)
            self._backfill_queue = queue
        return queue

    def _record_reconcile_rewind(self, from_block: int, to_block: int) -> None:
        """Record a reconciliation rewind for the next gap enqueue (bounded)."""
        rewinds = self._reconcile_rewinds
        rewinds.append((int(from_block), int(to_block)))
        del rewinds[:-BACKFILL_MAX_QUEUE_DEPTH]

    def _enqueue_backfill_gaps(self, tip: int | None) -> int:
        """Detect un-indexed ranges and enqueue them (idempotent, bounded).

        Includes the forward gap only when this chain stalled (state did not
        advance since the previous cycle) so an actively-syncing chain is
        left to the live loop. Reconciliation rewinds are always enqueued.
        """
        if not BACKFILL_ENABLED:
            return 0
        state = self.state.load()
        try:
            last_block = int(state.get("last_block", 0) or 0)
        except (TypeError, ValueError):
            last_block = 0
        previous = self._backfill_seen_block
        stalled = previous is not None and last_block <= previous
        self._backfill_seen_block = last_block

        rewinds = self._reconcile_rewinds
        # Rewinds overlap ranges already indexed; only enrol them when the
        # storage engine dedups (ReplacingMergeTree), else they duplicate.
        rewind_ranges = list(rewinds) if BACKFILL_INCLUDE_REWINDS else []
        chunks = detect_gaps(
            self.chain,
            last_block,
            tip,
            rewind_ranges,
            self.chunk_size,
            BACKFILL_MAX_QUEUE_DEPTH,
            include_forward_gap=stalled,
        )
        rewinds.clear()
        return self._get_backfill_queue().enqueue(chunks)

    async def _run_backfill_sidecar(self) -> dict[str, int]:
        """Drain a bounded number of queued chunks (memory-gated).

        Disabled when no executor is configured: the queue still collects
        gaps for a later release/operator wiring.
        """
        if not BACKFILL_ENABLED:
            return {}
        executor = self._backfill_executor
        if executor is None:
            return {}

        mem_mb = self.guarded_memory_mb()
        memory_ok = self.memory_gate_ok() and not (MEMORY_RECYCLE_MB > 0 and mem_mb >= MEMORY_RECYCLE_MB)
        stats = await drain_backfill_queue(
            self._get_backfill_queue(),
            executor,
            max_concurrency=BACKFILL_MAX_CONCURRENCY,
            max_chunks=BACKFILL_MAX_CHUNKS_PER_CYCLE,
            memory_ok=memory_ok,
        )
        if stats.get("dispatched") or stats.get("memory_paused"):
            logger.info("backfill_sidecar", chain=self.chain, memory_mb=mem_mb, **stats)
        return stats

    # ── Block Range Determination ──────────────────────────────────
