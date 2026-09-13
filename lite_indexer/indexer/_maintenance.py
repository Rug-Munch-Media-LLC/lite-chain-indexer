"""LiteIndexer mixins — extracted from the former god-file (indexer/__init__.py).

Degod split 2026-08-23: each mixin holds one cohesive responsibility and is
mixed into LiteIndexer. Method names/behavior unchanged — tests validate.
"""

from __future__ import annotations

import gc  # noqa: F401 — used by maintenance mixin body
from typing import Any  # noqa: F401

from lite_indexer.config import (
    DATA_RECONCILIATION_ENABLED,
    DATA_RECONCILIATION_EVERY_N_CYCLES,
    DATA_RECONCILIATION_MAX_DISCREPANCY_PCT,
    GC_EVERY_N_CYCLES,
)
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.indexer._resource_guards import get_guarded_memory_mb
from lite_indexer.logging import get_logger

log = get_logger(__name__)
logger = log  # god-file bodies reference `logger`; keep both names


class MaintenanceMixin(IndexerMixinBase):
    "Memory gates, GC cycles, ClickHouse range reconciliation."

    # ── Memory / GC / Reconciliation housekeeping ───────────────────

    def _maybe_run_gc(self) -> None:
        """Force ``gc.collect()`` every ``GC_EVERY_N_CYCLES`` sync cycles.

        Python's cyclic GC does not collect aggressively under cgroup
        memory pressure; event dicts, bloom filters, and httpx response
        objects form reference cycles that a manual collect reclaims
        between cycles. Runs at the START of every sync cycle so even
        aborted cycles (disk cap, no RPC, CH down) still count.
        """
        self._cycles_since_gc += 1
        if self._cycles_since_gc >= GC_EVERY_N_CYCLES:
            self._cycles_since_gc = 0
            collected = gc.collect()
            if collected:
                logger.debug("gc_collected", chain=self.chain, objects=collected)

    def memory_gate_ok(self) -> bool:
        """Whether new ingestion may proceed given current RSS.

        Returns False (and logs) when the in-process memory watchdog
        sees RSS over ``MEMORY_HIGH_MB`` — the daemon then sleeps
        instead of starting a new sync cycle (pause, not crash).

        Returns:
            True when under the threshold (or the watchdog is disabled).
        """
        return self._memory_gate.ok()

    def guarded_memory_mb(self) -> int:
        """Current guarded memory (max of process RSS and cgroup charge).

        This is the metric the daemon's hard self-recycle ceiling
        (``MEMORY_RECYCLE_MB``) is compared against. Read fresh rather
        than from the gate cache so the recycle decision is current.

        Returns:
            Guarded memory in MiB (0 when unavailable).
        """
        return get_guarded_memory_mb()

    async def aclose(self) -> None:
        """Release long-lived RPC + strategy HTTP clients (idempotent).

        Called by the daemon on shutdown (and before a memory self-recycle
        exit) so sockets are not leaked to the restarted process.
        """
        strategy, self._strategy = self._strategy, None
        if strategy is not None:
            await strategy.aclose()
        await self.rpc.aclose()

    def _maybe_reconcile_range(
        self,
        from_block: int,
        to_block: int,
        expected: int,
        summary: dict[str, Any],
    ) -> None:
        """Periodically compare CH row count vs expected for a synced range.

        Every ``DATA_RECONCILIATION_EVERY_N_CYCLES`` cycles, count the
        rows ClickHouse actually holds for ``[from_block, to_block]`` for
        this chain and compare against the events fetched from RPC. A
        discrepancy above ``DATA_RECONCILIATION_MAX_DISCREPANCY_PCT``
        percent means the durable store is missing rows (partial write,
        missed chunk, CH hiccup) — log a warning and rewind the state
        file so the next cycle re-fetches (backfills) the range.

        Best-effort: CH unavailable or a query failure logs at debug and
        never blocks or aborts the sync.

        Args:
            from_block: Start of the synced range.
            to_block: End of the synced range.
            expected: Events fetched from RPC for the range.
            summary: Sync summary dict; reconciliation stats are merged.
        """
        if not DATA_RECONCILIATION_ENABLED or expected <= 0:
            return
        self._cycles_since_reconcile += 1
        if self._cycles_since_reconcile < DATA_RECONCILIATION_EVERY_N_CYCLES:
            return
        self._cycles_since_reconcile = 0

        from lite_indexer.storage.reconciliation import check_range_completeness

        try:
            client = self.ch_writer.client()
            if client is None:
                return
            outcome = check_range_completeness(
                self.chain,
                client,
                from_block,
                to_block,
                expected,
                chain_type=self._chain_type,
            )
        except Exception as exc:  # noqa: BLE001 — reconciliation is best-effort
            logger.debug("reconcile_count_failed", chain=self.chain, error=str(exc)[:80])
            return

        actual = outcome.get("actual", 0)
        discrepancy_pct = float(outcome.get("discrepancy_pct", 0.0))
        summary["reconcile_expected"] = expected
        summary["reconcile_actual"] = actual
        summary["reconcile_discrepancy_pct"] = round(discrepancy_pct, 2)

        if discrepancy_pct <= DATA_RECONCILIATION_MAX_DISCREPANCY_PCT:
            logger.info(
                "reconcile_range_ok",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
                expected=expected,
                actual=actual,
                discrepancy_pct=round(discrepancy_pct, 2),
            )
            return

        logger.warning(
            "reconcile_range_mismatch_backfill",
            chain=self.chain,
            from_block=from_block,
            to_block=to_block,
            expected=expected,
            actual=actual,
            discrepancy_pct=round(discrepancy_pct, 2),
        )
        summary["reconcile_backfill_triggered"] = True
        # Feed the freemium backfill queue with the known-missing range
        # (idempotent), then rewind state so the next cycle re-fetches.
        # High-water mark is NOT rewound — it stays monotonic.
        self._record_reconcile_rewind(from_block, to_block)
        self.state.save(from_block - 1, 0, enabled_events=self._enabled_event_types())
