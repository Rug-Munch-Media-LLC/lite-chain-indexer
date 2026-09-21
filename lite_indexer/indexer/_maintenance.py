"""LiteIndexer mixins — extracted from the former god-file (indexer/__init__.py).

Degod split 2026-08-23: each mixin holds one cohesive responsibility and is
mixed into LiteIndexer. Method names/behavior unchanged — tests validate.
"""

from __future__ import annotations

import gc  # noqa: F401 — used by maintenance mixin body
from typing import Any  # noqa: F401

from lite_indexer.config import (
    GC_EVERY_N_CYCLES,
)
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.indexer._reconcile import ReconcileMixin, rewind_cooldown_ok  # noqa: F401  (public re-export)
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
        await self.stop_head_pool()
        strategy, self._strategy = self._strategy, None
        if strategy is not None:
            await strategy.aclose()
        await self.rpc.aclose()
