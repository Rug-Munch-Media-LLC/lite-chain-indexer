"""Reconciliation mixin — CH range verification + cooldown-throttled rewinds.

Extracted 2026-09-21 from ``indexer/_maintenance.py`` (structure gate: NEW >200).
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from lite_indexer import metrics
from lite_indexer.config import (
    DATA_DIR,
    DATA_RECONCILIATION_ENABLED,
    DATA_RECONCILIATION_EVERY_N_CYCLES,
    DATA_RECONCILIATION_MAX_DISCREPANCY_PCT,
    DATA_RECONCILIATION_REWIND_COOLDOWN_S,
)
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.logging import get_logger

log = get_logger(__name__)
logger = log


def rewind_cooldown_ok(chain: str, *, now: float | None = None, base_dir: Path | None = None) -> bool:
    """Whether this chain may be rewound now (per-chain cooldown).

    Returns False when the chain was rewound within
    ``DATA_RECONCILIATION_REWIND_COOLDOWN_S``. Fail-open: a write error
    never blocks recovery.
    """
    base = base_dir if base_dir is not None else DATA_DIR
    path = base / f"lite-indexer-reconcile-rewind-{chain}.ts"
    ts = time.time() if now is None else now
    try:
        last = float(path.read_text().strip())
    except (OSError, ValueError):
        last = None
    if last is not None and ts - last < DATA_RECONCILIATION_REWIND_COOLDOWN_S:
        return False
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(ts))
    except OSError:
        pass
    return True


class ReconcileMixin(IndexerMixinBase):
    "Periodic durable-store reconciliation; rewinds throttled per chain."

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

        from lite_indexer.indexer._reconcile_window import resolve_reconcile_window
        from lite_indexer.recovery import query_chain_stats
        from lite_indexer.storage.reconciliation import check_range_completeness

        # Resolve the window against the store's own durable max BEFORE
        # counting. The sync loop advances the head past this range before
        # calling us, so the caller's (from_block, to_block) is not by
        # itself a range the durable store can answer for — counting it
        # blind produced a 100% "mismatch" on every not-yet-flushed tail.
        try:
            client = self.ch_writer.client()
            if client is None:
                return
            stats = query_chain_stats(client, self.chain, chain_type=self._chain_type)
            durable_max = stats.get("max_block") if stats.get("ok") else None
            window = resolve_reconcile_window(from_block, to_block, durable_max)
            if not window["verifiable"]:
                logger.debug(
                    "reconcile_window_skipped",
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                    durable_max=durable_max,
                    reason=window["reason"],
                )
                summary["reconcile_skipped"] = window["reason"]
                return
            outcome = check_range_completeness(
                self.chain,
                client,
                window["from_block"],
                window["to_block"],
                expected,
                chain_type=self._chain_type,
            )
        except Exception as exc:  # noqa: BLE001 — reconciliation is best-effort
            logger.debug("reconcile_count_failed", chain=self.chain, error=str(exc)[:80])
            return

        actual = outcome.get("actual", 0)
        discrepancy_pct = float(outcome.get("discrepancy_pct", 0.0))
        win_from = int(window["from_block"])
        win_to = int(window["to_block"])
        summary["reconcile_expected"] = expected
        summary["reconcile_actual"] = actual
        summary["reconcile_discrepancy_pct"] = round(discrepancy_pct, 2)
        if window["clamped"]:
            summary["reconcile_window_clamped"] = True
        metrics.set_labeled_gauge(
            "lite_indexer_reconciliation_discrepancy_pct",
            round(discrepancy_pct, 2),
            chain=self.chain,
        )

        if discrepancy_pct <= DATA_RECONCILIATION_MAX_DISCREPANCY_PCT:
            logger.info(
                "reconcile_range_ok",
                chain=self.chain,
                from_block=win_from,
                to_block=win_to,
                expected=expected,
                actual=actual,
                discrepancy_pct=round(discrepancy_pct, 2),
            )
            return

        logger.warning(
            "reconcile_range_mismatch_backfill",
            chain=self.chain,
            from_block=win_from,
            to_block=win_to,
            expected=expected,
            actual=actual,
            discrepancy_pct=round(discrepancy_pct, 2),
        )
        summary["reconcile_backfill_triggered"] = True
        # Feed the freemium backfill queue with the known-missing range
        # (idempotent), then rewind state so the next cycle re-fetches.
        # High-water mark is NOT rewound — it stays monotonic.
        if not rewind_cooldown_ok(self.chain):
            logger.warning(
                "reconcile_rewind_throttled",
                chain=self.chain,
                from_block=win_from,
                to_block=win_to,
                discrepancy_pct=round(discrepancy_pct, 2),
                cooldown_s=DATA_RECONCILIATION_REWIND_COOLDOWN_S,
            )
            summary["reconcile_rewind_throttled"] = True
            return
        self._record_reconcile_rewind(win_from, win_to)
        self.state.save(win_from - 1, 0, enabled_events=self._enabled_event_types())
