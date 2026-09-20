"""Dex-phase outcome handling for the sync loop (split for size policy).

Extracted 2026-09-21 so _sync_loop.py stays within the structure ratchet while
the "dex leg deferred" semantics live on their own.
"""

from __future__ import annotations

from typing import Any

from lite_indexer import metrics
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_METRIC = "lite_indexer_dex_deferred"


def mark_dex_deferred(summary: dict[str, Any], *, chain: str) -> None:
    """Record a dex deferral visibly (non-fatal) + bump the metric."""
    summary["dex_deferred"] = True
    metrics.incr_labeled(_METRIC, chain=chain)


def note_dex_timeout(summary: dict[str, Any], *, chain: str, exc: BaseException) -> None:
    """PhaseTimeoutError path: log, record the phase failure, mark deferred."""
    logger.warning("sync_phase_failed", chain=chain, phase="dex_events", error=str(exc)[:120])
    summary.setdefault("phase_failures", []).append("dex_events")
    mark_dex_deferred(summary, chain=chain)


def resolve_dex_events(
    dex_events: list[dict[str, Any]] | None,
    summary: dict[str, Any],
    *,
    chain: str,
    from_block: int,
    to_block: int,
) -> list[dict[str, Any]]:
    """None (total dex-leg failure) -> [] + visible deferral; else pass through.

    Total failure is NOT the same as "no swaps in range": the degrade is
    recorded so the gap is detectable, but the slice stays non-fatal
    (the transfers leg already checkpointed).
    """
    if dex_events is None:
        logger.warning("dex_leg_deferred", chain=chain, from_block=from_block, to_block=to_block)
        mark_dex_deferred(summary, chain=chain)
        return []
    return dex_events
