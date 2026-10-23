"""Sync-phase guards (RC-2 + RC-3, 2026-09-19 audit).

RC-2: every RPC fetch phase of the sync cycle is time-bound — a hung
dex/pair/trace fetch previously stalled the chain's cycle indefinitely
(only the transfers leg had a timeout).

RC-3: zero-write stall breaker — see :class:`ZeroWriteTracker`.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

from lite_indexer import metrics
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

RPC_PHASE_TIMEOUT_S = 300.0


class PhaseTimeoutError(Exception):
    """A sync phase exceeded ``RPC_PHASE_TIMEOUT_S``."""

    def __init__(self, phase: str, from_block: int, to_block: int) -> None:
        super().__init__(f"{phase} fetch timed out for {from_block}-{to_block}")
        self.phase = phase
        self.from_block = from_block
        self.to_block = to_block


async def guarded_phase(
    phase: str,
    coro: Any,
    *,
    chain: str,
    from_block: int,
    to_block: int,
) -> Any:
    """Await a sync fetch phase under the phase timeout.

    Raises :class:`PhaseTimeoutError` after logging ``sync_phase_timeout`` and
    bumping the rpc-failure metric — the caller must not advance state.
    """
    try:
        return await asyncio.wait_for(coro, timeout=RPC_PHASE_TIMEOUT_S)
    except TimeoutError:
        logger.warning(
            "sync_phase_timeout",
            chain=chain,
            phase=phase,
            from_block=from_block,
            to_block=to_block,
            timeout_s=RPC_PHASE_TIMEOUT_S,
        )
        metrics.incr_labeled("lite_indexer_rpc_failures", chain=chain)
        raise PhaseTimeoutError(phase, from_block, to_block) from None


def defer_stalled_range(
    host: Any,
    block_range: tuple[int, int],
    summary: dict[str, Any],
    start_time: float,
    phase: str,
) -> dict[str, Any]:
    """Break the zero-write replay loop (RC-3): enqueue + advance.

    Called after MAX_CONSECUTIVE_ZERO_WRITES consecutive zero-write
    failures for the SAME range. The range is enqueued into the persistent
    backfill queue (which re-attempts the data) and state advances past it
    so the live loop is not pinned forever. ``host`` must provide chain,
    state, _highwater, _get_backfill_queue, _clear_reorg_recovery,
    _enabled_event_types, _save_current_state.
    """
    from_block, to_block = block_range
    metrics.incr_labeled("lite_indexer_writer_stalled", chain=host.chain)
    try:
        from lite_indexer.indexer.backfill import BackfillChunk

        host._get_backfill_queue().enqueue([BackfillChunk(chain=host.chain, from_block=from_block, to_block=to_block)])
    except Exception as exc:  # noqa: BLE001 — enqueue failure must not crash the loop
        logger.error("writer_stalled_enqueue_failed", chain=host.chain, error=str(exc)[:120])
    logger.warning(
        "sync_range_deferred_to_backfill",
        chain=host.chain,
        from_block=from_block,
        to_block=to_block,
        phase=phase,
    )
    host._clear_reorg_recovery()
    host.state.save(to_block, 0, enabled_events=host._enabled_event_types())
    host._highwater.update(to_block)
    summary["status"] = "writer_stalled_deferred"
    summary["deferred_phase"] = phase
    summary["duration_seconds"] = time.monotonic() - start_time
    host._save_current_state()
    return summary


def phase_failed(host: Any, summary: dict[str, Any], start_time: float, exc: Exception) -> dict[str, Any]:
    """Mark the cycle fetch-failed for a timed-out phase (no state advance)."""
    summary["status"] = "fetch_failed"
    summary["errors"].append(str(exc))
    summary["duration_seconds"] = time.monotonic() - start_time
    host._save_current_state()
    return summary
