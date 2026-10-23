"""Bounded internal-transfer trace phase (per-chain budget).

Kept out of the sync loop and the RPC mixin so a chain that cannot serve
traces cheaply (see :mod:`lite_indexer.traces.policy`) pays a small,
predictable slice cost instead of the global 300 s phase timeout.
"""

from __future__ import annotations

import asyncio
from typing import Any

from lite_indexer import metrics
from lite_indexer.logging import get_logger
from lite_indexer.traces.policy import trace_policy

logger = get_logger(__name__)


async def run_trace_phase(
    host: Any,
    summary: dict[str, Any],
    from_block: int,
    to_block: int,
) -> list[dict[str, Any]]:
    """Run the internal-transfer trace phase under the chain's budget.

    Timeout is non-fatal: the transfers leg has already checkpointed this
    slice, so the phase records a failure and returns no rows.

    Args:
        host: Indexer providing ``chain`` and ``_collect_internal_transfers``.
        summary: Sync summary for the slice (``phase_failures`` is appended).
        from_block: First block of the slice.
        to_block: Last block of the slice.

    Returns:
        Standardized internal-transfer rows (empty when the phase failed).
    """
    policy = trace_policy(host.chain)
    try:
        return await asyncio.wait_for(
            host._collect_internal_transfers(from_block, to_block),
            timeout=policy.phase_timeout_s,
        )
    except TimeoutError:
        error = f"internal_transfers fetch timed out for {from_block}-{to_block}"
        logger.warning(
            "sync_phase_timeout",
            chain=host.chain,
            phase="internal_transfers",
            from_block=from_block,
            to_block=to_block,
            timeout_s=policy.phase_timeout_s,
        )
        metrics.incr_labeled("lite_indexer_rpc_failures", chain=host.chain)
        logger.warning("sync_phase_failed", chain=host.chain, phase="internal_transfers", error=error)
        summary.setdefault("phase_failures", []).append("internal_transfers")
        return []


async def collect_internal_transfers(chain: str, from_block: int, to_block: int) -> list[dict[str, Any]]:
    """Sweep a chain's trace budget worth of blocks for internal transfers.

    Args:
        chain: Chain name.
        from_block: First block of the slice.
        to_block: Last block of the slice (sweep starts here and walks back).

    Returns:
        Standardized internal-transfer rows (possibly empty).
    """
    from lite_indexer.traces.endpoints import has_trace_endpoints
    from lite_indexer.traces.trace_collector import TraceCollector

    if not has_trace_endpoints(chain):
        return []
    policy = trace_policy(chain)
    max_blocks = min(policy.max_blocks_per_cycle, to_block - from_block + 1)
    if max_blocks <= 0:
        return []
    collector = TraceCollector(chain)
    try:
        return await collector.collect_range(
            chain,
            to_block - max_blocks + 1,
            to_block,
            concurrency=policy.concurrency,
            max_blocks=max_blocks,
        )
    except Exception as exc:  # noqa: BLE001 — trace collection is best-effort
        logger.warning(
            "trace_collection_failed",
            chain=chain,
            from_block=from_block,
            to_block=to_block,
            error=str(exc)[:120],
        )
        return []
