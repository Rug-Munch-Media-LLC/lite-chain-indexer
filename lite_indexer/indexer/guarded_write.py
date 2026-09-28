"""Reorg-checked write seam for every non-sync-loop ClickHouse write.

Three write paths reached ``strategy.write_clickhouse(...)`` directly with
no reorg checkpoint and no fork check: the EVM backfill executor, the TRON
backfill executor, and the lake import. The guard that prevents orphaned
rows lived in ``indexer/_reorg_phase.py`` and was gated to the main sync
loop, so a backfilled row could land on a block that was already
reorganized away — and the live rollback then deleted the backfill's GOOD
rows while leaving the orphaned ones behind.

This module is the single seam: checkpoint the tip, verify the deep block
against its checkpoint, and only then insert. When the strategy cannot
verify (TronGrid serves no block hash — a real protocol limitation, not a
bug), the write is refused rather than performed blind; callers treat a
refusal as "retry later", which is the correct behaviour for a
data-integrity guard and never loses data.
"""

from __future__ import annotations

from typing import Any, Protocol

from lite_indexer.logging import get_logger
from lite_indexer.strategies.reorg_status import ReorgStatus

logger = get_logger(__name__)


class GuardedStrategy(Protocol):
    """The strategy surface :func:`write_with_guard` depends on."""

    chain: str

    async def reorg_stage(
        self,
        from_block: int,
        to_block: int,
        ch_client: Any | None = None,
    ) -> Any: ...


async def write_with_guard(
    strategy: GuardedStrategy,
    ch_writer: Any,
    events: list[dict[str, Any]],
    from_block: int,
    to_block: int,
) -> int:
    """Checkpoint + fork-check a block range, then write its events.

    Args:
        strategy: Chain strategy (supplies ``reorg_stage`` + ``write_clickhouse``).
        ch_writer: ClickHouse writer handed through to the strategy.
        events: Parsed events to persist.
        from_block: Inclusive range start.
        to_block: Inclusive range end.

    Returns:
        Number of rows written, or 0 when the write was refused because the
        range could not be verified (the caller must retry, not advance).
    """
    if not events:
        return 0

    try:
        ch_client = ch_writer.client()
    except Exception as exc:  # noqa: BLE001 — a ClickHouse outage is a refuse, not a crash
        logger.warning(
            "guarded_write_client_unavailable",
            chain=strategy.chain,
            from_block=from_block,
            to_block=to_block,
            error=str(exc)[:80],
        )
        return 0

    if ch_client is None:
        logger.warning(
            "guarded_write_no_client",
            chain=strategy.chain,
            from_block=from_block,
            to_block=to_block,
        )
        return 0

    outcome = await strategy.reorg_stage(from_block, to_block, ch_client)
    if outcome.status is not ReorgStatus.NO_REORG:
        # UNKNOWN (cannot verify) and REORG (fork found) both refuse the
        # write. REORG means the rollback path already removed the
        # orphaned rows; re-inserting would resurrect them.
        logger.warning(
            "guarded_write_refused",
            chain=strategy.chain,
            status=str(outcome.status),
            fork_block=outcome.fork_block,
            from_block=from_block,
            to_block=to_block,
            error=outcome.error,
        )
        return 0

    return int(strategy.write_clickhouse(events, ch_writer) or 0)


__all__ = ["GuardedStrategy", "write_with_guard"]
