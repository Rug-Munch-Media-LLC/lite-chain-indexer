"""Tron reorg staging seam — mixed into :class:`TronStrategy`.

Extracted into its own module because ``strategy.py`` sits at 449 of its
458-line shrink-only baseline: the staging logic is ~50 lines and had to
live somewhere, and the established pattern for exactly this situation is
a mixin sibling (``strategies/address_activity.py`` for the cross-chain
buffering, ``indexer/_reorg_phase.py`` for the sync-loop staging).

Shape mirrors ``SolanaStrategy.reorg_stage`` one-for-one — checkpoint the
batch tip's ``blockID``, verify the now-deep block against its checkpoint,
and on a real fork roll ``TRON_ROLLBACK_TABLES`` back above the fork. The
Verdict semantics are the shared ones (:mod:`strategies.reorg_status`):

* ``NO_REORG``  — checkpoint matched (or there was nothing to compare).
* ``REORG``     — ``blockID`` mismatch; the rollback already deleted the
  orphaned rows, so the caller must NOT re-insert them.
* ``UNKNOWN``   — could not verify. The caller refuses to advance, which is
  the fail-CLOSED behaviour: it must retry, never skip the range.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import REORG_PROTECTION_ENABLED
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.strategies.evm.reorg import checkpoint_block, ensure_checkpoint_table
from lite_indexer.strategies.reorg_status import ReorgCheck, ReorgStatus
from lite_indexer.strategies.tron.reorg import check_reorg, rollback_to_block

logger = get_logger(__name__)


class TronReorgStageMixin:
    """blockID checkpointing + rollback for the Tron strategy.

    The concrete strategy supplies ``chain``, ``_get_client()`` (its shared
    ``httpx.AsyncClient``) and inherits ``AbstractChainStrategy``; everything
    else is shared. ``chain`` is declared rather than injected so the
    attribute is visible to the type checker on this mixin alone.
    """

    chain: str

    async def reorg_stage(
        self,
        from_block: int,
        to_block: int,
        ch_client: Any | None = None,
    ) -> ReorgCheck:
        """Checkpoint the batch tip and roll back deep forks for a batch.

        Args:
            from_block: Start block of the processed batch (unused — kept
                for signature parity with the EVM/Solana paths).
            to_block: Tip block of the processed batch (checkpointed).
            ch_client: ClickHouse client (raw, from ClickHouseWriter).

        Returns:
            :class:`ReorgCheck` — REORG with ``fork_block`` once the
            orphaned rows are deleted, UNKNOWN when the range could not be
            verified or the rollback failed, NO_REORG otherwise.
        """
        if not REORG_PROTECTION_ENABLED or ch_client is None:
            return ReorgCheck(ReorgStatus.NO_REORG)

        if not await ensure_checkpoint_table(ch_client):
            return ReorgCheck(ReorgStatus.UNKNOWN, error="checkpoint table unavailable")

        client = await self._get_client()  # type: ignore[attr-defined]

        # 1. Checkpoint the batch tip's blockID.
        tip_block_id = await _tip_block_id(client, to_block)
        if not tip_block_id:
            logger.debug("tron_tip_block_id_missing", chain=self.chain, block=to_block)
        elif not await checkpoint_block(self.chain, to_block, tip_block_id, ch_client):
            logger.debug("tron_tip_checkpoint_failed", chain=self.chain, block=to_block)

        # 2. Verify the block that should now be deep-confirmed.
        check = await check_reorg(client, to_block, ch_client)
        if check.status is ReorgStatus.UNKNOWN:
            if check.error is None:
                # Benign: no checkpoint for that height yet — nothing to
                # compare, so do NOT stall catch-up.
                return ReorgCheck(ReorgStatus.NO_REORG)
            logger.error("reorg_status_unknown", chain=self.chain, block=to_block, error=check.error)
            return check
        if check.status is ReorgStatus.NO_REORG:
            return check

        # 3. Fork confirmed — purge the orphaned rows, then report the fork.
        assert check.fork_block is not None  # REORG always carries the fork
        logger.warning("chain_reorg_deep", chain=self.chain, fork_block=check.fork_block)
        deleted = await rollback_to_block(ch_client, check.fork_block)
        if deleted is None:
            metrics_incr("reorg_rollback_errors")
            return ReorgCheck(ReorgStatus.UNKNOWN, fork_block=check.fork_block, error="rollback failed")
        return check


async def _tip_block_id(client: Any, to_block: int) -> str | None:
    """Resolve a block height's ``blockID`` for checkpointing."""
    from lite_indexer.strategies.tron.reorg import fetch_block_id

    return await fetch_block_id(client, to_block)


__all__ = ["TronReorgStageMixin"]
