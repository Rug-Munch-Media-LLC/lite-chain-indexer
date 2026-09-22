"""Reorg-staging phase for the sync loop.

Extracted from ``_sync_loop.py`` (shrink-only size ratchet) — the 7b block
that checkpoints the batch tip and rolls back rows above a fork point.
Behaviour is unchanged: the caller returns the summary when this helper
returns a non-None result (abort/rollback), else continues.
"""

from __future__ import annotations

import time
from typing import Any

from lite_indexer import metrics
from lite_indexer.config import REORG_PROTECTION_ENABLED
from lite_indexer.logging import get_logger
from lite_indexer.strategies import EVMChainStrategy, SolanaStrategy
from lite_indexer.strategies.reorg_status import ReorgStatus

logger = get_logger(__name__)


class ReorgPhaseMixin:
    """7b reorg staging — mixed into ``LiteIndexer``."""

    async def _run_reorg_stage(
        self,
        from_block: int,
        to_block: int,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any] | None:
        """Checkpoint the batch tip and verify tip - depth against it.

        Returns the summary when the range must abort (unknown status) or
        roll back (fork detected); returns None to continue the cycle.
        """
        if not (REORG_PROTECTION_ENABLED and isinstance(self.strategy, (EVMChainStrategy, SolanaStrategy))):
            return None
        try:
            ch_client = self.ch_writer.client()
        except Exception as exc:  # noqa: BLE001 — staging is best-effort
            ch_client = None
            logger.debug("reorg_ch_client_failed", chain=self.chain, error=str(exc)[:80])
        if ch_client is None:
            return None

        outcome = await self.strategy.reorg_stage(from_block, to_block, ch_client)
        if outcome.status is ReorgStatus.UNKNOWN:
            # Fail CLOSED: unverified range → do NOT advance state.
            logger.error(
                "reorg_status_unknown",
                chain=self.chain,
                block=outcome.fork_block if outcome.fork_block is not None else to_block,
                error=outcome.error,
            )
            metrics.incr_labeled("lite_indexer_reorg_unknown", chain=self.chain)
            summary["status"] = "reorg_status_unknown"
            summary["errors"].append(f"reorg status unknown for range {from_block}-{to_block}: {outcome.error}")
            summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
            self._save_current_state()
            return summary
        if outcome.status is ReorgStatus.REORG and outcome.fork_block is not None:
            logger.warning("reorg_rollback_scheduled", chain=self.chain, fork_block=outcome.fork_block)
            summary["status"] = "reorg_rollback"
            summary["reorg_fork_block"] = outcome.fork_block
            # Re-fetch from fork: next cycle starts at fork_block.
            self.state.save(outcome.fork_block - 1, 0, enabled_events=self._enabled_event_types())
            summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
            return summary
        return None
