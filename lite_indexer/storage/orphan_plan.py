"""Per-chain orphan plan — the read-only pass that decides what is a candidate.

Separated from the CLI so the whole decision can be exercised by tests
without an argparse namespace, and so the apply loop lives in a different
module from the reasoning that authorises it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lite_indexer.config.rollback_tables import RollbackTarget
from lite_indexer.logging import get_logger
from lite_indexer.storage.orphan_counts import count_orphans, max_height_above
from lite_indexer.storage.orphan_safe_point import STATUS_OK, SafePoint

logger = get_logger(__name__)


@dataclass(frozen=True)
class TablePlan:
    """One table's candidate count inside the window.

    ``cap`` is the window's inclusive upper bound — the height this run is
    allowed to touch. It equals the table's own highest stored height above
    the safe point for an uncapped dry run, and ``safe_point +
    max_rows_per_run`` when the operator bounded the run.

    Attributes:
        table: Fully-qualified table name.
    """

    table: str
    candidates: int
    cap: int
    deleted: int = 0
    applied: bool = False


@dataclass(frozen=True)
class ChainPlan:
    """The full per-chain verdict, including the reason for any refusal."""

    chain: str
    status: str
    reason: str
    safe_point: int
    checkpoint_max: int
    candidates: int = 0
    max_height: int = 0
    deleted: int = 0
    tables: list[TablePlan] = field(default_factory=list)

    @property
    def sweepable(self) -> bool:
        """True only when a DELETE was authorised for this chain."""
        return self.status == STATUS_OK


def _refusal(chain: str, point: SafePoint) -> ChainPlan:
    """Build a no-op plan for a chain whose safe point could not be established.

    The candidate count stays 0. This is the property that matters: an
    absent or empty checkpoint table must read as "nothing to do", never
    as "every row above zero is an orphan".
    """
    logger.info(
        "orphan_sweep_chain_refused",
        chain=chain,
        status=point.status,
        checkpoint_max=point.checkpoint_max,
        reason=point.reason,
    )
    return ChainPlan(
        chain=chain,
        status=point.status,
        reason=point.reason,
        safe_point=0,
        checkpoint_max=point.checkpoint_max,
    )


def plan_chain(
    ch_client: Any,
    chain: str,
    targets: tuple[RollbackTarget, ...],
    point: SafePoint,
    *,
    max_rows_per_run: int = 0,
) -> ChainPlan:
    """Count orphan candidates for one chain inside its bounded window.

    The window's upper bound is derived per table from the data itself
    (``max_height_above``) so an uncapped dry run still reports an exact
    count over the whole window rather than a guess, and a capped run
    reports the count it is actually going to be able to remove.

    Args:
        ch_client: ClickHouse client.
        chain: Chain to plan.
        targets: Tables to consider for this chain family.
        point: Resolved safe point. Only ``STATUS_OK`` is planned.
        max_rows_per_run: When > 0, each table's window stops at
            ``safe_point + max_rows_per_run``; otherwise the whole window
            above the safe point is counted.

    Returns:
        :class:`ChainPlan` — a refusal when ``point`` is not OK.
    """
    if not point.sweepable:
        return _refusal(chain, point)

    plans: list[TablePlan] = []
    total = 0
    highest = 0
    for target in targets:
        height = max_height_above(ch_client, target, chain, point.safe_point)
        if height <= point.safe_point:
            plans.append(TablePlan(table=target.table, candidates=0, cap=point.safe_point))
            continue
        cap = min(height, point.safe_point + max_rows_per_run) if max_rows_per_run > 0 else height
        candidates = count_orphans(ch_client, target, chain, point.safe_point, cap)
        plans.append(TablePlan(table=target.table, candidates=candidates, cap=cap))
        total += candidates
        highest = max(highest, height)
    return ChainPlan(
        chain=chain,
        status=point.status,
        reason=point.reason,
        safe_point=point.safe_point,
        checkpoint_max=point.checkpoint_max,
        candidates=total,
        max_height=highest,
        tables=plans,
    )


__all__ = ["ChainPlan", "TablePlan", "plan_chain"]
