"""Per-chain execution for the orphan sweep.

Extracted from ``cli/_orphan_sweep.py`` when that module crossed the
200-line size ratchet. Holds the single-chain plan → apply → report step
so the CLI module keeps only argparse and the multi-chain loop.

Fail-closed contract, identical to a refused chain: every error path
here returns a report row whose ``candidates`` is 0. A chain that cannot
be planned is never reported as a chain with nothing to clean.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from lite_indexer.cli._orphan_tables import chain_family, target_for
from lite_indexer.logging import get_logger
from lite_indexer.storage.orphan_apply import delete_orphans
from lite_indexer.storage.orphan_plan import ChainPlan, plan_chain
from lite_indexer.storage.orphan_safe_point import resolve_safe_point

logger = get_logger(__name__)


def _apply_chain(plan: ChainPlan, ch_client: Any, max_rows: int) -> ChainPlan:
    """Delete a planned chain's candidate rows inside its per-table budget.

    Walks the plan's tables in order, spending ``max_rows`` across them and
    stopping once the budget is exhausted, so a runaway backlog drains over
    several runs instead of in one unbounded statement.

    Args:
        plan: A sweepable plan (status OK).
        ch_client: ClickHouse client.
        max_rows: Total row budget for this chain in this run.

    Returns:
        A new :class:`ChainPlan` carrying per-table and total deletions.
    """
    budget = max_rows
    tables = []
    deleted_total = 0
    for entry in plan.tables:
        if budget <= 0 or entry.candidates <= 0:
            tables.append(replace(entry, deleted=0, applied=False))
            continue
        deleted = delete_orphans(ch_client, target_for(entry.table), plan.chain, plan.safe_point, entry.cap)
        budget -= deleted
        deleted_total += deleted
        tables.append(replace(entry, deleted=deleted, applied=deleted > 0))
    return replace(plan, deleted=deleted_total, tables=tables)


def _report(plan: ChainPlan, apply_flag: bool) -> dict[str, Any]:
    """Render one chain's plan as a JSON-serialisable report row."""
    return {
        "chain": plan.chain,
        "status": plan.status,
        "safe_point": plan.safe_point,
        "checkpoint_max": plan.checkpoint_max,
        "max_height_above_safe": plan.max_height,
        "candidates": plan.candidates,
        "deleted": plan.deleted,
        "applied": apply_flag and plan.deleted > 0,
        "reason": plan.reason,
        "tables": [
            {"table": t.table, "candidates": t.candidates, "cap": t.cap, "deleted": t.deleted} for t in plan.tables
        ],
    }


def sweep_one(ch_client: Any, chain: str, wanted: set[str], max_rows: int, apply_flag: bool) -> dict[str, Any]:
    """Plan (and optionally apply) a single chain; never raises.

    Args:
        ch_client: ClickHouse client.
        chain: Chain to sweep.
        wanted: Optional set of table short-names to restrict to.
        max_rows: Row budget per run for this chain.
        apply_flag: True when the operator passed ``--apply --yes``.

    Returns:
        A report row. On any error, a ``chain_failed`` row with
        ``candidates`` 0 — never a partial or optimistic count.
    """
    try:
        from lite_indexer.config.finality import reorg_depth

        _, targets, checkpoint_table, chain_scoped = chain_family(chain)
        if wanted:
            targets = tuple(t for t in targets if t.table.rsplit(".", 1)[-1] in wanted)
        point = resolve_safe_point(
            ch_client,
            checkpoint_table,
            chain,
            reorg_depth(chain),
            chain_scoped=chain_scoped,
        )
        plan = plan_chain(ch_client, chain, targets, point, max_rows_per_run=max_rows)
        if apply_flag and plan.sweepable:
            plan = _apply_chain(plan, ch_client, max_rows)
        return _report(plan, apply_flag)
    except Exception as exc:  # noqa: BLE001 — one bad chain must not abort the run
        logger.error("orphan_sweep_chain_failed", chain=chain, error=str(exc)[:120])
        return {
            "chain": chain,
            "status": "chain_failed",
            "safe_point": 0,
            "checkpoint_max": 0,
            "max_height_above_safe": 0,
            "candidates": 0,
            "deleted": 0,
            "applied": False,
            "reason": str(exc)[:120],
            "tables": [],
        }


__all__ = ["sweep_one"]
