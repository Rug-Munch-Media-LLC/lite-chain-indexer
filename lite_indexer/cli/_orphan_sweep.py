"""orphan-sweep CLI — historical orphan reconciliation for indexed tables.

Sibling CLI module (not added to the 701-line ``cli/__init__.py``, which is
shrink-only). Registered the way the lake family is: a
``add_orphan_sweep_parser(sub)`` call plus a dispatch branch.

WHAT AN ORPHAN IS. Before round 2 the reorg rollback only ever deleted
from two tables, so ``internal_transfers``, ``evm_launches``,
``address_activity`` and ``solana_transfers`` kept rows belonging to
blocks the chain reorganized away. Those rows (a) make wallet queries
return data for blocks that are no longer canonical and (b) poison the
dedup pre-check in ``storage/ch_transfers.py``, which asks ClickHouse
which transfer keys already exist — an orphan makes that answer wrong.

THE RULE, and why it cannot delete a live row. A row is a candidate only
when its height is strictly greater than the chain's safe point, where the
safe point is ``max(checkpointed block) - reorg depth``
(:func:`lite_indexer.storage.orphan_safe_point.resolve_safe_point`). The
checkpoint table is written only by a batch that ran to completion, so
``max`` is the highest height whose whole batch is known-durable; backing
off by a full reorg depth lands below the last height the live staging
path has already verified. A candidate therefore sits strictly above
everything the indexer has proven stable.

The rule is deliberately one-sided. It accepts false negatives — a row
orphaned by a reorg shallower than the safe point, or written by a batch
that never checkpointed at all, is NOT caught here. That is the intended
trade: a missed orphan is a stale row a later, more informed run can
remove, while a deleted live row is unrecoverable and also poisons the
dedup pre-check in the opposite direction.

THE CATASTROPHIC CASE, handled explicitly. If the checkpoint table is
missing, empty, shallower than the reorg depth, or unreadable, the chain
is REFUSED with a named status and a candidate count of exactly 0. It is
never treated as "safe point is 0, therefore everything is an orphan".
A DELETE is unreachable for a refused chain: the apply loop only iterates
plans whose status is OK.

DRY RUN IS THE DEFAULT. Without ``--apply`` this issues SELECTs only.
``--apply`` additionally requires ``--yes``, so a stray flag or a
copy-pasted command cannot mutate production.
"""

from __future__ import annotations

import argparse
import json
from typing import Any

from lite_indexer.cli._orphan_sweep_run import sweep_one
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.orphan_apply import RefuseApplyError, require_confirmation

logger = get_logger(__name__)

#: Hard ceiling on rows removed in a single run, regardless of what
#: ``--max-rows-per-run`` says. A typed number is not a safety property;
#: a ceiling the operator cannot exceed is.
ABSOLUTE_MAX_ROWS_PER_RUN = 5_000_000


def _add_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``orphan-sweep`` subcommand on the cli subparsers."""
    sweep = sub.add_parser(
        "orphan-sweep",
        help="Report (and optionally delete) rows orphaned by historical reorgs",
        description=(
            "Reconcile historical orphan rows left behind before the reorg "
            "rollback covered every indexed table. DRY RUN BY DEFAULT: "
            "reports per-chain, per-table candidate counts and mutates "
            "nothing. --apply deletes only above the safe point derived "
            "from the chain's checkpoint table, and additionally requires "
            "--yes. A chain with no/empty/unreadable checkpoint table is "
            "REFUSED, never swept."
        ),
    )
    sweep.add_argument(
        "--chains",
        type=str,
        required=True,
        help="Comma-separated chains to sweep (e.g. base,arbitrum,solana). No default: an "
        "all-chains default would sweep chains the operator has not looked at.",
    )
    sweep.add_argument(
        "--tables",
        type=str,
        default="",
        help="Comma-separated table short-names to restrict to (evm_transfers, dex_events, "
        "internal_transfers, evm_launches, address_activity). Default: every table in the "
        "chain's rollback list.",
    )
    sweep.add_argument(
        "--max-rows-per-run",
        type=int,
        default=100_000,
        help="Maximum rows to remove per run, per chain (default: 100000). Caps the height "
        f"window at safe_point + max_rows_per_run. Hard ceiling {ABSOLUTE_MAX_ROWS_PER_RUN}.",
    )
    sweep.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete the candidate rows. Without this flag the command only counts.",
    )
    sweep.add_argument(
        "--yes",
        action="store_true",
        help="Required confirmation for --apply. Both flags together are needed to mutate anything.",
    )


def add_orphan_sweep_parser(sub: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Public entrypoint used by ``cli/__init__.py`` to register the command."""
    _add_parser(sub)


def run_orphan_sweep(args: argparse.Namespace, ch_client: Any | None = None) -> int:
    """Run the orphan-sweep subcommand.

    Args:
        args: Parsed CLI args (chains, tables, max_rows_per_run, apply, yes).
        ch_client: ClickHouse client. When None, one is opened from
            :class:`ClickHouseWriter`; passing one in is how the tests
            drive the whole command without a database.

    Returns:
        Exit code: 0 = swept (or dry run) with no chain-level error,
        1 = ClickHouse unavailable, 2 = at least one chain refused or a
        DELETE failed, 3 = --apply without --yes.
    """
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    # Confirmation is required only when a mutation is requested. A dry
    # run is the default path and must never demand a flag to proceed.
    if args.apply:
        try:
            require_confirmation(args.apply, args.yes)
        except RefuseApplyError as exc:
            logger.error("orphan_sweep_apply_refused", error=str(exc)[:120])
            print(json.dumps({"status": "refused", "error": str(exc)}))  # noqa: T201
            return 3

    if ch_client is None:
        ch_client = ClickHouseWriter().client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    wanted = {t.strip() for t in args.tables.split(",") if t.strip()}
    max_rows = min(args.max_rows_per_run, ABSOLUTE_MAX_ROWS_PER_RUN)
    if max_rows <= 0:
        print(json.dumps({"status": "error", "error": "max_rows_per_run must be positive"}))  # noqa: T201
        return 2

    chains = [c.strip() for c in args.chains.split(",") if c.strip()]
    results = [sweep_one(ch_client, chain, wanted, max_rows, args.apply) for chain in chains]

    total_candidates = sum(r["candidates"] for r in results)
    total_deleted = sum(r["deleted"] for r in results)
    refused = [r["chain"] for r in results if r["status"] != "ok"]
    failures = len(refused)
    logger.info(
        "orphan_sweep_complete",
        apply=args.apply,
        chains=len(results),
        candidates=total_candidates,
        deleted=total_deleted,
        refused=refused,
    )
    metrics_incr("lite_indexer_orphan_sweep_runs")
    metrics_incr("lite_indexer_orphan_sweep_candidates", float(total_candidates))
    out = {
        "status": "partial" if failures else "ok",
        "apply": args.apply,
        "max_rows_per_run": max_rows,
        "candidates": total_candidates,
        "deleted": total_deleted,
        "chains": results,
    }
    print(json.dumps(out, default=str))  # noqa: T201
    return 2 if failures else 0


__all__ = ["add_orphan_sweep_parser", "run_orphan_sweep"]
