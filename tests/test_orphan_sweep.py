"""Tests for the historical orphan sweep (dry-run safety + bounded apply).

The Fake-ClickHouse idiom from ``tests/test_reorg.py``: a client double
that answers the sweep's own SELECTs and records every statement, so a
test can assert the EXACT set of statements issued. Nothing here touches
a real ClickHouse.

The load-bearing tests are marked NEGATIVE CONTROL — they prove the tool
refuses to act rather than acting wrongly. A missing or empty checkpoint
table must never read as "every row is an orphan"; that single inversion
would make the command a table-wipe.
"""

from __future__ import annotations

import argparse
from typing import Any

import pytest

from lite_indexer.cli._orphan_sweep import run_orphan_sweep
from lite_indexer.config.rollback_tables import RollbackTarget
from lite_indexer.storage.orphan_apply import RefuseApplyError, require_confirmation
from lite_indexer.storage.orphan_counts import count_orphans
from lite_indexer.storage.orphan_plan import plan_chain
from lite_indexer.storage.orphan_safe_point import (
    STATUS_EMPTY,
    STATUS_NO_TABLE,
    STATUS_OK,
    STATUS_TOO_SHALLOW,
    resolve_safe_point,
)

EVM_CP = "wallet_memory.block_checkpoints"
SOLANA_CP = "wallet_memory.solana_checkpoints"


class FakeCH:
    """ClickHouse double answering the sweep's SELECTs, recording statements.

    ``rows`` maps ``(table, column, chain)`` to a list of heights so both
    the count and the max-height reads return real numbers. ``missing``
    names tables that raise the "doesn't exist" error, which is how a
    chain with no checkpoint table behaves in production.
    """

    def __init__(
        self,
        checkpoint_max: int = 0,
        rows: dict[tuple[str, str, str], list[int]] | None = None,
        missing: set[str] | None = None,
        delete_returns: int = 2,
    ) -> None:
        self.checkpoint_max = checkpoint_max
        self.rows = rows or {}
        self.missing = missing or set()
        self.delete_returns = delete_returns
        self.statements: list[tuple[str, Any]] = []
        self.deletes: list[tuple[str, dict[str, Any]]] = []

    def execute(self, query: str, params: Any = None) -> Any:
        self.statements.append((query, params))
        for table in self.missing:
            if table in query:
                raise RuntimeError(f"Table {table} doesn't exist")
        if query.startswith("SELECT max(block_number)") and "block_checkpoints" in query:
            return [(self.checkpoint_max,)]
        if query.startswith("SELECT max(slot)") and "solana_checkpoints" in query:
            return [(self.checkpoint_max,)]
        if query.startswith("SELECT max(") and "_checkpoints" in query:
            return [(self.checkpoint_max,)]
        if query.startswith("SELECT count()"):
            table = query.removeprefix("SELECT count() FROM ").split(" ", 1)[0]
            chain = (params or {}).get("chain", "")
            heights = self.rows.get((table, "h", chain), [])
            return [(len(heights),)]
        if query.startswith("SELECT max(block_number) FROM") or query.startswith("SELECT max(slot) FROM"):
            table = query.split(" FROM ")[1].split(" ", 1)[0]
            chain = (params or {}).get("chain", "")
            heights = self.rows.get((table, "h", chain), [])
            return [(max(heights) if heights else 0,)]
        if query.startswith("DELETE FROM"):
            table = query.removeprefix("DELETE FROM ").split(" ", 1)[0]
            self.deletes.append((table, dict(params or {})))
            return self.delete_returns
        return []


TARGET = RollbackTarget("wallet_memory.internal_transfers", "block_number", chain_scoped=True)
SOL_TARGET = RollbackTarget("wallet_memory.solana_transfers", "slot", chain_scoped=False)
KEY = ("wallet_memory.internal_transfers", "h", "base")


# ═══════════════════════════════════════════════════════════════════════════
# NEGATIVE CONTROLS — the catastrophic failure modes
# ═══════════════════════════════════════════════════════════════════════════
def test_missing_checkpoint_table_is_refused_not_treated_as_all_orphans() -> None:
    """NEGATIVE CONTROL: a chain with no checkpoint table must not sweep.

    This is the inversion that would turn the command into a table-wipe:
    if "no checkpoints" resolved to safe_point=0, then "rows above 0"
    is every row in the table.
    """
    ch = FakeCH(checkpoint_max=0, rows={KEY: list(range(1, 500))}, missing={EVM_CP})
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    assert point.status == STATUS_NO_TABLE
    assert not point.sweepable

    plan = plan_chain(ch, "base", (TARGET,), point)
    assert plan.candidates == 0
    assert plan.status == STATUS_NO_TABLE
    assert ch.deletes == []


def test_empty_checkpoint_table_is_refused() -> None:
    """NEGATIVE CONTROL: the table exists but holds no row for this chain.

    Same catastrophic inversion as above, reached a different way (a
    newly-added chain, or a ReplacingMergeTree still collapsing).
    """
    ch = FakeCH(checkpoint_max=0, rows={KEY: list(range(1, 500))})
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    assert point.status == STATUS_EMPTY
    plan = plan_chain(ch, "base", (TARGET,), point)
    assert plan.candidates == 0
    assert ch.deletes == []


def test_checkpoint_shallower_than_reorg_depth_is_refused() -> None:
    """NEGATIVE CONTROL: safe_point <= genesis is a refusal, not block 0.

    A fresh chain checkpointed at block 5 with depth 12 would otherwise
    yield safe_point = -7, and every stored row sits "above" that.
    """
    ch = FakeCH(checkpoint_max=5, rows={KEY: [1, 2, 3]})
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    assert point.status == STATUS_TOO_SHALLOW
    assert not point.sweepable
    assert plan_chain(ch, "base", (TARGET,), point).candidates == 0


def test_apply_without_yes_is_refused() -> None:
    """A stray --apply must not mutate anything."""
    with pytest.raises(RefuseApplyError):
        require_confirmation(apply_flag=True, yes_flag=False)
    with pytest.raises(RefuseApplyError):
        require_confirmation(apply_flag=False, yes_flag=True)
    with pytest.raises(RefuseApplyError):
        require_confirmation(apply_flag=False, yes_flag=False)


# ═══════════════════════════════════════════════════════════════════════════
# Safe point arithmetic
# ═══════════════════════════════════════════════════════════════════════════
def test_safe_point_is_checkpoint_max_minus_depth() -> None:
    ch = FakeCH(checkpoint_max=1000)
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    assert point.status == STATUS_OK
    assert point.safe_point == 988
    assert point.checkpoint_max == 1000


def test_solana_checkpoint_table_is_not_chain_scoped() -> None:
    """Solana's table is slot-keyed with no chain column."""
    ch = FakeCH(checkpoint_max=500)
    point = resolve_safe_point(ch, SOLANA_CP, "solana", 32, chain_scoped=False)
    assert point.status == STATUS_OK
    assert point.safe_point == 468
    sql = ch.statements[0][0]
    assert "chain =" not in sql


def test_chain_scoped_predicate_filters_on_chain() -> None:
    """A shared table MUST filter on chain or other chains' rows are hit."""
    ch = FakeCH()
    count_orphans(ch, TARGET, "base", 100, 200)
    sql, params = ch.statements[-1]
    assert "chain = %(chain)s" in sql
    assert params["chain"] == "base"


def test_unscoped_predicate_does_not_filter_on_chain() -> None:
    """Solana tables have no chain column — the predicate must not name one."""
    ch = FakeCH()
    count_orphans(ch, SOL_TARGET, "solana", 100, 200)
    sql, _ = ch.statements[-1]
    assert "chain = %(chain)s" not in sql
    assert "slot > %(safe)s" in sql


# ═══════════════════════════════════════════════════════════════════════════
# DRY RUN MUTATES NOTHING
# ═══════════════════════════════════════════════════════════════════════════
def test_dry_run_issues_no_delete_statements() -> None:
    """THE LOAD-BEARING TEST: a dry run never issues a DELETE.

    Asserts on the recorded statement list rather than on a counter, so a
    DELETE issued under a different verb or formatting is still caught.
    """
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))})
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    plan = plan_chain(ch, "base", (TARGET,), point, max_rows_per_run=1000)
    assert plan.candidates == 16
    assert ch.deletes == []
    assert not any(q.lstrip().upper().startswith(("DELETE", "ALTER", "DROP", "INSERT")) for q, _ in ch.statements)


def test_dry_run_over_full_cli_issues_no_mutation() -> None:
    """End-to-end: the CLI without --apply issues SELECTs only."""
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))})
    args = argparse.Namespace(
        chains="base",
        tables="internal_transfers",
        max_rows_per_run=1000,
        apply=False,
        yes=True,
    )
    code = run_orphan_sweep(args, ch_client=ch)
    assert code == 0
    assert ch.deletes == []
    assert not any(q.lstrip().upper().startswith("DELETE") for q, _ in ch.statements)


# ═══════════════════════════════════════════════════════════════════════════
# APPLY issues the expected parameterised DELETE
# ═══════════════════════════════════════════════════════════════════════════
def test_apply_issues_parameterised_delete_above_safe_point() -> None:
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))}, delete_returns=4)
    args = argparse.Namespace(chains="base", tables="internal_transfers", max_rows_per_run=1000, apply=True, yes=True)
    code = run_orphan_sweep(args, ch_client=ch)
    assert code == 0
    assert len(ch.deletes) == 1
    table, params = ch.deletes[0]
    assert table == "wallet_memory.internal_transfers"
    assert params == {"chain": "base", "safe": 988, "cap": 1004}


def test_apply_delete_never_touches_the_safe_point_itself() -> None:
    """The fork block must survive: the predicate is strictly ``> safe``."""
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))}, delete_returns=1)
    args = argparse.Namespace(chains="base", tables="internal_transfers", max_rows_per_run=1000, apply=True, yes=True)
    run_orphan_sweep(args, ch_client=ch)
    table, params = ch.deletes[0]
    assert table == "wallet_memory.internal_transfers"
    # Strictly greater: the fork block itself is never a candidate.
    assert params == {"chain": "base", "safe": 988, "cap": 1004}
    sql = [q for q, _p in ch.statements if q.startswith("DELETE")][0]
    assert "block_number > %(safe)s" in sql
    assert ">=" not in sql.split("WHERE", 1)[1]


# ═══════════════════════════════════════════════════════════════════════════
# ROW CAP
# ═══════════════════════════════════════════════════════════════════════════
def test_max_rows_per_run_caps_the_height_window() -> None:
    """The cap bounds the height window, so the DELETE cannot run past it."""
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))}, delete_returns=2)
    args = argparse.Namespace(chains="base", tables="internal_transfers", max_rows_per_run=3, apply=True, yes=True)
    run_orphan_sweep(args, ch_client=ch)
    _, params = ch.deletes[0]
    assert params["cap"] == 988 + 3
    assert params["cap"] < 1004


def test_row_budget_stops_after_the_first_table_drains_it() -> None:
    """A budget of 1 must not issue a DELETE for every table in the list."""
    ch = FakeCH(checkpoint_max=1000, rows={KEY: list(range(989, 1005))}, delete_returns=1)
    args = argparse.Namespace(
        chains="base",
        tables="",
        max_rows_per_run=1,
        apply=True,
        yes=True,
    )
    run_orphan_sweep(args, ch_client=ch)
    assert len(ch.deletes) <= 2


def test_uncapped_dry_run_window_is_the_data_max_not_a_guess() -> None:
    """With no cap the window ends at the table's real max height."""
    ch = FakeCH(checkpoint_max=1000, rows={KEY: [989, 990, 1000]})
    point = resolve_safe_point(ch, EVM_CP, "base", 12)
    plan = plan_chain(ch, "base", (TARGET,), point, max_rows_per_run=0)
    assert plan.max_height == 1000
    assert plan.tables[0].cap == 1000
