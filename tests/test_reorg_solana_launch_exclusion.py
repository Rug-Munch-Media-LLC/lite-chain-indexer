"""Pin the Solana launch/bundle reorg-EXCLUSION decision, and prove it is live.

Round 2 left ``solana_launches`` and ``solana_bundles`` out of
``SOLANA_ROLLBACK_TABLES`` because nobody had proven their writers re-fire on
a reorged range. This suite answers that question and pins the answer so it
cannot drift silently:

1. THE DDL CROSS-CHECK. Each excluded table's height column is read out of
   the migration SQL that creates it and compared to the column the decision
   records — so a rename in the DDL fails here instead of producing a future
   rollback DELETE that matches zero rows (the round-2 ``blockNumber`` /
   ``block_number`` defect).
2. THE NON-ROLLBACK PIN. Neither table may appear in the rollback list.
   Reverting the decision to "roll them back" fails this.
3. THE NO-REPLAY PROOF. The in-memory writer state that gates a re-fetch is
   asserted to be non-rewinding. This is the pin that actually bites: if a
   future change makes the launchpad cursor rewind on reorg, the exclusion's
   justification disappears and the test fails, demanding the decision be
   revisited.

Reorg-specific; the EVM/Tron reorg tests live in the sibling files.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from lite_indexer.config.rollback_tables import SOLANA_ROLLBACK_TABLES
from lite_indexer.strategies.solana.reorg_launch_reconstruct import (
    SOLANA_BUNDLES_TABLE,
    SOLANA_LAUNCHES_TABLE,
    SOLANA_REORG_LAUNCH_EXCLUSIONS,
    ReorgLaunchWriter,
    describe_reorg_exclusion,
    reorg_launch_writers,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_TABLES = (SOLANA_LAUNCHES_TABLE, SOLANA_BUNDLES_TABLE)

# Strategy modules holding the launchpad writer state, and the cursor itself.
STRATEGY_INIT = "lite_indexer/strategies/solana/strategy/__init__.py"
STRATEGY_SYNC = "lite_indexer/strategies/solana/strategy/sync.py"
LAUNCHPAD_CURSOR = "_launchpad_last_slot"

# The migration that creates each excluded table, and the height column the
# rollback would have to filter on.
DDL_SOURCE: dict[str, tuple[str, str]] = {
    SOLANA_LAUNCHES_TABLE: ("migrations/009_solana_launch.sql", "slot"),
    SOLANA_BUNDLES_TABLE: ("migrations/011_jito_bundles.sql", "slot"),
}

_COLUMN_DECL = re.compile(r"^\s{4}(\w+)\s+\w")


def _balanced_body(text: str, start: int) -> tuple[str, int]:
    """Return the CREATE TABLE body opening at ``start``, nesting-aware.

    A naive ``text.index(")")`` stops at the ``CODEC(ZSTD(3))`` and
    ``DEFAULT []`` parens, so the scan must count depth.

    Args:
        text: Full migration text.
        start: Index just past the opening paren.

    Returns:
        ``(body, end)`` where ``body`` excludes the enclosing parens.
    """
    depth = 1
    index = start
    while index < len(text) and depth:
        char = text[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        index += 1
    return text[start : index - 1], index


def _declared_columns(rel_path: str, table: str) -> list[str]:
    """Return the column names the migration declares for ``table``.

    Parses the CREATE TABLE body rather than matching the whole file, so a
    sibling table in the same migration cannot supply a false column.

    Args:
        rel_path: Migration path relative to the repo root.
        table: Fully-qualified table whose body should be read.

    Returns:
        Column names in declaration order.

    Raises:
        AssertionError: When the CREATE TABLE body cannot be located.
    """
    text = (REPO_ROOT / rel_path).read_text(encoding="utf-8")
    marker = f"CREATE TABLE IF NOT EXISTS {table} ("
    start = text.index(marker) + len(marker)
    body, end = _balanced_body(text, start)
    columns: list[str] = []
    for line in body.splitlines():
        match = _COLUMN_DECL.match(line)
        if match and not match.group(1).upper().startswith(("TTL", "ENGINE", "ORDER", "PARTITION", "SETTINGS")):
            columns.append(match.group(1))
    assert columns, f"{rel_path} declares no columns for {table}"
    assert end > start
    return columns


def _assigning_functions(relative_path: str, attribute: str) -> set[str]:
    """Return the names of every function in a module that assigns ``attribute``.

    Scans function bodies (not the module) so a rewind added to the reorg
    handler is caught, while ``__init__`` initialisation and the watcher's
    forward advance are still expected. Handles annotated assignments
    (``self.x: int = 0``) as well as plain ones.

    Args:
        relative_path: Module path relative to the repo root.
        attribute: The attribute being assigned.

    Returns:
        Names of the functions containing such an assignment.
    """
    tree = ast.parse((REPO_ROOT / relative_path).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for sub in ast.walk(node):
            target = sub.target if isinstance(sub, ast.AnnAssign) else getattr(sub, "targets", [None])[0]
            if isinstance(target, ast.Attribute) and target.attr == attribute:
                names.add(node.name)
    return names


def test_excluded_height_column_matches_the_migration_ddl() -> None:
    """The recorded height column exists in the CREATE TABLE that makes it.

    This is the round-2 defect, prevented for these two tables: a rollback
    filtered on a column the DDL does not declare matches zero rows and looks
    exactly like "nothing was orphaned".
    """
    for table, (rel_path, expected) in DDL_SOURCE.items():
        columns = _declared_columns(rel_path, table)
        exclusion = describe_reorg_exclusion(table)

        assert expected in columns, f"{table} lost its height column {expected!r} in {rel_path}: {columns}"
        assert exclusion.height_column == expected, (
            f"{table} records height column {exclusion.height_column!r} but the DDL declares {expected!r}"
        )


def test_excluded_tables_are_not_in_the_rollback_list() -> None:
    """Neither table is purged by a Solana reorg, by decision.

    Rollback deletes what the next re-fetch rebuilds. These two are not
    rebuilt (see the writer-state pin below), so purging them would delete
    rows nothing ever recreates.
    """
    covered = {target.table for target in SOLANA_ROLLBACK_TABLES}

    for table in EXCLUDED_TABLES:
        assert table not in covered, (
            f"{table} is in the rollback list, but its writer state is not rewound on a reorg — "
            "a rollback would delete rows no re-fetch rebuilds"
        )


def test_every_excluded_table_has_a_non_rewinding_writer() -> None:
    """Each exclusion is backed by writer state a reorg does not rewind.

    The pin that bites: if a reorg starts rewinding the launchpad cursor, the
    exclusions lose their justification and this fails, forcing the decision
    to be revisited rather than left to rot.
    """
    for exclusion in SOLANA_REORG_LAUNCH_EXCLUSIONS:
        owners = [w for w in reorg_launch_writers if w.owner == exclusion.writer_owner]

        assert owners, f"{exclusion.table} claims {exclusion.reason} but records no writer state"
        assert not any(w.reset_on_reorg for w in owners), (
            f"{exclusion.table} is excluded as {exclusion.reason!r}, but its writer state now rewinds on "
            "reorg — the re-fetch WOULD rebuild the rows and the exclusion must be re-decided"
        )


def test_launchpad_cursor_is_written_only_by_init_and_the_watcher() -> None:
    """``_launchpad_last_slot`` is only written by ``__init__`` and the watcher.

    The pin that bites. A reorg must not rewind this cursor: the post-rollback
    re-fetch restarts at ``_launchpad_last_slot + 1``, which is already past the
    fork, so the orphaned launch range is skipped rather than replayed. If a
    rewind ever appears here, ``solana_launches`` and ``solana_bundles`` become
    rollback-eligible and this fails to demand the decision be revisited.
    """
    assigners = _assigning_functions(STRATEGY_INIT, LAUNCHPAD_CURSOR)
    assigners |= _assigning_functions(STRATEGY_SYNC, LAUNCHPAD_CURSOR)
    expected = {"__init__", "poll_launchpad_blocks"}

    assert assigners == expected, (
        f"{LAUNCHPAD_CURSOR} is now assigned in {sorted(assigners - expected)} — if a reorg rewinds it, "
        "the solana_launches/solana_bundles exclusion loses its justification"
    )


@pytest.mark.parametrize("writer", reorg_launch_writers, ids=lambda w: w.attribute)
def test_writer_state_owner_module_exists(writer: ReorgLaunchWriter) -> None:
    """Recorded writer owners point at modules that still exist."""
    path = REPO_ROOT / (writer.owner.replace(".", "/") + ".py")

    assert path.exists(), f"recorded writer owner {writer.owner} no longer exists at {path}"
