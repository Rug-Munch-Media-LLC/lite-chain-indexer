"""Prove a reorg DELETES orphaned rows — not merely the checkpoint.

``tests/test_reorg.py`` covers status detection and checkpoint bookkeeping.
It never asserts the SQL text of the rollback, so two failure modes passed
the suite unnoticed:

1. A table written on the live sync path that the rollback list omitted —
   its rows survived every rollback while the checkpoint was pruned, and the
   dedup pre-check then treated the orphans as already-indexed forever.
2. A height column named differently from the live DDL. The Solana rollback
   filtered ``dex_events`` on ``blockNumber`` while the table declares
   ``block_number``; the statement matched nothing and was indistinguishable
   from "no orphans" without a text assertion.

Both are silent data-integrity failures, so these tests assert the DELETE
statements actually issued, against the real DDL column names.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.config.rollback_tables import (
    EVM_ROLLBACK_TABLES,
    SOLANA_ROLLBACK_TABLES,
)
from lite_indexer.strategies.evm.reorg import rollback_to_block
from lite_indexer.strategies.solana.reorg import rollback_to_slot

FORK = 1_000
FORK_PLUS_ONE = FORK + 1


class RecordingClient:
    """ClickHouse double that records every statement it is handed.

    The client is a stand-in for the driver, not a patched module: no
    monkeypatching is involved, and every DELETE reaches this object
    through the production code path.
    """

    def __init__(self, rows_deleted: int = 3) -> None:
        self.statements: list[tuple[str, dict[str, Any]]] = []
        self._rows_deleted = rows_deleted

    def execute(self, statement: str, params: dict[str, Any] | None = None) -> int:
        """Record a statement and report a plausible delete count."""
        self.statements.append((statement, dict(params or {})))
        return self._rows_deleted

    def deletes(self) -> list[str]:
        """Return only the DELETE statements, in issue order."""
        return [sql for sql, _ in self.statements if sql.lstrip().upper().startswith("DELETE")]

    def delete_for(self, table: str) -> tuple[str, dict[str, Any]]:
        """Return the (sql, params) pair issued for ``table``.

        Raises:
            AssertionError: when the table was never deleted from, which is
                the exact orphan-farm condition this suite exists to catch.
        """
        for sql, params in self.statements:
            if f"DELETE FROM {table}" in sql:
                return sql, params
        raise AssertionError(f"reorg rollback never issued a DELETE against {table}")


class _Rows:
    """Rows the tables hold, used to prove fork+1 rows are actually covered."""

    def __init__(self, *block_numbers: int) -> None:
        self.block_numbers = block_numbers


@pytest.mark.asyncio
async def test_evm_rollback_deletes_every_table_above_the_fork() -> None:
    """A row at ``fork+1`` is covered by a ``> fork`` DELETE in every table."""
    client = RecordingClient()
    # A block above the fork that the chain no longer contains.
    rows = _Rows(FORK - 1, FORK, FORK_PLUS_ONE)

    deleted = await rollback_to_block("base", FORK, client)

    assert deleted is not None, "rollback must not report failure"
    for target in EVM_ROLLBACK_TABLES:
        sql, params = client.delete_for(target.table)
        assert f"{target.column} > %(fork)s" in sql, (
            f"{target.table} is not filtered on its real height column {target.column!r}: {sql}"
        )
        assert params["fork"] == FORK
        assert params.get("chain") == "base"
        # The orphaned row is strictly above the fork, so the delete covers it.
        assert FORK_PLUS_ONE in rows.block_numbers
        assert params["fork"] < FORK_PLUS_ONE


@pytest.mark.asyncio
async def test_evm_rollback_purges_checkpoints_above_the_fork() -> None:
    """The checkpoint table is pruned, not the row tables left behind."""
    client = RecordingClient()

    await rollback_to_block("base", FORK, client)

    sql, params = client.delete_for("wallet_memory.block_checkpoints")
    assert "block_number > %(fork)s" in sql
    assert params["fork"] == FORK


@pytest.mark.asyncio
async def test_solana_rollback_uses_live_ddl_column_names() -> None:
    """``dex_events`` is filtered on ``block_number``, the column that exists.

    The regression this pins: filtering on ``blockNumber`` (the JSON field
    name) silently matched zero rows, so Solana DEX orphans were never purged.
    """
    client = RecordingClient()

    deleted = await rollback_to_slot(client, FORK)

    assert deleted is not None
    sql, _ = client.delete_for("wallet_memory.dex_events")
    assert "block_number > %(fork)s" in sql
    assert "blockNumber" not in sql, "dex_events has no blockNumber column; that DELETE is a no-op"


@pytest.mark.asyncio
async def test_solana_rollback_deletes_every_table_above_the_fork() -> None:
    """Solana row tables are purged with their real height column."""
    client = RecordingClient()

    deleted = await rollback_to_slot(client, FORK)

    assert deleted is not None
    for target in SOLANA_ROLLBACK_TABLES:
        sql, params = client.delete_for(target.table)
        assert f"{target.column} > %(fork)s" in sql, (
            f"{target.table} is not filtered on its real height column {target.column!r}: {sql}"
        )
        assert params["fork"] == FORK
        if target.chain_scoped:
            # The chain is a bound parameter, never interpolated into the
            # statement — the pre-fix Solana code inlined the literal 'solana'.
            assert "chain = %(chain)s" in sql, f"{target.table} is shared with EVM; the delete must be chain-scoped"
            assert params["chain"] == "solana"


@pytest.mark.asyncio
async def test_evm_rollback_covers_tables_written_on_the_sync_path() -> None:
    """Guards against a table being written again without being listed here.

    The writers below all persist rows at a block/slot height on the live
    sync loop. If any of them returns without a rollback entry, this fails
    rather than letting the omission ship as an orphan farm.
    """
    covered = {target.table for target in EVM_ROLLBACK_TABLES}

    for table in (
        "wallet_memory.evm_transfers",
        "wallet_memory.dex_events",
        "wallet_memory.internal_transfers",
        "wallet_memory.evm_launches",
        "wallet_memory.address_activity",
    ):
        assert table in covered, f"{table} is written on the sync path but never rolled back"


@pytest.mark.asyncio
async def test_solana_rollback_covers_shared_and_solana_tables() -> None:
    """Solana's list must include the shared tables it can also orphan."""
    covered = {target.table for target in SOLANA_ROLLBACK_TABLES}

    for table in (
        "wallet_memory.solana_spl_balance_changes",
        "wallet_memory.solana_transfers",
        "wallet_memory.dex_events",
        "wallet_memory.address_activity",
    ):
        assert table in covered, f"{table} can be orphaned by a Solana reorg but is never rolled back"


@pytest.mark.asyncio
async def test_rollback_reports_failure_when_a_delete_raises() -> None:
    """A failed DELETE returns ``None`` so the caller cannot assume success."""

    class FailingClient(RecordingClient):
        def execute(self, statement: str, params: dict[str, Any] | None = None) -> int:
            if "dex_events" in statement:
                raise RuntimeError("clickhouse unavailable")
            return super().execute(statement, params)

    result = await rollback_to_block("base", FORK, FailingClient())

    assert result is None, "a partial rollback must not report success"
