"""Canonical per-chain table list for reorg row deletion.

A reorg is only handled correctly if EVERY table that can hold a row at a
reorgable height is deleted above the fork point. A table that is written
but missing from :data:`EVM_ROLLBACK_TABLES` / :data:`SOLANA_ROLLBACK_TABLES`
is an orphan farm: its rows survive the rollback, the dedup pre-check then
sees them as "already indexed" on re-extraction, and the range is silently
never repaired.

This module is the single source of that list for both reorg modules
(``strategies.evm.reorg`` and ``strategies.solana.reorg``) so the two
chains cannot drift apart. The ``column`` is the block-height column as it
exists in the live DDL — getting it wrong produces a DELETE that matches
zero rows, which is indistinguishable from "nothing was orphaned" unless a
test asserts the statement text.

Every table here is keyed by a height column and is written on the live
sync path (not only by backfill), so any of them can be orphaned:

* ``evm_transfers``    — ``strategies/evm`` transfer writer
* ``dex_events``       — DEX swap/creation writer, shared by EVM + Solana
* ``internal_transfers`` — trace writer (``storage/trace_store``)
* ``evm_launches``     — ``strategies/evm/launch.write_launch``
* ``address_activity`` — per-address tracing rows (chain-discriminated)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from lite_indexer.config.storage import (
    CH_ADDRESS_ACTIVITY_TABLE,
    CH_DEX_TABLE,
    CH_TABLE,
    CH_TRACE_TABLE,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class RollbackTarget:
    """One table to purge above a fork height.

    Attributes:
        table: Fully-qualified ClickHouse table name.
        column: Height column as named in the live DDL.
        chain_scoped: True when the table holds rows for several chains and
            the DELETE must therefore filter on ``chain``.
    """

    table: str
    column: str
    chain_scoped: bool


# EVM-family tables. All of these carry ``block_number`` and are keyed by it,
# so a single ``WHERE chain = ... AND block_number > fork`` covers them.
EVM_ROLLBACK_TABLES: tuple[RollbackTarget, ...] = (
    RollbackTarget(CH_TABLE, "block_number", chain_scoped=True),
    RollbackTarget(CH_DEX_TABLE, "block_number", chain_scoped=True),
    RollbackTarget(CH_TRACE_TABLE, "block_number", chain_scoped=True),
    # Declared in ``strategies.evm.launch`` with the same env override as
    # the writer, so a custom table name stays consistent between the two.
    RollbackTarget(
        os.environ.get("CH_EVM_LAUNCHES_TABLE", "wallet_memory.evm_launches"),
        "block_number",
        chain_scoped=True,
    ),
)

# ``address_activity`` is chain-discriminated by a ``chain`` column but
# stores its height in ``slot`` for every chain (see
# ``storage/address_activity_ddl`` and migration 016), so the column differs
# from the other EVM tables even though the height is a block number.
EVM_ROLLBACK_TABLES += (RollbackTarget(CH_ADDRESS_ACTIVITY_TABLE, "slot", chain_scoped=True),)

# Solana-family tables: ``solana_spl_balance_changes`` and
# ``solana_transfers`` are keyed by ``slot``; ``dex_events`` is shared with
# EVM and is therefore chain-scoped.
#
# ``solana_launches`` joined this list when the launchpad watcher's writer
# state started rewinding on a reorg
# (``strategies/solana/strategy/launchpad_rewind.py``, called from
# ``strategy/sync.py``): a rollback previously deleted nothing that a
# re-fetch rebuilt, because the watcher resumed past the fork. It does now —
# ``poll_launchpad_blocks`` re-scans the orphaned range and re-buffers the
# mints, so the rows are reconstructed. ``solana_bundles`` stays OUT: its
# evidence is not re-derivable from a partial signature walk, see
# ``strategies/solana/reorg_launch_reconstruct.py``.
SOLANA_ROLLBACK_TABLES: tuple[RollbackTarget, ...] = (
    RollbackTarget("wallet_memory.solana_spl_balance_changes", "slot", chain_scoped=False),
    RollbackTarget("wallet_memory.solana_transfers", "slot", chain_scoped=False),
    RollbackTarget("wallet_memory.solana_launches", "slot", chain_scoped=False),
    RollbackTarget(CH_DEX_TABLE, "block_number", chain_scoped=True),
    RollbackTarget(CH_ADDRESS_ACTIVITY_TABLE, "slot", chain_scoped=True),
)

# Tron-family tables. ``tron_launches`` was the orphan farm round 2 flagged for
# a decision: written by ``strategies/tron/launch.write_launch`` keyed on
# ``block_number`` (migrations/010_tron_launch.sql) with no rollback path at
# all. TRC20 transfers land in the SHARED ``evm_transfers`` table and Tron
# address-activity rows in the shared ``address_activity`` table (which stores
# every chain's height in ``slot`` — Tron block numbers included, see
# ``storage/address_activity_crosschain.parse_tron_address_activity``), so
# those two are chain-scoped here.
#
# ``internal_transfers`` / ``dex_events`` are deliberately ABSENT: the Tron
# strategy has no ``supports_dex`` path and no trace writer, so a Tron fork
# can never orphan their rows, and including them would make a Tron rollback
# delete other chains' shared rows if it ever false-positived.
TRON_ROLLBACK_TABLES: tuple[RollbackTarget, ...] = (
    RollbackTarget("wallet_memory.tron_launches", "block_number", chain_scoped=True),
    RollbackTarget(CH_TABLE, "block_number", chain_scoped=True),
    RollbackTarget(CH_ADDRESS_ACTIVITY_TABLE, "slot", chain_scoped=True),
)


def delete_above_fork(
    ch_client: Any,
    targets: tuple[RollbackTarget, ...],
    fork: int,
    *,
    chain: str | None = None,
) -> tuple[int, str | None]:
    """Delete every target's rows above ``fork``; stop at the first failure.

    Shared by both reorg modules so the EVM and Solana rollbacks cannot
    drift in how they build the predicate. The height column comes from
    each :class:`RollbackTarget` (the name in the live DDL) — never from a
    JSON field name, which would silently match zero rows.

    Args:
        ch_client: ClickHouse client.
        targets: Tables to purge, in delete order.
        fork: Fork height; rows strictly above it are orphaned.
        chain: Chain to filter on for chain-scoped tables. Required when any
            target is chain-scoped.

    Returns:
        ``(deleted_rows, failed_table)``. ``failed_table`` is ``None`` on
        full success; the caller reports failure rather than assuming the
        orphaned rows were removed.
    """
    deleted = 0
    for target in targets:
        if target.chain_scoped:
            where = f"chain = %(chain)s AND {target.column} > %(fork)s"
        else:
            where = f"{target.column} > %(fork)s"
        try:
            result = ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                f"DELETE FROM {target.table} WHERE {where}",
                {"chain": chain, "fork": fork},
            )
        except Exception as exc:  # noqa: BLE001 — report failure, do not pretend success
            logger.error(
                "reorg_rollback_table_failed",
                table=target.table,
                column=target.column,
                chain=chain,
                fork=fork,
                error=str(exc)[:120],
            )
            return deleted, target.table
        if isinstance(result, int):
            deleted += result
    return deleted, None


__all__ = [
    "EVM_ROLLBACK_TABLES",
    "RollbackTarget",
    "SOLANA_ROLLBACK_TABLES",
    "TRON_ROLLBACK_TABLES",
    "delete_above_fork",
]
