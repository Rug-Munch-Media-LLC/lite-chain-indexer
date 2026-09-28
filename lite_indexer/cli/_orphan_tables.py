"""Chain-family resolution for the orphan sweep.

Extracted from ``cli/_orphan_sweep.py`` when that module crossed the
200-line size ratchet. Holds the two registry lookups the sweep needs
before it can plan anything, kept apart so the CLI module stays at
argparse + orchestration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lite_indexer.config.rollback_tables import RollbackTarget


def chain_family(chain: str) -> tuple[str, tuple[RollbackTarget, ...], str, bool]:
    """Resolve a chain to its checkpoint table, rollback targets and depth.

    Args:
        chain: Chain name.

    Returns:
        ``(family, targets, checkpoint_table, chain_scoped)``. Unknown
        chains raise ``ValueError`` rather than defaulting — a default
        would pick the wrong checkpoint table and produce a wrong
        safe point.
    """
    from lite_indexer.config.rollback_tables import EVM_ROLLBACK_TABLES, SOLANA_ROLLBACK_TABLES
    from lite_indexer.strategies import SUPPORTED_CHAINS

    if chain not in SUPPORTED_CHAINS:
        raise ValueError(f"unsupported chain {chain!r}")
    family = SUPPORTED_CHAINS[chain].get("type", "evm")
    if family == "solana":
        # Both the table and the rollback list come from the module that
        # owns them; the list is re-exported there, not defined there.
        from lite_indexer.strategies.solana.reorg import SOLANA_CHECKPOINTS_TABLE

        return family, SOLANA_ROLLBACK_TABLES, SOLANA_CHECKPOINTS_TABLE, False
    from lite_indexer.config import CH_BLOCK_CHECKPOINTS_TABLE

    return family, EVM_ROLLBACK_TABLES, CH_BLOCK_CHECKPOINTS_TABLE, True


def target_for(table: str) -> RollbackTarget:
    """Look up a :class:`RollbackTarget` by table name across both families.

    Needed because the apply loop re-derives the height column and the
    chain-scoped flag from the shared registry rather than trusting a name
    carried through the plan.
    """
    from lite_indexer.config.rollback_tables import EVM_ROLLBACK_TABLES, SOLANA_ROLLBACK_TABLES

    for target in (*EVM_ROLLBACK_TABLES, *SOLANA_ROLLBACK_TABLES):
        if target.table == table:
            return target
    raise ValueError(f"table {table!r} is not in any rollback list")


__all__ = ["chain_family", "target_for"]
