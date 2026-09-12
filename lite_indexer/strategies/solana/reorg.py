"""
Solana reorg detection via slot-finality checkpointing.

Mirrors the EVM reorg staging (``strategies/evm/reorg.py``) for Solana:
each processed batch checkpoints the tip slot's blockhash into a
ClickHouse table (``wallet_memory.solana_checkpoints``); when a slot has
advanced past ``SOLANA_REORG_DEPTH`` slots, its stored blockhash is
compared against the current chain blockhash. On mismatch the chain
reorganized underneath us, so the indexer rolls back
``solana_spl_balance_changes`` + ``dex_events`` (chain='solana') above
the fork slot and re-fetches from there.

Note: finalized slots (``commitment: finalized``) are theoretically
immutable, but RPC providers occasionally serve divergent/optimistic
blocks — this staging catches those inconsistencies the same way the
EVM path catches reorgs. Gated by ``REORG_PROTECTION_ENABLED``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import (
    CH_DEX_TABLE,
    SOLANA_CH_TABLE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# How many slots back a checkpoint must be confirmed before we trust the
# blockhash. Solana slots are ~400ms, so 32 slots ≈ 13s of safety margin.
SOLANA_REORG_DEPTH = 32

# Checkpoint table: one row per slot. ReplacingMergeTree keeps only the
# latest blockhash per slot so re-checkpointing is a no-op.
SOLANA_CHECKPOINTS_TABLE = "wallet_memory.solana_checkpoints"

_SOLANA_CHECKPOINTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {SOLANA_CHECKPOINTS_TABLE} (
    slot UInt64,
    blockhash String,
    checked_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (slot)
"""


async def ensure_checkpoint_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_checkpoints`` if it does not exist.

    Idempotent and non-fatal: a missing database or lacking privileges
    degrades to a debug log (reorg staging skips that cycle).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False otherwise.
    """
    try:
        ch_client.execute(_SOLANA_CHECKPOINTS_DDL)  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
        logger.info("solana_checkpoints_table_ensured", table=SOLANA_CHECKPOINTS_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_checkpoints_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_checkpoints_table_failed", error=str(exc)[:100])
        return False


async def _get_blockhash(client: Any, slot: int) -> str | None:
    """Fetch a slot's blockhash via ``getBlock`` (no tx details).

    Args:
        client: httpx AsyncClient for Solana RPC.
        slot: Slot number.

    Returns:
        The block's blockhash, or None when unavailable (skipped slot,
        RPC failure, pruned block).
    """
    from lite_indexer.strategies.solana.rpc import rpc_call

    result = await rpc_call(
        client,
        "getBlock",
        [slot, {"transactionDetails": "none", "maxSupportedTransactionVersion": 0, "commitment": "finalized"}],
    )
    if not result or not isinstance(result.get("result"), dict):
        return None
    blockhash = result["result"].get("blockhash")
    return str(blockhash) if blockhash else None


async def checkpoint_slot(client: Any, slot: int, ch_client: Any) -> bool:
    """Record a slot's blockhash for later reorg detection.

    INSERTs into ``wallet_memory.solana_checkpoints``. ReplacingMergeTree
    means re-checkpointing the same slot simply updates its blockhash.

    Args:
        client: httpx AsyncClient for Solana RPC.
        slot: Slot number to checkpoint.
        ch_client: ClickHouse client.

    Returns:
        True when the checkpoint was written, False on failure.
    """
    blockhash = await _get_blockhash(client, slot)
    if not blockhash:
        logger.debug("solana_slot_checkpoint_hash_missing", slot=slot)
        return False
    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"INSERT INTO {SOLANA_CHECKPOINTS_TABLE} (slot, blockhash, checked_at) VALUES",
            [(slot, blockhash, datetime.now(UTC))],
        )
        return True
    except Exception as exc:  # noqa: BLE001 — checkpoint is best-effort
        logger.debug("solana_slot_checkpoint_failed", slot=slot, error=str(exc)[:80])
        return False


async def detect_reorg(client: Any, current_slot: int, ch_client: Any) -> int | None:
    """Compare a confirmed slot's blockhash against the stored checkpoint.

    Verifies the slot at ``current_slot - SOLANA_REORG_DEPTH``: fetches
    its current blockhash from RPC and compares it against the blockhash
    checkpointed when that slot was the batch tip. On mismatch the chain
    reorganized underneath us.

    Args:
        client: httpx AsyncClient for Solana RPC.
        current_slot: Tip slot of the processed batch.
        ch_client: ClickHouse client.

    Returns:
        The fork slot when a reorg was detected, otherwise None (no
        checkpoint yet, chain matches, or lookup failed).
    """
    verify_slot = current_slot - SOLANA_REORG_DEPTH
    if verify_slot < 0:
        return None

    try:
        rows = ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"SELECT blockhash FROM {SOLANA_CHECKPOINTS_TABLE} WHERE slot = %(slot)s LIMIT 1",
            {"slot": verify_slot},
        )
    except Exception as exc:  # noqa: BLE001 — lookup is best-effort
        logger.debug("solana_checkpoint_lookup_failed", slot=verify_slot, error=str(exc)[:80])
        return None

    if not rows:
        # No checkpoint yet — cannot detect a reorg for this slot.
        return None

    current_blockhash = await _get_blockhash(client, verify_slot)
    if not current_blockhash:
        logger.debug("solana_verify_blockhash_missing", slot=verify_slot)
        return None

    stored_blockhash = str(rows[0][0])
    if stored_blockhash == current_blockhash:
        return None

    logger.warning(
        "solana_reorg_detected_at_checkpoint",
        slot=verify_slot,
        stored=stored_blockhash[:20],
        current=current_blockhash[:20],
    )
    metrics_incr("reorg_detected")
    return verify_slot


async def rollback_to_slot(ch_client: Any, fork_slot: int) -> int:
    """Delete rows above ``fork_slot`` from the Solana indexed tables.

    Removes ``solana_spl_balance_changes`` rows with ``slot > fork_slot``
    and ``dex_events`` rows with ``chain = 'solana'`` and
    ``blockNumber > fork_slot`` (the fork slot stays). Also prunes stale
    checkpoints above the fork so the re-fetched chain is checkpointed
    fresh. Returns the total number of rows deleted (best effort).

    Args:
        ch_client: ClickHouse client (raw, from ClickHouseWriter).
        fork_slot: Common-ancestor slot; rows above it are orphaned.

    Returns:
        Total deleted rows (0 on failure).
    """
    deleted = 0
    for table, _column, chain_filter in (
        (SOLANA_CH_TABLE, "slot", False),
        (CH_DEX_TABLE, "blockNumber", True),
    ):
        where = "chain = 'solana' AND blockNumber > %(fork)s" if chain_filter else "slot > %(fork)s"
        try:
            result = ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                f"DELETE FROM {table} WHERE {where}",
                {"fork": fork_slot},
            )
            if isinstance(result, int):
                deleted += result
        except Exception as exc:  # noqa: BLE001 — rollback is best-effort
            logger.warning(
                "solana_reorg_rollback_table_failed",
                table=table,
                fork_slot=fork_slot,
                error=str(exc)[:100],
            )

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"DELETE FROM {SOLANA_CHECKPOINTS_TABLE} WHERE slot > %(fork)s",
            {"fork": fork_slot},
        )
    except Exception as exc:  # noqa: BLE001 — checkpoint pruning is best-effort
        logger.debug("solana_checkpoint_prune_failed", fork_slot=fork_slot, error=str(exc)[:80])

    logger.warning("solana_reorg_rollback_complete", fork_slot=fork_slot, deleted=deleted)
    return deleted


__all__ = [
    "SOLANA_CHECKPOINTS_TABLE",
    "SOLANA_REORG_DEPTH",
    "ensure_checkpoint_table",
    "checkpoint_slot",
    "detect_reorg",
    "rollback_to_slot",
]
