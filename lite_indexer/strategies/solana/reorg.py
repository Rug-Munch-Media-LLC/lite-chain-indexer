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

from lite_indexer.config.finality import reorg_depth
from lite_indexer.config.rollback_tables import SOLANA_ROLLBACK_TABLES, delete_above_fork
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.strategies.reorg_status import ReorgCheck, ReorgStatus

logger = get_logger(__name__)

# How many slots back a checkpoint must be confirmed before we trust the
# blockhash. Solana slots are ~400ms, so 32 slots ≈ 13s of safety margin.
# Canonical value lives in ``config.finality`` so the staging and on-demand
# query paths cannot disagree (historical name kept — sync.py names it).
SOLANA_REORG_DEPTH = reorg_depth("solana")

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
    """Create ``wallet_memory.solana_checkpoints`` if absent.

    Idempotent and non-fatal: a missing database or lacking privileges
    degrades to a debug log (reorg staging skips that cycle). Returns True
    when the table exists (or was created).
    """
    try:
        ch_client.execute(
            _SOLANA_CHECKPOINTS_DDL
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
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
        [slot, {"transactionDetails": "none", "maxSupportedTransactionVersion": 1, "commitment": "finalized"}],
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
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {SOLANA_CHECKPOINTS_TABLE} (slot, blockhash, checked_at) VALUES",
            [(slot, blockhash, datetime.now(UTC))],
        )
        return True
    except Exception as exc:  # noqa: BLE001 — checkpoint is best-effort
        logger.debug("solana_slot_checkpoint_failed", slot=slot, error=str(exc)[:80])
        return False


async def check_reorg(client: Any, current_slot: int, ch_client: Any) -> ReorgCheck:
    """Compare a confirmed slot's blockhash against the stored checkpoint.

    Verifies the slot at ``current_slot - SOLANA_REORG_DEPTH``: fetches
    its current blockhash from RPC and compares it against the blockhash
    checkpointed when that slot was the batch tip.

    Fail-CLOSED: a ClickHouse lookup error or an RPC failure fetching the
    live blockhash is reported as :attr:`ReorgStatus.UNKNOWN` — never as
    "no reorg". :class:`ReorgCheck` keeps the benign "no checkpoint yet"
    (``error is None``) distinct from those failures (``error`` set).

    Args:
        client: httpx AsyncClient for Solana RPC.
        current_slot: Tip slot of the processed batch.
        ch_client: ClickHouse client.

    Returns:
        :class:`ReorgCheck` — REORG with the fork slot on mismatch;
        UNKNOWN (``error`` set on failure) when it cannot tell; NO_REORG
        when the stored and current blockhashes match.
    """
    verify_slot = current_slot - SOLANA_REORG_DEPTH
    if verify_slot < 0:
        return ReorgCheck(ReorgStatus.NO_REORG)

    try:
        rows = ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"SELECT blockhash FROM {SOLANA_CHECKPOINTS_TABLE} WHERE slot = %(slot)s LIMIT 1",
            {"slot": verify_slot},
        )
    except Exception as exc:  # noqa: BLE001 — surface as UNKNOWN, never fail open
        logger.error("solana_checkpoint_lookup_failed", slot=verify_slot, error=str(exc)[:120])
        metrics_incr("reorg_lookup_errors")
        return ReorgCheck(ReorgStatus.UNKNOWN, error=str(exc)[:120])

    if not rows:
        # No checkpoint yet — benign: nothing to compare for this slot.
        logger.debug("solana_checkpoint_absent", slot=verify_slot)
        return ReorgCheck(ReorgStatus.UNKNOWN)

    current_blockhash = await _get_blockhash(client, verify_slot)
    if not current_blockhash:
        logger.error("solana_verify_blockhash_missing", slot=verify_slot)
        return ReorgCheck(ReorgStatus.UNKNOWN, error="verify blockhash unavailable")

    stored_blockhash = str(rows[0][0])
    if stored_blockhash == current_blockhash:
        return ReorgCheck(ReorgStatus.NO_REORG)

    logger.warning(
        "solana_reorg_detected_at_checkpoint",
        slot=verify_slot,
        stored=stored_blockhash[:20],
        current=current_blockhash[:20],
    )
    metrics_incr("reorg_detected")
    return ReorgCheck(ReorgStatus.REORG, fork_block=verify_slot)


async def detect_reorg(client: Any, current_slot: int, ch_client: Any) -> ReorgStatus:
    """Return the tri-state reorg status for a Solana slot checkpoint.

    Thin wrapper over :func:`check_reorg` for callers that only need the
    status (callers that must fail closed should use :func:`check_reorg`
    so they can inspect ``error``).

    Args:
        client: httpx AsyncClient for Solana RPC.
        current_slot: Tip slot of the processed batch.
        ch_client: ClickHouse client.

    Returns:
        :class:`ReorgStatus` — NO_REORG / REORG / UNKNOWN.
    """
    return (await check_reorg(client, current_slot, ch_client)).status


async def rollback_to_slot(ch_client: Any, fork_slot: int) -> int | None:
    """Delete rows above ``fork_slot`` from the Solana indexed tables.

    Removes every table listed in
    :data:`lite_indexer.config.rollback_tables.SOLANA_ROLLBACK_TABLES`
    with its height column above the fork slot (the fork slot stays) —
    ``solana_spl_balance_changes``, ``solana_transfers`` and the Solana
    rows of the shared ``dex_events`` / ``address_activity`` tables. Also
    prunes stale checkpoints above the fork so the re-fetched chain is
    checkpointed fresh.

    Fail-CLOSED signalling: any DELETE failure logs at ERROR and makes the
    whole rollback report failure (``None``) — the caller must not assume
    orphaned rows were removed.

    Args:
        ch_client: ClickHouse client (raw, from ClickHouseWriter).
        fork_slot: Common-ancestor slot; rows above it are orphaned.

    Returns:
        Total deleted rows, or ``None`` when any DELETE failed.
    """
    deleted, failed = delete_above_fork(ch_client, SOLANA_ROLLBACK_TABLES, fork_slot, chain="solana")
    if failed is not None:
        metrics_incr("reorg_rollback_errors")
        return None

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"DELETE FROM {SOLANA_CHECKPOINTS_TABLE} WHERE slot > %(fork)s",
            {"fork": fork_slot},
        )
    except Exception as exc:  # noqa: BLE001 — checkpoint pruning failed
        logger.error("solana_checkpoint_prune_failed", fork_slot=fork_slot, error=str(exc)[:120])
        metrics_incr("reorg_rollback_errors")
        return None

    logger.warning("solana_reorg_rollback_complete", fork_slot=fork_slot, deleted=deleted)
    return deleted


__all__ = [
    "SOLANA_CHECKPOINTS_TABLE",
    "SOLANA_REORG_DEPTH",
    "ensure_checkpoint_table",
    "checkpoint_slot",
    "check_reorg",
    "detect_reorg",
    "rollback_to_slot",
]
