"""
Reorg-safe block confirmation (Envio/TrueBlocks staging).

Tracks the last N blocks per chain as "unripe" using a ClickHouse
checkpoint table (``wallet_memory.block_checkpoints``). Each processed
batch stores the hash of its tip block; when a block has advanced past
``MAX_REORG_DEPTH`` blocks, its stored hash is compared against the
current chain hash. On mismatch the chain reorganized underneath us, so
the indexer rolls back ``evm_transfers`` + ``dex_events`` above the fork
point and re-fetches from there.

Designed to be wired into the EVM strategy after each block batch
(``EVMChainStrategy.reorg_stage``) and gated by
``REORG_PROTECTION_ENABLED`` (default on for EVM, off for Solana/Tron).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import (
    CH_BLOCK_CHECKPOINTS_TABLE,
    CH_DEX_TABLE,
    CH_TABLE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# How many blocks back a checkpoint must be confirmed before we trust the
# hash. Per-chain confirmation depths: block times vary (Polygon 2s, BSC
# 3s, Ethereum 12s), so fast chains need a deeper safety margin.
MAX_REORG_DEPTH: dict[str, int] = {
    "ethereum": 12,
    "polygon": 32,
    "bsc": 12,
    "arbitrum": 12,
    "optimism": 12,
    "base": 12,
    "avalanche": 12,
    "gnosis": 12,
}

# Fallback depth for EVM chains not listed above.
_DEFAULT_DEPTH = 12

# One row per (chain, block_number). ReplacingMergeTree keeps only the
# latest hash per block so re-checkpointing the same block is a no-op.
_BLOCK_CHECKPOINTS_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_BLOCK_CHECKPOINTS_TABLE} (
    chain LowCardinality(String),
    block_number UInt64,
    block_hash String,
    checked_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, block_number)
"""


def reorg_depth(chain: str) -> int:
    """Return the confirmation depth for a chain (fallback: 12)."""
    return MAX_REORG_DEPTH.get(chain, _DEFAULT_DEPTH)


async def ensure_checkpoint_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.block_checkpoints`` if it does not exist.

    Idempotent and non-fatal: a missing database or lacking privileges
    degrades to a debug log (reorg staging skips that cycle).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False otherwise.
    """
    try:
        ch_client.execute(_BLOCK_CHECKPOINTS_DDL)  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
        logger.info("block_checkpoints_table_ensured", table=CH_BLOCK_CHECKPOINTS_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("block_checkpoints_table_skip", error=str(exc)[:80])
        else:
            logger.warning("block_checkpoints_table_failed", error=str(exc)[:100])
        return False


async def checkpoint_block(
    chain: str,
    block_number: int,
    block_hash: str,
    ch_client: Any,
) -> bool:
    """Record a block's hash for later reorg detection.

    INSERTs into ``wallet_memory.block_checkpoints``. ReplacingMergeTree
    means re-checkpointing the same block simply updates its hash.

    Args:
        chain: Chain name.
        block_number: Block number.
        block_hash: Block hash (0x-prefixed).
        ch_client: ClickHouse client.

    Returns:
        True when the checkpoint was written, False on failure.
    """
    if not block_hash:
        return False
    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_BLOCK_CHECKPOINTS_TABLE} (chain, block_number, block_hash, checked_at) VALUES",
            [(chain, block_number, block_hash, datetime.now(UTC))],
        )
        return True
    except Exception as exc:  # noqa: BLE001 — checkpoint is best-effort
        logger.debug(
            "block_checkpoint_failed",
            chain=chain,
            block=block_number,
            error=str(exc)[:80],
        )
        return False


async def detect_reorg(
    chain: str,
    block_number: int,
    block_hash: str,
    ch_client: Any,
) -> bool:
    """Compare a current block hash against the stored checkpoint.

    Returns True when a reorg was detected (the current hash differs from
    the checkpointed hash). Returns False when there is no checkpoint yet
    (first time we've seen this block — nothing to compare), the chain
    matches, or the lookup fails.

    Args:
        chain: Chain name.
        block_number: Block number to verify.
        block_hash: Current block hash from RPC.
        ch_client: ClickHouse client.

    Returns:
        True = reorg detected, False = no reorg (or cannot tell yet).
    """
    if not block_hash:
        return False
    try:
        rows = ch_client.execute(
            f"SELECT block_hash FROM {CH_BLOCK_CHECKPOINTS_TABLE} "
            "WHERE chain = %(chain)s AND block_number = %(block)s LIMIT 1",
            {"chain": chain, "block": block_number},
        )
    except Exception as exc:  # noqa: BLE001 — lookup is best-effort
        logger.debug(
            "block_checkpoint_lookup_failed",
            chain=chain,
            block=block_number,
            error=str(exc)[:80],
        )
        return False

    if not rows:
        # No checkpoint yet — cannot detect a reorg for this block.
        return False

    stored_hash = str(rows[0][0]).lower()
    current_hash = str(block_hash).lower()
    mismatched = stored_hash != current_hash
    if mismatched:
        logger.warning(
            "reorg_detected_at_checkpoint",
            chain=chain,
            block=block_number,
            stored=stored_hash[:20],
            current=current_hash[:20],
        )
        metrics_incr("reorg_detected")
    return mismatched


async def rollback_to_block(
    chain: str,
    fork_block: int,
    ch_client: Any,
) -> int:
    """Delete rows above ``fork_block`` from indexed tables.

    Removes ``evm_transfers`` and ``dex_events`` rows with
    ``block_number > fork_block`` (the fork point stays). Also prunes
    stale block checkpoints above the fork so the re-fetched chain is
    checkpointed fresh. Returns the total number of rows deleted (best
    effort — ClickHouse lightweight DELETE returns affected count when
    available).

    Args:
        chain: Chain name.
        fork_block: Common-ancestor block; rows above it are orphaned.
        ch_client: ClickHouse client.

    Returns:
        Total deleted rows (0 on failure).
    """
    deleted = 0
    for table in (CH_TABLE, CH_DEX_TABLE):
        try:
            result = ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                f"DELETE FROM {table} WHERE chain = %(chain)s AND block_number > %(fork)s",
                {"chain": chain, "fork": fork_block},
            )
            if isinstance(result, int):
                deleted += result
        except Exception as exc:  # noqa: BLE001 — rollback is best-effort
            logger.warning(
                "reorg_rollback_table_failed",
                chain=chain,
                table=table,
                fork_block=fork_block,
                error=str(exc)[:100],
            )

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            f"DELETE FROM {CH_BLOCK_CHECKPOINTS_TABLE} WHERE chain = %(chain)s AND block_number > %(fork)s",
            {"chain": chain, "fork": fork_block},
        )
    except Exception as exc:  # noqa: BLE001 — checkpoint pruning is best-effort
        logger.debug(
            "reorg_checkpoint_prune_failed",
            chain=chain,
            fork_block=fork_block,
            error=str(exc)[:80],
        )

    logger.warning("reorg_rollback_complete", chain=chain, fork_block=fork_block, deleted=deleted)
    return deleted


__all__ = [
    "MAX_REORG_DEPTH",
    "reorg_depth",
    "ensure_checkpoint_table",
    "checkpoint_block",
    "detect_reorg",
    "rollback_to_block",
]
