"""Tron reorg detection via blockID checkpointing.

Tron is the third chain given a first-class reorg path. It was missing one
for a specific, provable reason: every entry point that could reach the
rollback machinery is typed to the EVM/Solana strategies, so ``TronStrategy``
never had a ``reorg_stage`` to call, and ``tron_launches`` — keyed on
``block_number`` (migrations/010_tron_launch.sql) — accumulated orphans with
no purge path. The round-2 fix widened the shared rollback list for EVM and
Solana and deliberately left Tron out pending a decision; this module is that
decision, taken as (a) real checking.

**The integrity signal is ``blockID``.** TronGrid serves it as a top-level
field on both ``/wallet/getblockbynum`` and ``/wallet/getnowblock`` (verified
live 2026-09-28: block 66000000 → ``0000...f92c39``). It is the 64-hex
identifier of the block header and the parent pointer of its child
(``block_header.raw_data.parentHash``), so it is directly analogous to EVM's
``eth_getBlockByNumber(...).hash`` and Solana's ``getBlock(slot).blockhash``.
A mismatch against a stored checkpoint is therefore real fork evidence.

``srw`` was the other candidate and is **not** used: it is absent from
current blocks (42 super-representative signatures were returned in its
place), so it cannot anchor a checkpoint.

Cost: one POST to an endpoint already in the failover pool, once per batch.
Tron blocks are ~3s and ``config.finality`` sets ``REORG_DEPTH['tron'] = 19``
(≈57s of unripe window), so this is the same per-batch shape as EVM/Solana.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import CH_BLOCK_CHECKPOINTS_TABLE
from lite_indexer.config.finality import reorg_depth
from lite_indexer.config.rollback_tables import TRON_ROLLBACK_TABLES, delete_above_fork
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.strategies.reorg_status import ReorgCheck, ReorgStatus

logger = get_logger(__name__)

# Blocks a blockID must age before it is trusted. Sourced from the shared
# finality config (not hardcoded) so this path, the query path and the
# Solana staging path cannot disagree on how deep "safe" is.
TRON_REORG_DEPTH = reorg_depth("tron")


async def fetch_block_id(client: Any, block_number: int) -> str | None:
    """Fetch one block's ``blockID`` from TronGrid.

    Uses ``/wallet/getblockbynum``, the same endpoint the strategy already
    calls to resolve block timestamps, so it adds no new endpoint trust and
    no new failure mode to the failover path.

    Returns ``None`` when the height is unknown to the node (an empty JSON
    object, which TronGrid returns for heights above the tip) or when every
    endpoint failed — both are "cannot tell", never "no reorg".
    """
    from lite_indexer.strategies.tron.rpc import tron_post

    result = await tron_post(client, "/wallet/getblockbynum", {"num": int(block_number)})
    if not isinstance(result, dict):
        return None
    block_id = result.get("blockID")
    if not isinstance(block_id, str) or len(block_id) < 32:
        # An empty dict (height above tip) or a malformed body: the node
        # cannot vouch for this height.
        return None
    return block_id


async def check_reorg(
    client: Any,
    tip_block: int,
    ch_client: Any,
) -> ReorgCheck:
    """Compare the now-deep block's ``blockID`` against its checkpoint.

    Verifies ``tip_block - TRON_REORG_DEPTH``: fetches that height's current
    ``blockID`` and compares it against the one checkpointed when it was the
    batch tip. Fail-CLOSED — a ClickHouse lookup error or a missing
    ``blockID`` is :attr:`ReorgStatus.UNKNOWN`, never "no reorg", and
    ``error`` stays ``None`` only for the benign "no checkpoint yet" case so
    callers can tell "nothing to compare" from "verification failed".
    """
    verify_block = tip_block - TRON_REORG_DEPTH
    if verify_block < 0:
        return ReorgCheck(ReorgStatus.NO_REORG)

    try:
        rows = ch_client.execute(
            f"SELECT block_hash FROM {CH_BLOCK_CHECKPOINTS_TABLE} "
            "WHERE chain = %(chain)s AND block_number = %(block)s LIMIT 1",
            {"chain": "tron", "block": verify_block},
        )
    except Exception as exc:  # noqa: BLE001 — surface as UNKNOWN, never fail open
        logger.error("tron_checkpoint_lookup_failed", block=verify_block, error=str(exc)[:120])
        metrics_incr("reorg_lookup_errors")
        return ReorgCheck(ReorgStatus.UNKNOWN, error=str(exc)[:120])

    if not rows:
        logger.debug("tron_checkpoint_absent", block=verify_block)
        return ReorgCheck(ReorgStatus.UNKNOWN)

    stored = str(rows[0][0]).lower()
    current = await fetch_block_id(client, verify_block)
    if not current:
        logger.error("tron_verify_block_id_missing", block=verify_block)
        return ReorgCheck(ReorgStatus.UNKNOWN, error="blockID unavailable for verify height")

    if stored == current.lower():
        return ReorgCheck(ReorgStatus.NO_REORG)

    logger.warning(
        "tron_reorg_detected_at_checkpoint",
        block=verify_block,
        stored=stored[:20],
        current=current[:20],
    )
    metrics_incr("reorg_detected")
    return ReorgCheck(ReorgStatus.REORG, fork_block=verify_block)


async def rollback_to_block(
    ch_client: Any,
    fork_block: int,
) -> int | None:
    """Delete rows above ``fork_block`` from every table Tron writes.

    Purges :data:`TRON_ROLLBACK_TABLES` — ``tron_launches`` (the table this
    whole path exists for), the ``chain='tron'`` rows of the shared
    ``evm_transfers`` and ``address_activity`` tables, and the chain's block
    checkpoints. ``internal_transfers``/``dex_events`` are intentionally not
    listed: the Tron strategy implements no ``supports_dex`` path and no trace
    writer, so a Tron rollback touching them would delete rows belonging to
    other chains' shared tables on a false positive.

    Fail-CLOSED: any DELETE failure returns ``None`` so the caller cannot
    report orphaned rows as removed.
    """
    deleted, failed = delete_above_fork(ch_client, TRON_ROLLBACK_TABLES, fork_block, chain="tron")
    if failed is not None:
        metrics_incr("reorg_rollback_errors")
        return None

    logger.warning("tron_reorg_rollback_complete", fork_block=fork_block, deleted=deleted)
    return deleted


def ensure_tron_checkpoints(ch_client: Any) -> None:
    """No-op kept so callers need not know Tron reuses the EVM table.

    Tron checkpoints live in the same ``block_checkpoints`` table as the EVM
    (discriminated by ``chain``) rather than in a chain-specific table, so
    there is no separate DDL to apply — the EVM helper already did it.
    """


__all__ = [
    "TRON_REORG_DEPTH",
    "TRON_ROLLBACK_TABLES",
    "check_reorg",
    "fetch_block_id",
    "rollback_to_block",
]
