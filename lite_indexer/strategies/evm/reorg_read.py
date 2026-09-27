"""The pool walk that replaced "pick one endpoint and hope" (2026-09-26).

``EVMChainStrategy.reorg_stage`` hashes two blocks per cycle: the batch tip
and the tip minus the reorg depth. Both reads used to be issued against a
single endpoint chosen by ``get_best_endpoint``, which ends in a random
power-of-two-choices draw. On a chain whose head is 45,388,234 blocks behind
the tip — far outside the ~200k-block window most public gateways retain —
roughly two thirds of those draws landed on an endpoint that answers
``200 {"result": null}``. The chain failed closed on
``reorg_status_unknown`` on average every third cycle, and reordering the
pool could not help: order never decided the winner, the coin did.

This module keeps that walk out of ``strategy.py`` (which is at its size
ratchet) and in one place, so both the tip read and the deeper verify read
get the same treatment and a future third read cannot regress to a single
draw.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lite_indexer.logging import get_logger
from lite_indexer.rpc.block_reader import resolve_block_hash

if TYPE_CHECKING:
    from collections.abc import Sequence

    from lite_indexer.rpc.block_reader import BlockHashReader, BlockHashResult
    from lite_indexer.strategies.reorg_status import ReorgCheck

logger = get_logger(__name__)


async def stage_block_hashes(
    rpc: BlockHashReader,
    chain: str,
    to_block: int,
    endpoints: Sequence[str],
    ch_client: object,
) -> tuple[ReorgCheck | None, str | None]:
    """Checkpoint the batch tip and read the deep block's current hash.

    Split out of ``EVMChainStrategy.reorg_stage`` (which is at its size and
    function-length ratchet) and kept here so both hash reads live beside
    the pool walk that fixed them.

    Returns:
        ``(check, None)`` where ``check`` is the fail-closed result when a
        block could not be hashed, or ``(None, current_hash)`` when both
        reads succeeded and the caller should compare against the
        checkpoint table.
    """
    from lite_indexer.strategies.evm.reorg import checkpoint_block, reorg_depth

    tip_hash, unserved = await hash_block_or_fail(rpc, chain, to_block, endpoints)
    if unserved is not None:
        return unserved, None
    await checkpoint_block(chain, to_block, str(tip_hash), ch_client)

    depth = reorg_depth(chain)
    verify_block = to_block - depth
    if verify_block < 0:
        return None, None

    # The verify read is deeper than the tip read, so it is if anything MORE
    # likely to fall outside a retained window. Same pool walk, same verdict.
    current_hash, unserved = await hash_block_or_fail(rpc, chain, verify_block, endpoints)
    if unserved is not None:
        logger.debug("reorg_verify_hash_missing", chain=chain, block=verify_block)
        return unserved, None
    return None, current_hash


async def hash_block_or_fail(
    rpc: BlockHashReader,
    chain: str,
    block: int,
    endpoints: Sequence[str],
) -> tuple[str | None, ReorgCheck | None]:
    """Hash ``block`` from the pool.

    Args:
        rpc: RPC client used for the read (injected).
        chain: Chain name.
        block: The block whose hash is needed.
        endpoints: The chain's pool, in configured order.

    Returns:
        ``(hash, None)`` when an endpoint served the block, else
        ``(None, fail_closed_check)``. Never both non-None: one call, one
        pool walk, no re-read.
    """
    outcome: BlockHashResult = await resolve_block_hash(rpc, chain, block, endpoints)
    if outcome.ok:
        return str(outcome.block_hash), None
    return None, _unserved(chain, block, outcome)


def _unserved(chain: str, block: int, outcome: BlockHashResult) -> ReorgCheck:
    """Build the fail-closed result and name the cause in the log.

    The log line distinguishes a pool that has the block somewhere from a
    pool that is refusing us outright, because the operator action differs:
    the first is a window problem that self-heals as the crawl moves into
    range, the second is a provider or key problem that will not.
    """
    from lite_indexer.strategies.reorg_status import ReorgCheck, ReorgStatus

    logger.warning(
        "reorg_block_hash_unserved",
        chain=chain,
        block=block,
        endpoints_tried=len(outcome.tried),
    )
    return ReorgCheck(ReorgStatus.UNKNOWN, error=f"block hash unserved for {block}")


__all__ = ["hash_block_or_fail", "stage_block_hashes"]
