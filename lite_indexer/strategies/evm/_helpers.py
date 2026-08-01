"""
Shared helpers for EVM strategy modules.

Provides block range enforcement and reorg detection used across
the transfer, DEX, and deployer handler modules.
"""

from lite_indexer.chains import CHAIN_BLOCK_RANGE_LIMITS, get_max_block_range
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def enforce_block_range(
    chain: str,
    from_block: int,
    to_block: int,
    batch_size: int,
    endpoints: list[str] | None = None,
) -> tuple[int, int]:
    """Apply chain-specific block range limits + batch size cap.

    Uses CHAIN_BLOCK_RANGE_LIMITS to cap the range per chain, then
    applies batch_size as a second safety limit. When ``endpoints`` is
    provided, the cap is further constrained by the largest
    per-provider MAX_BLOCK_RANGE among those endpoints (so the range is
    never larger than what at least one endpoint can serve). This
    prevents "response too large" errors from restrictive free RPCs
    (e.g. base.org which only supports 10-block eth_getLogs ranges).

    Args:
        chain: Chain name.
        from_block: Requested start block.
        to_block: Requested end block.
        batch_size: Maximum batch size as a secondary cap.
        endpoints: Endpoint URLs used for this query (optional).

    Returns:
        (from_block, capped_to_block) tuple.
    """
    max_range = CHAIN_BLOCK_RANGE_LIMITS.get(chain, 10000)
    range_size = to_block - from_block
    effective_cap = min(max_range, batch_size)

    if endpoints:
        provider_caps = [c for c in (get_max_block_range(ep) for ep in endpoints) if c is not None]
        if provider_caps:
            # Largest cap any endpoint supports — get_endpoints_for_range
            # will filter the endpoint list to those that can serve it.
            effective_cap = min(effective_cap, max(provider_caps))

    if range_size > effective_cap:
        logger.debug(
            "capping_block_range",
            chain=chain,
            requested=range_size,
            cap=effective_cap,
        )
        return (from_block, from_block + effective_cap)
    return (from_block, to_block)


def detect_reorg(from_block: int, to_block: int) -> bool:
    """Detect chain reorgs where block number goes backwards.

    When from_block > to_block, it means either:
    - The chain experienced a reorganization (blocks unwound)
    - There was an RPC consistency issue

    Args:
        from_block: Previously indexed block.
        to_block: Current chain tip.

    Returns:
        True if a reorg was detected.
    """
    return from_block > to_block


__all__ = [
    "enforce_block_range",
    "detect_reorg",
]
