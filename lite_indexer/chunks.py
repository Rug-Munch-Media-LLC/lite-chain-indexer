"""
Chunk-based indexing utilities.

Splits a block/slot range into configurable chunks so each chunk can be
fetched with a single ``eth_getLogs`` call and processed in parallel.
Reduces RPC round trips by the chunk-size factor and avoids
"response too large" errors from restrictive free RPCs.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from lite_indexer.config import CHUNK_PARALLELISM, CHUNK_SIZE_EVM, CHUNK_SIZE_SOLANA


def chunk_size_for_chain(chain: str) -> int:
    """Return the default chunk size for a chain.

    EVM chains use 50 blocks per chunk; Solana uses 500 slots (its
    ``getSignaturesForAddress`` flow is signature-bound, not log-bound);
    Tron caps at its configured max block range.

    Args:
        chain: Chain name (``ethereum``, ``solana``, ``tron``, ...).

    Returns:
        Chunk size in blocks/slots.
    """
    if chain == "solana":
        return CHUNK_SIZE_SOLANA
    return CHUNK_SIZE_EVM


def chunk_range(
    from_block: int,
    to_block: int,
    chunk_size: int,
) -> Iterator[tuple[int, int]]:
    """Yield ``(start, end)`` inclusive sub-ranges of ``[from_block, to_block]``.

    Each yielded range is at most ``chunk_size`` blocks wide and the
    ranges partition the input range with no overlap and no gaps.

    Args:
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        chunk_size: Max width of each chunk (must be >= 1).

    Yields:
        ``(chunk_start, chunk_end)`` tuples.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    if to_block < from_block:
        return
    start = from_block
    while start <= to_block:
        end = min(start + chunk_size - 1, to_block)
        yield start, end
        start = end + 1


def num_chunks(from_block: int, to_block: int, chunk_size: int) -> int:
    """Return the number of chunks covering ``[from_block, to_block]``.

    Args:
        from_block: Start block (inclusive).
        to_block: End block (inclusive).
        chunk_size: Max width of each chunk.

    Returns:
        Number of chunks (0 when the range is empty/inverted).
    """
    if to_block < from_block or chunk_size < 1:
        return 0
    width = to_block - from_block + 1
    return (width + chunk_size - 1) // chunk_size


def merge_chunked_results(results: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Merge chunk results, deduplicating by ``(txHash, logIndex)``.

    Parallel chunk fetches can return overlapping events when an RPC
    serves a slightly different view per request (reorg/consistency), so
    results are deduplicated on the same key used elsewhere in the
    codebase.

    Args:
        results: List of event lists, one per chunk (in order).

    Returns:
        Merged, de-duplicated event list preserving chunk order.
    """
    seen_keys: set[str] = set()
    merged: list[dict[str, Any]] = []
    for chunk in results:
        for evt in chunk:
            tx_hash = evt.get("txHash") or evt.get("transactionHash") or ""
            key = f"{tx_hash}:{evt.get('logIndex', 0)}"
            if key not in seen_keys:
                seen_keys.add(key)
                merged.append(evt)
    return merged


__all__ = [
    "CHUNK_PARALLELISM",
    "chunk_size_for_chain",
    "chunk_range",
    "num_chunks",
    "merge_chunked_results",
]
