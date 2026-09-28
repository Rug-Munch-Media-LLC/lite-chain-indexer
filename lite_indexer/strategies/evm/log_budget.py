"""Suspicion test for a silently truncated ``eth_getLogs`` response.

A public RPC that caps its result list will answer a ranged
``eth_getLogs`` with ``{"result": [...]}`` and HTTP 200 — no error, no
warning. The endpoint-rotation loop used to ``break`` on the first
200-with-a-list, so a short page was accepted as the complete chunk and
the sync loop advanced its watermark past blocks that were never
queried. The old ``len(parsed) == len(logs)`` guard could not catch this
because the parser legitimately drops rows (NFT transfers, malformed
logs), so the count comparison was always unequal and the chunk queried
every endpoint in rotation.

What we CAN check without a second RPC: the block span the response
actually covers. A well-formed response for ``[from_block, to_block]``
has a non-empty result set whose first and last ``blockNumber`` are
inside the range. A truncated page is missing its tail, so the response
either is empty (a genuine zero) or its blocks do not reach the range
end. That is a pure predicate over data already in hand — no bloom
round-trip, no extra endpoint call.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: A response whose widest block is this far from the range end is
#: treated as possibly truncated. A handful of blocks of slack absorbs
#: a quiet tail (the last few blocks legitimately carry no matching
#: logs) without letting a mid-range cut-off pass as complete.
TAIL_SLACK_BLOCKS = 5


def is_truncated_log_response(
    logs: list[dict[str, Any]],
    from_block: int,
    to_block: int,
) -> bool:
    """Return True when ``logs`` looks like a truncated ``eth_getLogs`` page.

    Args:
        logs: The raw (unparsed) log dicts from the JSON-RPC result.
        from_block: Inclusive range start that was requested.
        to_block: Inclusive range end that was requested.

    Returns:
        True when the response is suspicious enough that the chunk must be
        re-queried on a different endpoint rather than accepted. An empty
        list is NOT suspicious (a range with no matching logs is a valid
        answer) and a response that reaches the range end is not either.
    """
    if not logs:
        return False

    numbers = [_block_number(log) for log in logs]
    present = [n for n in numbers if n is not None]
    if not present:
        # Unparseable block numbers: we cannot prove the tail is covered.
        return True

    return max(present) < to_block - TAIL_SLACK_BLOCKS


def _block_number(log: dict[str, Any]) -> int | None:
    """Parse a log's ``blockNumber`` (hex string or int) to an int."""
    raw = log.get("blockNumber") if isinstance(log, dict) else None
    if raw is None:
        return None
    if isinstance(raw, int):
        return raw
    try:
        return int(str(raw), 16)
    except (TypeError, ValueError) as exc:
        logger.debug("log_budget_unparseable_block_number", error=str(exc)[:80], raw=str(raw)[:24])
        return None


__all__ = ["TAIL_SLACK_BLOCKS", "is_truncated_log_response"]
