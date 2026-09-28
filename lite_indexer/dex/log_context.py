"""Context shared by every ``eth_getLogs`` decoder.

The swap and LP decoders take the same six values — the raw logs, the chain,
the inclusive block range, the topic being decoded, and the pair-token map —
because they answer the same question about different topics. Bundling them
keeps each decoder under the too-many-params ratchet and means a new decoder
cannot forget to thread one of them through.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class LogDecodeContext:
    """Everything a decoder needs besides the raw log dicts themselves.

    Attributes:
        chain: Chain name the logs came from.
        from_block: Inclusive block-number filter that was requested.
        to_block: Inclusive block-number filter that was requested.
        topic: Event topic hash being decoded, so ``type`` is derived from
            it rather than hardcoded in each decoder.
        pair_tokens: Pair address to ``(token0, token1)``, for labelling.
    """

    chain: str
    from_block: int
    to_block: int
    topic: str
    pair_tokens: dict[str, tuple[str, str]]

    def in_range(self, block: Any) -> bool:
        """True when a log's block number falls inside this context's range."""
        try:
            number = int(block)
        except (TypeError, ValueError):
            logger.debug("log_block_number_unreadable", raw=str(block)[:40])
            return False
        return self.from_block <= number <= self.to_block


__all__ = ["LogDecodeContext"]


__all__ = ["LogDecodeContext"]
