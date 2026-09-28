"""A resolved TronGrid event window.

Own module rather than a class inside ``strategies/tron/strategy.py`` so the
strategy stays under its shrink-only size ratchet.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TokenEventWindow:
    """A resolved TronGrid event window.

    TronGrid filters events on block *timestamps*, not block numbers, so the
    block range travels only for logging while ``min_ts``/``max_ts`` are the
    real query bounds. Bundled so a caller cannot pass a block range without
    also resolving the timestamps it corresponds to.
    """

    from_block: int
    to_block: int
    min_ts: int
    max_ts: int


__all__ = ["TokenEventWindow"]
