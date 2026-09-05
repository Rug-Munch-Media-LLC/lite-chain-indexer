"""DEX swap event buffering mixin (wallet_memory.dex_events, chain='solana').

Split from the former monolithic strategy.py. Swap rows are parsed from
the same getTransaction results as SPL balance changes (no extra RPC),
buffered on the strategy instance and drained by the sync loop, which
writes them via the shared write_dex_events path.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Upper bound on buffered dex_events swap rows awaiting the sync-loop drain
# (same rationale as _SOLANA_TRACE_ROW_CAP).
_SOLANA_SWAP_ROW_CAP = 10_000


class DexEventsMixin:
    """Buffering of Solana DEX swap rows (best-effort)."""

    def _buffer_swap_rows(self, swap_rows: list[dict[str, Any]]) -> None:
        """Buffer swap rows, dropping the oldest when over the cap."""
        self._last_swap_events.extend(swap_rows)
        if len(self._last_swap_events) > _SOLANA_SWAP_ROW_CAP:
            self._last_swap_events = self._last_swap_events[-_SOLANA_SWAP_ROW_CAP:]
