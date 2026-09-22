"""Hot/cold planning + merge for Transparent Historical Access (mixin).

Extracted from ``storage/historical.py``: deciding which tiers a range
needs (``_plan``) and merging/deduplicating hot + cold results with hot
wins and global newest-first ordering (``_smart_merge``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lite_indexer.storage.historical_tables import _ROW_KEY_COLS, _SORT_COLS, _sort_value, _Split


class HistoricalAccessMerge:
    """Tier planning + hot/cold merge mixin."""

    def _plan(
        self,
        table: str,
        from_block: int | None,
        to_block: int | None,
        from_time: datetime | None,
        to_time: datetime | None,
    ) -> _Split:
        """Decide which tiers a query range needs."""
        cutoff = self.hot_cutoff(table)

        if from_time is not None and to_time is not None:
            if to_time <= cutoff:
                return _Split(hot=False, cold=True)  # fully cold
            if from_time >= cutoff:
                return _Split(hot=True, cold=False)  # fully hot
            return _Split(hot=True, cold=True)  # spans the boundary

        if from_time is not None:
            # Open-ended future bound → includes hot; also cold when the
            # start predates the cutoff.
            return _Split(hot=True, cold=from_time < cutoff)

        if to_time is not None:
            if to_time < cutoff:
                return _Split(hot=False, cold=True)
            return _Split(hot=True, cold=True)

        if from_block is not None or to_block is not None:
            # Block-bounded without time bounds: cannot rule out cold
            # history, so consult both tiers (cold scan bounded by the
            # cold store's file cap).
            return _Split(hot=True, cold=True)

        # No bounds at all → the "recent everything" API contract: hot only.
        return _Split(hot=True, cold=False)

    def _smart_merge(
        self,
        table: str,
        hot_results: list[dict[str, Any]],
        cold_results: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        """Merge and deduplicate hot + cold results.

        * Row identity is the table's key columns (``_ROW_KEY_COLS``).
        * Hot wins on overlap: the same logical row read from both tiers
          keeps the hot (ClickHouse) copy.
        * Global ordering is preserved (newest first) and the merged
          result is truncated to ``limit``.
        """
        merged: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in cold_results:
            key = self._row_key(table, row)
            if key not in merged:
                merged[key] = row
        for row in hot_results:
            merged[self._row_key(table, row)] = row  # hot wins

        rows = list(merged.values())
        sort_cols = _SORT_COLS[table]
        rows.sort(
            key=lambda r: tuple(_sort_value(r, c) for c in sort_cols),
            reverse=True,
        )
        return rows[:limit]

    @staticmethod
    def _row_key(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
        """Return the dedupe identity for a row."""
        key: list[Any] = []
        for col in _ROW_KEY_COLS[table]:
            value = row.get(col)
            if isinstance(value, (list, tuple)):
                value = str(value)
            key.append(value)
        return tuple(key)
