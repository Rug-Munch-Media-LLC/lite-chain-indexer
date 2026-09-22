"""Portable WHERE-bound extraction for THA SQL routing.

Extracted from ``storage/historical_sqlparse.py``: the bounds accumulator
and the per-conjunct appliers for ``col op %(param)s`` and
``col BETWEEN %(lo)s AND %(hi)s`` conjuncts. Non-portable conjuncts
(OR-joins, literals without params, ambiguous string comparisons) return
False so the caller falls back to the hot path.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lite_indexer.storage.historical_tables import _sql_lit

_TIME_COLS = ("block_timestamp", "block_time", "timestamp")
_BLOCK_COLS = ("block_number", "block", "slot")


class _Bounds:
    """Accumulator for the portable WHERE bounds of one query."""

    def __init__(self) -> None:
        self.chain: str = ""
        self.from_block: int | None = None
        self.to_block: int | None = None
        self.from_time: datetime | None = None
        self.to_time: datetime | None = None
        self.portable: list[str] = []

    def as_tuple(self) -> tuple[str, int | None, int | None, datetime | None, datetime | None, str]:
        return self.chain, self.from_block, self.to_block, self.from_time, self.to_time, " AND ".join(self.portable)

    def apply_cond(self, col: str, op: str, param: str, params: dict[str, Any]) -> bool:
        """Apply one ``col op %(param)s`` conjunct; False when non-portable."""
        if param not in params:
            return False
        value = params[param]
        if isinstance(value, datetime):
            return self._apply_time_cond(col, op, value)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return self._apply_block_cond(col, op, value)
        if isinstance(value, str):
            return self._apply_str_cond(col, op, value)
        return False

    def _apply_time_cond(self, col: str, op: str, value: datetime) -> bool:
        if col not in _TIME_COLS:
            return False
        if op == ">=":
            self.from_time = value
        elif op == "<=":
            self.to_time = value
        else:
            return False
        return True

    def _apply_block_cond(self, col: str, op: str, value: float) -> bool:
        if col not in _BLOCK_COLS:
            return False
        if op == ">=":
            self.from_block = int(value)
        elif op == "<=":
            self.to_block = int(value)
        else:
            return False
        return True

    def _apply_str_cond(self, col: str, op: str, value: str) -> bool:
        if col == "chain" and op == "=":
            self.chain = value
            return True
        if col in _TIME_COLS or col in _BLOCK_COLS:
            return False  # string comparisons on time/block are ambiguous
        self.portable.append(f"{col} {op} {_sql_lit(value)}")
        return True

    def apply_between(self, col: str, lo_param: str, hi_param: str, params: dict[str, Any]) -> bool:
        """Apply one ``col BETWEEN %(lo)s AND %(hi)s`` conjunct."""
        if lo_param not in params or hi_param not in params:
            return False
        lo_v, hi_v = params[lo_param], params[hi_param]
        if isinstance(lo_v, datetime) and isinstance(hi_v, datetime) and col in _TIME_COLS:
            self.from_time, self.to_time = lo_v, hi_v
            return True
        if isinstance(lo_v, (int, float)) and isinstance(hi_v, (int, float)) and col in _BLOCK_COLS:
            self.from_block, self.to_block = int(lo_v), int(hi_v)
            return True
        return False
