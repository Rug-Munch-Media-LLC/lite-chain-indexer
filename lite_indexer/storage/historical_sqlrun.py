"""Unified SQL routing + post-merge finishing for THA (mixin).

Extracted from ``storage/historical.py``: ``query_sql`` (route a simple
single-table SELECT through the unified hot+cold layer) plus the ORDER BY
application and SELECT-list projection applied after the merge.
"""

from __future__ import annotations

import re
from functools import cmp_to_key
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.storage.historical_sqlparse import HistoricalAccessSQLParse
from lite_indexer.storage.historical_tables import _sort_value

logger = get_logger(__name__)


class HistoricalAccessSQL(HistoricalAccessSQLParse):
    """SQL routing mixin (API wiring path 2)."""

    def query_sql(
        self, sql: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]] | None:
        """Route a simple single-table SELECT through the unified layer.

        Only queries that are safe to execute on BOTH engines are routed:
        plain column SELECTs (no ClickHouse-only functions/aliases), a
        portable WHERE (``col = / >= / <= / BETWEEN`` with bound params),
        and a time/block-bounded range that extends into cold territory.

        Returns the merged rows, or None when the query is not safely
        routable — the caller should execute the original SQL against
        ClickHouse as-is (hot path, unchanged behavior).
        """
        parsed = self._parse_sql(sql)
        if parsed is None:
            return None
        table, select_cols, where_body, order_by, limit, offset = parsed

        # The view path already covers this table — let the caller use it.
        if self._view_available(table):
            return None

        bounds = self._extract_bounds(where_body, params or {})
        if bounds is None:
            return None
        chain, from_block, to_block, from_time, to_time, portable_where = bounds

        if from_time is None and to_time is None and from_block is None and to_block is None:
            return None  # unbounded → hot-only API contract

        split = self._plan(table, from_block, to_block, from_time, to_time)
        if not split.cold:
            return None  # fully hot → let the caller run the original SQL

        hot_rows = self._query_sql_hot(sql, params or {}, table)
        cap = limit + offset + 1
        cold_rows = self._cold.query(
            table,
            chain,
            from_block=from_block,
            to_block=to_block,
            from_time=from_time,
            to_time=to_time,
            where=portable_where,
            limit=cap,
        )
        merged = self._smart_merge(table, hot_rows, cold_rows, cap)
        merged = self._order_rows(merged, order_by)
        merged = self._project(merged, select_cols)
        return merged[offset : offset + limit]

    def _query_sql_hot(self, sql: str, params: dict[str, Any], table: str) -> list[dict[str, Any]]:
        """Run the hot side (original SQL minus LIMIT/OFFSET); [] on failure."""
        hot_rows: list[dict[str, Any]] = []
        client = self._client()
        if client is None:
            return hot_rows
        try:
            rows, column_types = client.execute(
                self._strip_limit(sql), params, with_column_types=True
            )
            names = [name for name, _ in column_types]
            hot_rows = [dict(zip(names, row, strict=False)) for row in rows]
        except Exception as exc:  # noqa: BLE001 — hot side is best-effort
            logger.warning("tha_sql_hot_failed", table=table, error=str(exc)[:150])
        return hot_rows

    @staticmethod
    def _order_rows(rows: list[dict[str, Any]], order_by: str) -> list[dict[str, Any]]:
        """Apply an ORDER BY clause (column [ASC|DESC], comma-separated)."""
        if not order_by:
            return rows
        specs: list[tuple[str, bool]] = []
        for part in order_by.split(","):
            tokens = part.strip().split()
            if not tokens:
                continue
            col = tokens[0].strip()
            if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", col):
                return rows  # non-portable order → keep merge order
            desc = len(tokens) > 1 and tokens[1].upper() == "DESC"
            specs.append((col, desc))
        if not specs:
            return rows
        return sorted(rows, key=cmp_to_key(_row_comparator(specs)))

    @staticmethod
    def _project(rows: list[dict[str, Any]], select_cols: list[str]) -> list[dict[str, Any]]:
        """Project merged rows onto the original SELECT list (if explicit)."""
        if not select_cols:
            return rows
        return [{col: row.get(col) for col in select_cols} for row in rows]


def _row_comparator(specs: list[tuple[str, bool]]) -> Any:
    """Build a descending-capable comparator over the ORDER BY specs."""

    def _cmp(a: dict[str, Any], b: dict[str, Any]) -> int:
        for col, desc in specs:
            va, vb = _sort_value(a, col), _sort_value(b, col)
            if va < vb:
                return 1 if desc else -1
            if va > vb:
                return -1 if desc else 1
        return 0

    return _cmp
