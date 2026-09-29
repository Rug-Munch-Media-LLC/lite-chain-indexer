"""Conservative SQL parsing for Transparent Historical Access (mixin).

Extracted from ``storage/historical.py``: parsing a simple single-table
SELECT into (table, columns, where, order, limit, offset) and extracting
portable chain/block/time bounds from the WHERE clause. Anything not
plainly portable parses to ``None`` — the caller falls back to the hot
path unchanged.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from lite_indexer.storage.historical_bounds import _Bounds
from lite_indexer.storage.historical_tables import _UNCLEAN_KEYWORDS, _sql_lit
from lite_indexer.storage.tha_api import ThaApi


class HistoricalAccessSQLParse:
    """Conservative SQL parse mixin."""

    _SELECT_RE = re.compile(
        r"^\s*SELECT\s+(?P<cols>.+?)\s+FROM\s+wallet_memory\.(?P<table>[a-zA-Z0-9_]+)\s*(?P<rest>.*)$",
        re.DOTALL | re.IGNORECASE,
    )
    _ORDER_RE = re.compile(r"\s+ORDER\s+BY\s+(?P<order>.*?)(?=\s+LIMIT\s|\s+OFFSET\s|$)", re.DOTALL | re.IGNORECASE)
    _LIMIT_RE = re.compile(r"\s+LIMIT\s+(?P<limit>\d+)", re.IGNORECASE)
    _OFFSET_RE = re.compile(r"\s+OFFSET\s+(?P<offset>\d+)", re.IGNORECASE)
    _COND_RE = re.compile(
        r"^\s*(?P<col>[a-zA-Z_][a-zA-Z0-9_]*)\s*(?P<op>=|>=|<=|>|<)\s*%\((?P<param>[a-zA-Z0-9_]+)\)s\s*$"
    )
    _BETWEEN_RE = re.compile(
        r"^\s*(?P<col>[a-zA-Z_][a-zA-Z0-9_]*)\s*BETWEEN\s*%\((?P<lo>[a-zA-Z0-9_]+)\)s\s*AND\s*%\((?P<hi>[a-zA-Z0-9_]+)\)s\s*$"
    )

    def _parse_sql(self: ThaApi, sql: str) -> tuple[str, list[str], str, str, int, int] | None:
        """Parse a simple single-table SELECT (or return None)."""
        stmt = sql.strip().rstrip(";").strip()
        if not stmt or ";" in stmt:
            return None
        if not (stmt.upper().startswith("SELECT") or stmt.upper().startswith("WITH")):
            return None
        upper = " " + stmt.upper() + " "
        if any(kw in upper for kw in _UNCLEAN_KEYWORDS):
            return None

        m = self._SELECT_RE.match(stmt)
        if m is None:
            return None
        cols_text = m.group("cols").strip()
        table = m.group("table")
        rest = m.group("rest")

        # Table must be a supported event table.
        if table == "solana_spl_balance_changes":
            table = "solana_transfers"
        if table not in self.AVAILABLE_TABLES:
            return None
        select_cols = _parse_select_cols(cols_text)
        if select_cols is None:
            return None

        where_body = _cut_where(rest)
        order_by = ""
        om = self._ORDER_RE.search(rest)
        if om:
            order_by = om.group("order").strip()
        lm = self._LIMIT_RE.search(rest)
        limit = int(lm.group("limit")) if lm else 1000
        om_off = self._OFFSET_RE.search(rest)
        offset = int(om_off.group("offset")) if om_off else 0
        return table, select_cols, where_body, order_by, limit, offset

    def _extract_bounds(
        self,
        where_body: str,
        params: dict[str, Any],
    ) -> tuple[str, int | None, int | None, datetime | None, datetime | None, str] | None:
        """Extract chain/block/time bounds and a portable WHERE fragment.

        Returns None when any WHERE conjunct is not portable (ClickHouse-
        only functions, OR-joins, literals without params) — the caller
        falls back to the hot path.
        """
        bounds = _Bounds()

        if not where_body:
            return bounds.as_tuple()

        for conjunct in re.split(r"\s+AND\s+", where_body, flags=re.IGNORECASE):
            c = conjunct.strip().strip("()").strip()
            if not c:
                continue
            if " OR " in c.upper():
                return None
            if not self._apply_conjunct(c, params, bounds):
                return None

        return bounds.as_tuple()

    def _apply_conjunct(self, c: str, params: dict[str, Any], bounds: _Bounds) -> bool:
        """Apply one WHERE conjunct to the bounds; False when non-portable."""
        m = self._COND_RE.match(c)
        if m:
            return bounds.apply_cond(m.group("col").lower(), m.group("op"), m.group("param"), params)
        mb = self._BETWEEN_RE.match(c)
        if mb:
            return bounds.apply_between(mb.group("col").lower(), mb.group("lo"), mb.group("hi"), params)
        return False  # non-portable conjunct

    _lit = staticmethod(_sql_lit)

    @staticmethod
    def _strip_limit(sql: str) -> str:
        """Remove a trailing LIMIT/OFFSET so the hot side returns all rows."""
        return re.sub(r"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*$", "", sql, flags=re.IGNORECASE)


def _parse_select_cols(cols_text: str) -> list[str] | None:
    """Parse the SELECT list; None when non-portable (functions/aliases)."""
    if cols_text == "*":
        return []
    select_cols = [c.strip() for c in cols_text.split(",")]
    if not select_cols or any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", c) for c in select_cols):
        return None
    return select_cols


def _cut_where(rest: str) -> str:
    """Extract the WHERE body from the statement tail (cut at ORDER/LIMIT)."""
    rest_upper = rest.upper()
    if "WHERE" not in rest_upper:
        return ""
    where_body = rest[rest_upper.index("WHERE") + len("WHERE") :]
    for kw in ("ORDER BY", "LIMIT", "OFFSET"):
        idx = where_body.upper().find(kw)
        if idx != -1:
            where_body = where_body[:idx]
    return where_body.strip()
