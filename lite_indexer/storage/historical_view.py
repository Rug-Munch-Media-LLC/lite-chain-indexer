"""``*_historical`` view rewrite for Transparent Historical Access (mixin).

Extracted from ``storage/historical.py``: transparently re-pointing
single-table SELECTs at the ClickHouse ``*_historical`` views (which UNION
hot + cold) when the statement is a clean SELECT over one supported table
and the view exists.
"""

from __future__ import annotations

import re
import time
from typing import cast

from lite_indexer.config import CH_DATABASE
from lite_indexer.logging import get_logger
from lite_indexer.storage.historical_tables import _TABLE_NAME, _UNCLEAN_KEYWORDS, _VIEW_CHECK_TTL
from lite_indexer.storage.tha_api import ThaApi

logger = get_logger(__name__)


class HistoricalAccessView:
    """View-rewrite mixin (API wiring path 1)."""

    def maybe_rewrite_view(self: ThaApi, sql: str) -> str:
        """Return ``sql`` re-pointed at the unified ``*_historical`` view.

        Rewrites ``FROM wallet_memory.<table>`` → ``FROM
        wallet_memory.<table>_historical`` when (a) historical access is
        available, (b) the statement is a SELECT over exactly one
        supported event table, and (c) the view exists in ClickHouse.
        Returns the original SQL otherwise (safe no-op).

        The ``*_historical`` views (migrations/006_historical_views.sql)
        UNION hot ClickHouse + cold B2 parquet, so ClickHouse applies the
        caller's exact filters to both tiers.
        """
        table = self._referenced_table(sql)
        if table is None or not self._view_available(table):
            return sql
        return re.sub(rf"wallet_memory\.{table}\b", f"wallet_memory.{table}_historical", sql, count=1)

    def _referenced_table(self: ThaApi, sql: str) -> str | None:
        """Return the single supported event table referenced by ``sql``.

        Returns None when the statement is not a clean SELECT over exactly
        one supported ``wallet_memory.<table>`` (joins, subqueries, other
        tables, write keywords).
        """
        if not isinstance(sql, str) or not sql.strip():
            return None
        upper = " " + sql.upper() + " "
        if not (upper.lstrip().startswith("SELECT") or upper.lstrip().startswith("WITH")):
            return None
        if any(kw in upper for kw in _UNCLEAN_KEYWORDS):
            return None
        refs = re.findall(r"wallet_memory\.([a-zA-Z0-9_]+)", sql)
        if not refs:
            return None
        distinct = {r for r in refs}
        if len(distinct) != 1:
            return None
        table = next(iter(distinct))
        if table not in self.AVAILABLE_TABLES and table not in {
            "solana_spl_balance_changes",
            "internal_transfers",
        }:
            return None
        # Normalize solana_spl_balance_changes to the THA logical name.
        if table == "solana_spl_balance_changes":
            return "solana_transfers"
        return cast(str, table)

    def _view_available(self: ThaApi, table: str) -> bool:
        """True when ``wallet_memory.<table>_historical`` exists (cached)."""
        now = time.monotonic()
        if now - self._view_checked > _VIEW_CHECK_TTL:
            self._view_cache.clear()
            self._view_checked = now
        if table in self._view_cache:
            return self._view_cache[table]
        view = f"{_TABLE_NAME[table]}_historical"
        ok = False
        client = self._client()
        if client is not None:
            try:
                rows = client.execute(
                    "SELECT name FROM system.tables WHERE database = %(db)s AND name = %(view)s",
                    {"db": CH_DATABASE, "view": view},
                )
                ok = bool(rows)
            except Exception as exc:  # noqa: BLE001 — view probe is best-effort
                logger.debug("tha_view_probe_failed", view=view, error=str(exc)[:80])
        self._view_cache[table] = ok
        return ok
