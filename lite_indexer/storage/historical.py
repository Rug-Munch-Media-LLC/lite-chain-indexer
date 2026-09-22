"""
Transparent Historical Access (THA) — unified hot + cold reads.

The indexer keeps the last ~30 days in ClickHouse (hot) and the full
history back to genesis in B2 parquet (cold, daily exports). This layer
is the single access point that spans both tiers: callers ask for a
table, chain, block/time range and get merged rows back without ever
knowing or caring where the data lives.

* Hot rows (< the table's TTL window) are read from ClickHouse — fast.
* Cold rows (older) are read from B2 parquet via DuckDB
  (:class:`lite_indexer.storage.cold_store.ColdStore`).
* Ranges that span both tiers are split, executed on both sides, then
  merged with ``_smart_merge`` (hot wins on overlap, global ordering
  preserved).

The API wires this in two complementary ways (both opt-in via
``HISTORICAL_ACCESS_ENABLED``):

1. ``maybe_rewrite_view`` — single-table SELECTs over a supported event
   table are transparently re-pointed at the ClickHouse ``*_historical``
   view (migrations/006_historical_views.sql) which UNIONs hot + cold.
2. ``query_sql`` — for simple, portable single-table SELECTs (no
   ClickHouse-only functions, no joins) the Python engine splits the
   query, merges hot + cold, and applies ORDER/LIMIT after the merge.

Split across sibling modules (this file holds the core class):

* ``historical_tables.py``  — per-table metadata + row helpers
* ``historical_merge.py``   — tier planning + hot/cold merge (mixin)
* ``historical_query.py``   — unified query + tier readers (mixin)
* ``historical_view.py``    — ``*_historical`` view rewrite (mixin)
* ``historical_sqlparse.py``— conservative SQL parsing (mixin)
* ``historical_sqlrun.py``  — unified SQL routing (mixin)
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any

from lite_indexer.config import (
    CH_DATABASE,
    HISTORICAL_CACHE_MAX,
    HISTORICAL_HOT_TTL_DAYS,
    RETENTION_DAYS,
)
from lite_indexer.logging import get_logger
from lite_indexer.storage.cold_store import TABLE_COLUMNS, TABLE_KINDS, ColdStore
from lite_indexer.storage.historical_query import HistoricalAccessQuery
from lite_indexer.storage.historical_sqlrun import HistoricalAccessSQL
from lite_indexer.storage.historical_tables import asyncio_to_thread_with_timeout
from lite_indexer.storage.historical_view import HistoricalAccessView
from lite_indexer.util.bounded import BoundedDict

logger = get_logger(__name__)

__all__ = [
    "HistoricalAccess",
    "TABLE_KINDS",
    "TABLE_COLUMNS",
    "asyncio_to_thread_with_timeout",
]


class HistoricalAccess(HistoricalAccessSQL, HistoricalAccessQuery, HistoricalAccessView):
    """Transparent access to hot (ClickHouse) + cold (B2 parquet) data.

    Args:
        ch_client: A ClickHouse client (anything with ``execute``), or
            None when ClickHouse is unavailable (hot reads return []).
        b2_bucket: rclone remote root of the cold parquet archive
            (e.g. ``b2-crypt:chain-data/parquet``).
        rclone_binary: Path to the rclone binary.
        cold_store: Optional pre-built :class:`ColdStore` (injected in
            tests); defaults to a store over ``b2_bucket``.
        client_factory: Optional zero-arg callable returning a ClickHouse
            client lazily (used by the API so constructing the layer does
            not force a connection at startup).
        hot_ttl_days: Optional override for the hot window in days;
            defaults to auto-detection from the table TTL.
    """

    AVAILABLE_TABLES = [
        "evm_transfers",
        "dex_events",
        "deployer_events",
        "internal_transfers",
        "solana_transfers",
    ]

    def __init__(
        self,
        ch_client: Any | None = None,
        b2_bucket: str = "",
        rclone_binary: str = "rclone",
        cold_store: ColdStore | None = None,
        client_factory: Any | None = None,
        hot_ttl_days: int | None = None,
    ) -> None:
        self.ch = ch_client
        self._client_factory = client_factory
        self.b2_bucket = b2_bucket
        self.rclone = rclone_binary
        self._cold = cold_store or ColdStore(remote=b2_bucket or "", rclone_bin=rclone_binary)
        self._hot_ttl_days = hot_ttl_days
        self._ttl_cache: BoundedDict[str, int] = BoundedDict(HISTORICAL_CACHE_MAX)
        self._view_cache: BoundedDict[str, bool] = BoundedDict(HISTORICAL_CACHE_MAX)
        self._view_checked: float = 0.0

    # ── Clients / helpers ────────────────────────────────────────────

    def _client(self) -> Any | None:
        """Resolve the ClickHouse client (lazily via ``client_factory``)."""
        if self.ch is None and self._client_factory is not None:
            try:
                self.ch = self._client_factory()
            except Exception as exc:  # noqa: BLE001 — lazy connect is best-effort
                logger.warning("tha_client_resolve_failed", error=str(exc)[:100])
        return self.ch

    def _check_table(self, table: str) -> str:
        """Validate a logical table name against the supported set."""
        if table not in self.AVAILABLE_TABLES:
            raise ValueError(
                f"unknown table '{table}' — expected one of {', '.join(self.AVAILABLE_TABLES)}"
            )
        return table

    def _detect_ttl(self, table: str) -> int:
        """Return the hot retention window (days) for ``table``.

        Detection order: explicit override → ``RETENTION_DAYS`` config
        mirror (the "existing tables" source of truth) → the table's TTL
        expression in ``system.tables`` → ``HISTORICAL_HOT_TTL_DAYS``.
        Non-fatal: any failure falls back to the config value.
        """
        cached = self._ttl_cache.get(table)
        if cached is not None:
            return cached

        base = RETENTION_DAYS.get(_table_key(table), HISTORICAL_HOT_TTL_DAYS)
        client = self._client()
        if client is not None:
            base = _probe_ttl(client, table, base)
        self._ttl_cache[table] = base
        return base

    def hot_cutoff(self, table: str) -> datetime:
        """Return the UTC boundary between hot and cold for ``table``."""
        days = self._hot_ttl_days or self._detect_ttl(table)
        return datetime.now(UTC) - timedelta(days=days)


def _table_key(table: str) -> str:
    """Unqualified ClickHouse table name for a THA logical table."""
    from lite_indexer.storage.historical_tables import _TABLE_NAME

    return _TABLE_NAME.get(table, table)


def _probe_ttl(client: Any, table: str, base: int) -> int:
    """Best-effort TTL detection from ``system.tables`` (falls back to base)."""
    from lite_indexer.storage.historical_tables import _TABLE_NAME

    try:
        rows = client.execute(
            "SELECT ttl FROM system.tables WHERE database = %(db)s AND table = %(name)s",
            {"db": CH_DATABASE, "name": _TABLE_NAME.get(table, table)},
        )
        for (expr,) in rows:
            text = str(expr or "")
            m = re.search(r"toIntervalDay\((\d+)\)|INTERVAL\s+(\d+)\s+DAY", text)
            if m:
                return int(m.group(1) or m.group(2))
    except Exception as exc:  # noqa: BLE001 — detection is best-effort
        logger.debug("tha_ttl_detect_failed", table=table, error=str(exc)[:80])
    return base
