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
   ClickHouse applies the exact same filters to both tiers, so every
   existing endpoint gets full history with zero semantics risk.
2. ``query_sql`` — for simple, portable single-table SELECTs (no
   ClickHouse-only functions, no joins) the Python engine splits the
   query, merges hot + cold, and applies ORDER/LIMIT after the merge.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from lite_indexer.config import (
    CH_DATABASE,
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_SOLANA_TABLE,
    CH_TABLE,
    CH_TRACE_TABLE,
    COLD_QUERY_TIMEOUT,
    HISTORICAL_CACHE_MAX,
    HISTORICAL_HOT_TTL_DAYS,
    RETENTION_DAYS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.cold_store import TABLE_COLUMNS, TABLE_KINDS, ColdStore
from lite_indexer.util.bounded import BoundedDict

logger = get_logger(__name__)

# ClickHouse table reference per THA logical table name.
_TABLE_REF: dict[str, str] = {
    "evm_transfers": CH_TABLE,
    "dex_events": CH_DEX_TABLE,
    "deployer_events": CH_DEPLOYER_TABLE,
    "internal_transfers": CH_TRACE_TABLE,
    "solana_transfers": CH_SOLANA_TABLE,
}

# Logical table → unqualified ClickHouse table name (for view names and
# system.tables lookups).
_TABLE_NAME: dict[str, str] = {
    "evm_transfers": "evm_transfers",
    "dex_events": "dex_events",
    "deployer_events": "deployer_events",
    "internal_transfers": "internal_transfers",
    "solana_transfers": "solana_spl_balance_changes",
}

# Row identity per table (used for hot/cold dedupe).
_ROW_KEY_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("chain", "block_number", "tx_hash", "log_index"),
    "dex_events": ("chain", "block_number", "tx_hash", "log_index"),
    "deployer_events": ("chain", "block_number", "tx_hash", "token_address", "pair_address"),
    "internal_transfers": ("chain", "block_number", "tx_hash", "trace_address"),
    "solana_transfers": ("chain", "slot", "tx_hash", "mint", "token_account", "owner"),
}

# Sort columns per table for the merged result (descending). Solana has no
# log_index; deployer_events/internal_transfers have no log_index either.
_SORT_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("block_timestamp", "block_number", "log_index"),
    "dex_events": ("block_timestamp", "block_number", "log_index"),
    "deployer_events": ("block_timestamp", "block_number"),
    "internal_transfers": ("block_timestamp", "block_number", "tx_hash"),
    "solana_transfers": ("block_timestamp", "slot"),
}

# How long (seconds) a *_historical view-existence check is cached.
_VIEW_CHECK_TTL = 60

# Conservative SQL routing: reject statements that reference anything
# other than a plain SELECT over one supported event table.
_UNCLEAN_KEYWORDS = (
    " INSERT ",
    " ALTER ",
    " CREATE ",
    " DROP ",
    " TRUNCATE ",
    " DELETE ",
    " UPDATE ",
    " GRANT ",
    " INTO ",
    " JOIN ",
    " GROUP BY ",
    " HAVING ",
    " UNION ",
)


@dataclass
class _Split:
    """Whether a query range needs the hot and/or cold tier."""

    hot: bool
    cold: bool


class HistoricalAccess:
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

    AVAILABLE_TABLES = ["evm_transfers", "dex_events", "deployer_events", "internal_transfers", "solana_transfers"]

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
            raise ValueError(f"unknown table '{table}' — expected one of {', '.join(self.AVAILABLE_TABLES)}")
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

        base = RETENTION_DAYS.get(_TABLE_NAME.get(table, table), HISTORICAL_HOT_TTL_DAYS)
        client = self._client()
        if client is not None:
            try:
                rows = client.execute(
                    "SELECT ttl FROM system.tables WHERE database = %(db)s AND table = %(name)s",
                    {"db": CH_DATABASE, "name": _TABLE_NAME.get(table, table)},
                )
                for (expr,) in rows:
                    text = str(expr or "")
                    m = re.search(r"toIntervalDay\((\d+)\)|INTERVAL\s+(\d+)\s+DAY", text)
                    if m:
                        base = int(m.group(1) or m.group(2))
                        break
            except Exception as exc:  # noqa: BLE001 — detection is best-effort
                logger.debug("tha_ttl_detect_failed", table=table, error=str(exc)[:80])
        self._ttl_cache[table] = base
        return base

    def hot_cutoff(self, table: str) -> datetime:
        """Return the UTC boundary between hot and cold for ``table``."""
        days = self._hot_ttl_days or self._detect_ttl(table)
        return datetime.now(UTC) - timedelta(days=days)

    # ── Unified query ────────────────────────────────────────────────

    async def query(
        self,
        table: str,
        chain: str,
        from_block: int | None = None,
        to_block: int | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        where: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Unified query — automatically splits across hot/cold as needed.

        * Range entirely within the hot TTL → ClickHouse only (fast).
        * Range entirely cold → B2 parquet only (DuckDB).
        * Range spanning both → split, query both, merge transparently.

        ``where`` is a portable SQL fragment (column = value predicates,
        no ClickHouse-only functions) applied to both sides.

        Args:
            table: One of :attr:`AVAILABLE_TABLES`.
            chain: Chain name filter (empty = all chains).
            from_block/to_block: Inclusive block/slot bounds.
            from_time/to_time: Inclusive ``block_timestamp`` bounds.
            where: Extra portable predicate fragment (no leading AND).
            limit: Cap on returned rows (enforced after the merge).

        Returns:
            Merged rows as dicts, newest first.
        """
        self._check_table(table)
        limit = int(limit) if limit is not None else 1000
        split = self._plan(table, from_block, to_block, from_time, to_time)

        hot_rows: list[dict[str, Any]] = []
        cold_rows: list[dict[str, Any]] = []
        if split.hot:
            hot_rows = await self._query_hot(table, chain, from_block, to_block, from_time, to_time, where, limit)
        if split.cold:
            cold_rows = await self._query_cold(table, chain, from_block, to_block, from_time, to_time, where, limit)
        return self._smart_merge(table, hot_rows, cold_rows, limit)

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

    async def _query_hot(
        self,
        table: str,
        chain: str,
        from_block: int | None,
        to_block: int | None,
        from_time: datetime | None,
        to_time: datetime | None,
        where: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """ClickHouse query for recent (hot) data.

        Returns [] when ClickHouse is unavailable or the query fails —
        the unified layer degrades to the cold result instead of failing.
        """
        client = self._client()
        if client is None:
            metrics_incr("tha_hot_skipped")
            return []

        clauses: list[str] = []
        params: dict[str, Any] = {}
        block_col = _block_col(table)
        if chain:
            clauses.append("chain = %(chain)s")
            params["chain"] = chain
        if from_block is not None:
            clauses.append(f"{block_col} >= %(from_block)s")
            params["from_block"] = from_block
        if to_block is not None:
            clauses.append(f"{block_col} <= %(to_block)s")
            params["to_block"] = to_block
        if from_time is not None:
            clauses.append("block_timestamp >= %(from_time)s")
            params["from_time"] = from_time
        if to_time is not None:
            clauses.append("block_timestamp <= %(to_time)s")
            params["to_time"] = to_time
        if where:
            clauses.append(f"({where})")
        where_sql = " AND ".join(clauses) if clauses else "1=1"

        cols = ", ".join(TABLE_COLUMNS[table])
        order = ", ".join(f"{c} DESC" for c in _SORT_COLS[table])
        sql = f"SELECT {cols} FROM {_TABLE_REF[table]} WHERE {where_sql} ORDER BY {order} LIMIT {int(limit)}"
        metrics_incr("tha_hot_queries")
        try:
            rows, column_types = client.execute(
                sql, params, with_column_types=True
            )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        except Exception as exc:  # noqa: BLE001 — hot reads are best-effort
            metrics_incr("tha_hot_errors")
            logger.warning("tha_hot_query_failed", table=table, error=str(exc)[:150])
            return []
        names = [name for name, _ in column_types]
        return [dict(zip(names, row, strict=False)) for row in rows]

    async def _query_cold(
        self,
        table: str,
        chain: str,
        from_block: int | None,
        to_block: int | None,
        from_time: datetime | None,
        to_time: datetime | None,
        where: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        """DuckDB query over B2 parquet for historical (cold) data.

        Runs the blocking DuckDB read in a worker thread bounded by
        ``COLD_QUERY_TIMEOUT`` (DuckDB has no cancellable statement
        timeout, so the timeout bounds the caller's wait; the worker
        thread finishes in the background).
        """
        try:
            return cast(
                list[dict[str, Any]],
                await asyncio_to_thread_with_timeout(
                    self._cold.query,
                    COLD_QUERY_TIMEOUT,
                    table,
                    chain,
                    from_block=from_block,
                    to_block=to_block,
                    from_time=from_time,
                    to_time=to_time,
                    where=where,
                    limit=limit,
                ),
            )
        except TimeoutError:
            metrics_incr("tha_cold_timeouts")
            logger.warning("tha_cold_timeout", table=table, timeout=COLD_QUERY_TIMEOUT)
            return []
        except Exception as exc:  # noqa: BLE001 — cold reads are best-effort
            metrics_incr("tha_cold_errors")
            logger.warning("tha_cold_query_failed", table=table, error=str(exc)[:150])
            return []

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

    # ── API wiring ───────────────────────────────────────────────────

    def maybe_rewrite_view(self, sql: str) -> str:
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

    def _referenced_table(self, sql: str) -> str | None:
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

    def _view_available(self, table: str) -> bool:
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

    def query_sql(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]] | None:
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

        # Hot side: original SQL minus LIMIT/OFFSET (re-applied post-merge).
        hot_sql = self._strip_limit(sql)
        hot_rows: list[dict[str, Any]] = []
        client = self._client()
        if client is not None:
            try:
                rows, column_types = client.execute(hot_sql, params or {}, with_column_types=True)
                names = [name for name, _ in column_types]
                hot_rows = [dict(zip(names, row, strict=False)) for row in rows]
            except Exception as exc:  # noqa: BLE001 — hot side is best-effort
                logger.warning("tha_sql_hot_failed", table=table, error=str(exc)[:150])

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

    # ── Conservative SQL parsing ─────────────────────────────────────

    _SELECT_RE = re.compile(
        r"^\s*SELECT\s+(?P<cols>.+?)\s+FROM\s+wallet_memory\.(?P<table>[a-zA-Z0-9_]+)\s*(?P<rest>.*)$",
        re.DOTALL | re.IGNORECASE,
    )
    _ORDER_RE = re.compile(r"\s+ORDER\s+BY\s+(?P<order>.*?)(?=\s+LIMIT\s|\s+OFFSET\s|$)", re.DOTALL | re.IGNORECASE)
    _LIMIT_RE = re.compile(r"\s+LIMIT\s+(?P<limit>\d+)", re.IGNORECASE)
    _OFFSET_RE = re.compile(r"\s+OFFSET\s+(?P<offset>\d+)", re.IGNORECASE)

    def _parse_sql(self, sql: str) -> tuple[str, list[str], str, str, int, int] | None:
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
        # Bare `*` or a comma list of plain identifiers only.
        if cols_text == "*":
            select_cols: list[str] = []
        else:
            select_cols = [c.strip() for c in cols_text.split(",")]
            if not select_cols or any(not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", c) for c in select_cols):
                return None

        where_body = ""
        rest_upper = rest.upper()
        if "WHERE" in rest_upper:
            where_body = rest[rest_upper.index("WHERE") + len("WHERE") :]
            # Cut at ORDER BY / LIMIT / OFFSET (case-insensitive).
            for kw in ("ORDER BY", "LIMIT", "OFFSET"):
                idx = where_body.upper().find(kw)
                if idx != -1:
                    where_body = where_body[:idx]
            where_body = where_body.strip()

        order_by = ""
        om = self._ORDER_RE.search(rest)
        if om:
            order_by = om.group("order").strip()
        lm = self._LIMIT_RE.search(rest)
        limit = int(lm.group("limit")) if lm else 1000
        om_off = self._OFFSET_RE.search(rest)
        offset = int(om_off.group("offset")) if om_off else 0
        return table, select_cols, where_body, order_by, limit, offset

    _COND_RE = re.compile(
        r"^\s*(?P<col>[a-zA-Z_][a-zA-Z0-9_]*)\s*(?P<op>=|>=|<=|>|<)\s*%\((?P<param>[a-zA-Z0-9_]+)\)s\s*$"
    )
    _BETWEEN_RE = re.compile(
        r"^\s*(?P<col>[a-zA-Z_][a-zA-Z0-9_]*)\s*BETWEEN\s*%\((?P<lo>[a-zA-Z0-9_]+)\)s\s*AND\s*%\((?P<hi>[a-zA-Z0-9_]+)\)s\s*$"
    )

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
        chain: str = ""
        from_block: int | None = None
        to_block: int | None = None
        from_time: datetime | None = None
        to_time: datetime | None = None
        portable: list[str] = []

        if not where_body:
            return chain, from_block, to_block, from_time, to_time, ""

        for conjunct in re.split(r"\s+AND\s+", where_body, flags=re.IGNORECASE):
            c = conjunct.strip().strip("()").strip()
            if not c:
                continue
            if " OR " in c.upper():
                return None
            m = self._COND_RE.match(c)
            if m:
                col, op, param = m.group("col").lower(), m.group("op"), m.group("param")
                if param not in params:
                    return None
                value = params[param]
                if isinstance(value, datetime):
                    if col in ("block_timestamp", "block_time", "timestamp"):
                        if op == ">=":
                            from_time = value
                        elif op == "<=":
                            to_time = value
                        else:
                            return None
                        continue
                    return None
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    if col in ("block_number", "block", "slot"):
                        if op == ">=":
                            from_block = int(value)
                        elif op == "<=":
                            to_block = int(value)
                        else:
                            return None
                        continue
                    return None
                if isinstance(value, str):
                    if col == "chain" and op == "=":
                        chain = value
                        continue
                    if col in ("block_timestamp", "block_time", "timestamp", "block_number", "block", "slot"):
                        return None  # string comparisons on time/block are ambiguous
                    portable.append(f"{col} {op} {self._lit(value)}")
                    continue
                return None
            mb = self._BETWEEN_RE.match(c)
            if mb:
                col = mb.group("col").lower()
                lo, hi = mb.group("lo"), mb.group("hi")
                if lo not in params or hi not in params:
                    return None
                lo_v, hi_v = params[lo], params[hi]
                if (
                    isinstance(lo_v, datetime)
                    and isinstance(hi_v, datetime)
                    and col
                    in (
                        "block_timestamp",
                        "block_time",
                        "timestamp",
                    )
                ):
                    from_time, to_time = lo_v, hi_v
                    continue
                if (
                    isinstance(lo_v, (int, float))
                    and isinstance(hi_v, (int, float))
                    and col in ("block_number", "block", "slot")
                ):
                    from_block, to_block = int(lo_v), int(hi_v)
                    continue
                return None
            return None  # non-portable conjunct

        return chain, from_block, to_block, from_time, to_time, " AND ".join(portable)

    @staticmethod
    def _lit(value: Any) -> str:
        """Render a Python scalar as a SQL literal (DuckDB quote-doubling — SEC-1)."""
        if isinstance(value, (datetime,)):
            return "'" + value.isoformat(sep=" ") + "'"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def _strip_limit(self, sql: str) -> str:
        """Remove a trailing LIMIT/OFFSET so the hot side returns all rows."""
        return re.sub(r"\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*$", "", sql, flags=re.IGNORECASE)

    @staticmethod
    def _order_rows(rows: list[dict[str, Any]], order_by: str) -> list[dict[str, Any]]:
        """Apply an ORDER BY clause (column [ASC|DESC], comma-separated)."""
        from functools import cmp_to_key

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

        def _cmp(a: dict[str, Any], b: dict[str, Any]) -> int:
            for col, desc in specs:
                va, vb = _sort_value(a, col), _sort_value(b, col)
                if va < vb:
                    return 1 if desc else -1
                if va > vb:
                    return -1 if desc else 1
            return 0

        return sorted(rows, key=cmp_to_key(_cmp))

    @staticmethod
    def _project(rows: list[dict[str, Any]], select_cols: list[str]) -> list[dict[str, Any]]:
        """Project merged rows onto the original SELECT list (if explicit)."""
        if not select_cols:
            return rows
        out: list[dict[str, Any]] = []
        for row in rows:
            out.append({col: row.get(col) for col in select_cols})
        return out


def _block_col(table: str) -> str:
    """Return the block-ordering column for a table (slot for Solana)."""
    return "slot" if table == "solana_transfers" else "block_number"


def _sort_value(row: dict[str, Any], col: str) -> tuple[int, Any]:
    """Return a comparable sort key for a row/column pair.

    Missing values sort first (bucket 0), numerics/datetimes in bucket 1,
    strings in bucket 2 — so mixed-type rows never raise on comparison.
    """
    value = row.get(col)
    if value is None or value == "":
        return (0, 0)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, (int, float)):
        return (1, float(value))
    if isinstance(value, datetime):
        return (1, value.timestamp())
    if isinstance(value, (list, tuple)):
        return (1, 0)
    return (2, str(value))


async def asyncio_to_thread_with_timeout(func: Any, timeout: float, *args: Any, **kwargs: Any) -> Any:
    """Run a blocking call in a worker thread bounded by ``timeout``.

    DuckDB has no cancellable statement timeout, so ``COLD_QUERY_TIMEOUT``
    is enforced here: the caller never waits longer than ``timeout``, even
    though the worker thread may finish in the background.
    """
    import asyncio

    return await asyncio.wait_for(asyncio.to_thread(func, *args, **kwargs), timeout=timeout)


__all__ = [
    "HistoricalAccess",
    "TABLE_KINDS",
    "TABLE_COLUMNS",
    "asyncio_to_thread_with_timeout",
]
