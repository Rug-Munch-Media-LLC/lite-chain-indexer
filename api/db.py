"""ClickHouse read-only client for the Chain Query API.

Uses the native protocol (port 9000) via ``clickhouse_driver`` — the same
library the indexer writes with. A thread-local client pool keeps one
connection per worker thread (``clickhouse_driver.Client`` is not
thread-safe), so FastAPI's threadpool worker threads never share a
connection concurrently.

Every query is executed with guardrails passed as per-query ClickHouse
settings: ``max_execution_time`` (kill long scans) and
``max_result_rows`` / ``max_block_size`` (bounded memory). The DB user is
granted SELECT-only on ``wallet_memory.*`` — the server refuses writes
even if application validation is bypassed.

Transparent Historical Access (THA): when ``HISTORICAL_ACCESS_ENABLED``,
``query()`` consults :class:`lite_indexer.storage.historical.HistoricalAccess`
so reads over supported event tables span hot ClickHouse + cold B2
parquet with zero API changes:

* ``maybe_rewrite_view`` re-points single-table SELECTs at the unified
  ``*_historical`` ClickHouse views (migrations/006_historical_views.sql)
  when they exist — ClickHouse applies the exact same filters to both
  tiers.
* ``query_sql`` routes simple portable SELECTs through the Python
  split/merge engine as a fallback.

Anything not safely routable falls back to the original hot-path SQL, so
behavior is unchanged unless a query can genuinely span both tiers.
"""

from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, cast

from clickhouse_driver import Client

from api.config import (
    CH_DATABASE,
    CH_HOST,
    CH_PASSWORD,
    CH_PORT,
    CH_QUERY_MAX_RESULT_ROWS,
    CH_QUERY_TIMEOUT,
    CH_USER,
)
from lite_indexer.config import HISTORICAL_ACCESS_ENABLED, HISTORICAL_PARQUET_REMOTE

# Tables the public API may read. Every /query statement is checked
# against this allowlist at the application layer (the DB user is also
# restricted to SELECT on wallet_memory.* as defense-in-depth).
ALLOWED_TABLES: frozenset[str] = frozenset(
    {
        "evm_transfers",
        "dex_events",
        "deployer_events",
        "solana_spl_balance_changes",
        "wallet_labels",
        "holder_balances",
        "token_meta_ref",
        "scam_addresses",
        "wallet_profiles",
        "wallet_entities",
        "internal_transfers",
        "solana_transfers",
        "bridge_mappings",
        "token_prices",
        "dex_ohlcv_hourly",
        # Launch + bundle detection tables (migrations 007/008/009/011).
        # Written by the launch/bundle strategies and read by
        # /api/v1/launches and /api/v1/bundles (api/routers/launches.py)
        # as well as raw /query.
        "solana_launches",
        "tron_launches",
        "solana_bundles",
        "evm_launches",
    }
)

# Table alias → canonical table for /query FROM/JOIN resolution.
TABLE_ALIASES: dict[str, str] = {
    "transfers": "evm_transfers",
    "dex": "dex_events",
    "deployers": "deployer_events",
    "deployer": "deployer_events",
    "solana_balance_changes": "solana_spl_balance_changes",
    "solana": "solana_spl_balance_changes",
    "labels": "wallet_labels",
    "balances": "holder_balances",
    "holder_balance": "holder_balances",
    "token_meta": "token_meta_ref",
    "ohlcv": "dex_ohlcv_hourly",
    "solana_launch": "solana_launches",
    "tron_launch": "tron_launches",
    "evm_launch": "evm_launches",
    "bundle": "solana_bundles",
    "bundles": "solana_bundles",
}

_local = threading.local()

# Lazy Transparent Historical Access singleton (None when disabled).
_historical: Any | None = None
_historical_init = False

# Lazy on-demand QueryRouter singleton (None when disabled).
_query_router: Any | None = None
_query_router_init = False


def get_historical() -> Any | None:
    """Return the shared :class:`HistoricalAccess` (None when disabled).

    Constructed lazily on first use — no ClickHouse connection is forced
    at app creation (the access layer resolves its client on demand).
    """
    global _historical, _historical_init
    if _historical_init:
        return _historical
    _historical_init = True
    if not HISTORICAL_ACCESS_ENABLED:
        return None
    try:
        from lite_indexer.storage.historical import HistoricalAccess

        _historical = HistoricalAccess(
            client_factory=get_client,
            b2_bucket=HISTORICAL_PARQUET_REMOTE,
        )
    except Exception:  # noqa: BLE001 — THA is optional
        _historical = None
    return _historical


def get_query_router() -> Any | None:
    """Return the shared on-demand :class:`QueryRouter` (None when disabled).

    The "no-full-indexing" layer: when the indexed stores (ClickHouse +
    B2 parquet) cannot answer a query, the router falls through to the
    chain's RPC pool on demand (cached, rate-limit protected). Constructed
    lazily on first use and gated by ``ON_DEMAND_RPC_ENABLED`` — no
    ClickHouse connection or RPC call is made at app creation.
    """
    global _query_router, _query_router_init
    if _query_router_init:
        return _query_router
    _query_router_init = True
    from lite_indexer.config import ON_DEMAND_RPC_ENABLED

    if not ON_DEMAND_RPC_ENABLED:
        return None
    try:
        from lite_indexer.query import QueryRouter
        from lite_indexer.storage.cold_store import ColdStore

        _query_router = QueryRouter(
            ch_client=None,
            cold_store=ColdStore(remote=HISTORICAL_PARQUET_REMOTE),
            client_factory=get_client,
        )
    except Exception:  # noqa: BLE001 — on-demand routing is optional
        _query_router = None
    return _query_router


def get_client() -> Client:
    """Return a thread-local ClickHouse client (lazily connected)."""
    client: Client | None = getattr(_local, "client", None)
    if client is None:
        client = Client(
            host=CH_HOST,
            port=CH_PORT,
            user=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE,
            connect_timeout=5,
            send_receive_timeout=CH_QUERY_TIMEOUT,
        )
        client.execute("SELECT 1")
        _local.client = client
    return client


def _query_settings(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """Per-query ClickHouse settings: bounded time + memory + result size."""
    settings: dict[str, Any] = {
        "max_execution_time": CH_QUERY_TIMEOUT,
        "max_result_rows": CH_QUERY_MAX_RESULT_ROWS,
        "max_block_size": 8192,
    }
    if extra:
        settings.update(extra)
    return settings


def query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Execute a read-only SELECT and return rows as dicts.

    When Transparent Historical Access is enabled, single-table SELECTs
    over supported event tables are transparently served from the unified
    hot + cold layer (ClickHouse + B2 parquet). Every other statement
    takes the original ClickHouse hot path unchanged.

    Raises:
        clickhouse_driver.errors.ServerException: on ClickHouse errors
            (invalid SQL, timeout, access denied).
    """
    historical = get_historical()
    if historical is not None:
        rewritten = historical.maybe_rewrite_view(sql)
        if rewritten != sql:
            return _query_clickhouse(rewritten, params)
        routed = historical.query_sql(sql, params or {})
        if routed is not None:
            return cast(list[dict[str, Any]], routed)
    return _query_clickhouse(sql, params)


def _query_clickhouse(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Execute SQL against ClickHouse and return rows as dicts."""
    rows, column_types = get_client().execute(
        sql,
        params or {},
        with_column_types=True,
        settings=_query_settings(),
    )
    columns = [name for name, _ in column_types]
    return [dict(zip(columns, row, strict=True)) for row in rows]


def query_rows(
    sql: str,
    params: dict[str, Any] | None = None,
    settings: dict[str, Any] | None = None,
) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
    """Execute SQL and return raw (rows, column_types) — used by /query.

    Keeps the raw shape so the raw-SQL endpoint can return the exact
    columns the caller selected, including non-string values. Raw SQL over
    arbitrary allowlisted tables stays on the ClickHouse hot path — cold
    routing applies to the structured event-table reads in ``query()``.
    """
    return get_client().execute(  # type: ignore[no-any-return]
        sql,
        params or {},
        with_column_types=True,
        settings=_query_settings(settings),
    )


def ping() -> tuple[bool, str]:
    """Return (ok, detail) for the health check."""
    try:
        client = get_client()
        version_rows = client.execute("SELECT version()")
        version = str(version_rows[0][0]) if version_rows else "unknown"
        return True, version
    except Exception as exc:  # noqa: BLE001 — health must never raise
        return False, str(exc)[:200]


def table_stats() -> dict[str, dict[str, Any]]:
    """Return row counts + approximate freshness per allowed table.

    Queries system.tables / system.columns (granted to chain_api) so the
    status endpoint can report warehouse health without reading the data
    tables themselves.
    """
    rows, column_types = get_client().execute(
        "SELECT database, table, engine, total_rows "
        "FROM system.tables "
        "WHERE database = %(db)s AND total_rows > 0 "
        "ORDER BY total_rows DESC",
        {"db": CH_DATABASE},
        with_column_types=True,
        settings=_query_settings(),
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        db, table, engine, total_rows = row
        result[str(table)] = {
            "database": db,
            "engine": engine,
            "rows": int(total_rows or 0),
        }
    return result


def normalize_date(value: str | None) -> datetime | None:
    """Parse an ISO date/datetime string into a datetime, or None.

    Accepts ``YYYY-MM-DD`` and ``YYYY-MM-DD HH:MM:SS`` (and full ISO
    8601). Returns None when the input is None or unparsable.
    """
    if not value:
        return None
    text = value.strip()
    if len(text) == 10:
        return datetime.strptime(text, "%Y-%m-%d")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
