"""
QueryRouter — intelligently routes queries to the best data source.

The "no-full-indexing-needed" architecture. Instead of answering every
query from a genesis-complete index, the router is a smart proxy over
three tiers:

1. **ClickHouse** (fast, already indexed) — covers the blocks the indexer
   actively synced (hot).
2. **B2 parquet cold store** (historical, free) — covers blocks exported
   to the cold tier.
3. **RPC on-demand** (live, rate-limited) — covers anything else: gaps,
   pre-indexing history, or addresses that were never scanned.

Routing never says "data not available": the RPC leg is the last resort
(gated by ``ON_DEMAND_RPC_ENABLED``), so a caller can query ANY block
range even if the indexer never indexed it. RPC results are cached
(:class:`~lite_indexer.query.cache.QueryCache`) and the fetcher enforces a
consecutive-call budget so free-tier rate limits are never hammered.

The router mirrors the canonical column lists / table refs from the
storage layer so rows returned from any tier have the same shape.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from lite_indexer.config import (
    CH_ADDRESS_ACTIVITY_TABLE,
    CH_DEPLOYER_TABLE,
    CH_DEX_TABLE,
    CH_SOLANA_TABLE,
    CH_TABLE,
    CH_TRACE_TABLE,
    ON_DEMAND_RPC_ENABLED,
    ON_DEMAND_RPC_MAX_BLOCK_RANGE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.address_backfill import AddressBackfill, BackfillResult
from lite_indexer.query.biglake_source import BigLakeSource
from lite_indexer.query.cache import QueryCache
from lite_indexer.query.envio_source import EnvioSource
from lite_indexer.query.rpc_fetcher import RPCFetcher
from lite_indexer.storage.cold_store import TABLE_COLUMNS, TABLE_KINDS, TABLE_ORDER, ColdStore

logger = get_logger(__name__)

# Logical event table → ClickHouse table reference. Mirrors
# lite_indexer.storage.historical._TABLE_REF (kept local so the query
# package is self-contained).
_CH_TABLE_REF: dict[str, str] = {
    "evm_transfers": CH_TABLE,
    "dex_events": CH_DEX_TABLE,
    "deployer_events": CH_DEPLOYER_TABLE,
    "internal_transfers": CH_TRACE_TABLE,
    "solana_transfers": CH_SOLANA_TABLE,
}

# Row identity per table (hot/cold/RPC dedupe). Mirrors the THA merge.
_ROW_KEYS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("chain", "block_number", "tx_hash", "log_index"),
    "dex_events": ("chain", "block_number", "tx_hash", "log_index"),
    "deployer_events": ("chain", "block_number", "tx_hash", "token_address", "pair_address"),
    "internal_transfers": ("chain", "block_number", "tx_hash", "trace_address"),
    "solana_transfers": ("chain", "slot", "tx_hash", "mint", "token_account", "owner"),
}

# Sort columns per table (newest first). Mirrors the THA merge.
_SORT_COLS: dict[str, tuple[str, ...]] = {
    "evm_transfers": ("block_timestamp", "block_number", "log_index"),
    "dex_events": ("block_timestamp", "block_number", "log_index"),
    "deployer_events": ("block_timestamp", "block_number"),
    "internal_transfers": ("block_timestamp", "block_number", "tx_hash"),
    "solana_transfers": ("block_timestamp", "slot"),
}


def _block_col(table: str) -> str:
    """Return the block-ordering column for a table (slot for Solana)."""
    return "slot" if table == "solana_transfers" else "block_number"


def _cold_lit(value: Any) -> str:
    """Render a scalar as a DuckDB/parquet literal for cold-store WHERE."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return "1" if value else "0"
    if value is None:
        return "NULL"
    return str(value)


def _sort_value(row: dict[str, Any], col: str) -> tuple[int, Any]:
    """Comparable sort key for a row/column (mixed types never raise).

    Missing values sort first (bucket 0), numerics/datetimes in bucket 1,
    strings in bucket 2.
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


class QueryRouter:
    """Routes queries across ClickHouse → B2 cold store → live RPC.

    Args:
        ch_client: A ClickHouse client (anything with ``execute``), or
            None when ClickHouse is unavailable / resolved lazily via
            ``client_factory``.
        cold_store: A cold-tier store (``ColdStore``-compatible with a
            ``query(table, chain, *, from_block, to_block, where, limit)``
            method), or None to disable the cold tier.
        rpc_pool: An on-demand RPC fetcher (``RPCFetcher``-compatible
            with ``fetch_transfer`` / ``fetch_address_transfers`` /
            ``fetch_deployer`` / ``fetch_block_info``), or None to build a
            default :class:`RPCFetcher`.
        cache: A :class:`QueryCache` (a default is built when omitted).
        biglake: Optional :class:`BigLakeSource` (defaults to enabled —
            reads AWS's free Ethereum archive for ranges the indexed
            tiers cannot serve).
        envio: Optional :class:`EnvioSource` (defaults to enabled —
            Envio HyperRPC for bounded wallet transfers and eth/base
            trace blocks the indexed tiers missed).
        client_factory: Optional zero-arg callable returning a ClickHouse
            client lazily (used by the API so constructing the router does
            not force a connection at startup).
        on_demand_enabled: Gate for the RPC leg
            (``ON_DEMAND_RPC_ENABLED``, default True).
        max_block_range: Cap on on-demand RPC block windows.
        limit_default: Default row cap for range queries.
    """

    def __init__(
        self,
        ch_client: Any | None = None,
        cold_store: ColdStore | None = None,
        rpc_pool: RPCFetcher | None = None,
        cache: QueryCache | None = None,
        *,
        backfill: AddressBackfill | None = None,
        biglake: BigLakeSource | None = None,
        envio: EnvioSource | None = None,
        client_factory: Any | None = None,
        on_demand_enabled: bool = ON_DEMAND_RPC_ENABLED,
        max_block_range: int = ON_DEMAND_RPC_MAX_BLOCK_RANGE,
        limit_default: int = 100,
    ) -> None:
        self.ch = ch_client
        self._client_factory = client_factory
        self.cold = cold_store
        self.rpc = rpc_pool if rpc_pool is not None else RPCFetcher(max_block_range=max_block_range)
        self.cache = cache if cache is not None else QueryCache()
        self._backfill = backfill
        self.biglake = biglake if biglake is not None else BigLakeSource()
        self.envio = envio if envio is not None else EnvioSource()
        self.on_demand_enabled = bool(on_demand_enabled)
        self.max_block_range = int(max_block_range)
        self.limit_default = int(limit_default)

    # ── Client resolution ────────────────────────────────────────────

    def _ch_client(self) -> Any | None:
        """Resolve the ClickHouse client (lazily via ``client_factory``)."""
        if self.ch is None and self._client_factory is not None:
            try:
                self.ch = self._client_factory()
            except Exception as exc:  # noqa: BLE001 — lazy connect is best-effort
                logger.warning("ondemand_ch_resolve_failed", error=str(exc)[:100])
        return self.ch

    def _check_table(self, table: str) -> str:
        """Validate a logical table name against the routable set."""
        if table not in _CH_TABLE_REF or table not in TABLE_KINDS:
            raise ValueError(f"unknown table for on-demand routing: {table}")
        return table

    # ── Address-activity lazy backfill ───────────────────────────────

    def _get_backfill(self) -> AddressBackfill:
        """Return the shared :class:`AddressBackfill` (lazily constructed)."""
        if self._backfill is None:
            self._backfill = AddressBackfill(ch_client_factory=self._ch_client)
        return self._backfill

    async def backfill_address(
        self,
        chain: str,
        address: str,
        *,
        force: bool = False,
        max_tx: int | None = None,
    ) -> BackfillResult:
        """Backfill one address's history into ``address_activity`` (guarded).

        Delegates to :class:`AddressBackfill`; the never-re-fetch guard makes
        repeated calls a no-op once the address is indexed. Best-effort —
        never raises.
        """
        try:
            return await self._get_backfill().backfill_address(
                address, chain=chain, force=force, max_tx=max_tx
            )
        except Exception as exc:  # noqa: BLE001 — backfill is best-effort
            logger.warning(
                "ondemand_backfill_failed",
                chain=chain,
                address=str(address)[:12],
                error=str(exc)[:120],
            )
            return BackfillResult(
                address=str(address or ""), chain=chain, skipped=True, reason="error"
            )

    async def get_address_activity(
        self,
        chain: str,
        address: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Serve a wallet's transaction history, backfilling on first touch.

        Lazy backfill: on a cache miss the address is fetched once from the
        free RPC pool and written to ``wallet_memory.address_activity``, then
        re-read from ClickHouse. Every later call is a local read. Gated by
        ``on_demand_enabled`` and the backfill's own guard.
        """
        addr = str(address or "").strip()
        if not addr:
            return []
        capped = max(1, min(int(limit or self.limit_default), 1000))
        rows = await asyncio.to_thread(self._query_address_activity, chain, addr, capped)
        if rows or not self.on_demand_enabled:
            return rows
        try:
            await self.backfill_address(chain, addr)
        except Exception as exc:  # noqa: BLE001 — lazy backfill must not break reads
            logger.warning("ondemand_lazy_backfill_failed", chain=chain, error=str(exc)[:120])
            return rows
        return await asyncio.to_thread(self._query_address_activity, chain, addr, capped)

    def _query_address_activity(self, chain: str, address: str, limit: int) -> list[dict[str, Any]]:
        """Read one address's rows from the keystone table ([] on failure)."""
        client = self._ch_client()
        if client is None:
            return []
        sql = (
            "SELECT address, tx_signature, chain, slot, block_time, involved_accounts, program_ids, is_signer "
            f"FROM {CH_ADDRESS_ACTIVITY_TABLE} FINAL "
            "WHERE address = %(address)s AND chain = %(chain)s "
            "ORDER BY block_time DESC, slot DESC "
            f"LIMIT {int(limit)}"
        )
        try:
            rows, column_types = client.execute(
                sql, {"address": address, "chain": chain}, with_column_types=True
            )
        except Exception as exc:  # noqa: BLE001 — reads are best-effort
            metrics_incr("ondemand_ch_errors")
            logger.debug("ondemand_address_activity_failed", error=str(exc)[:100])
            return []
        names = [name for name, _ in column_types]
        return [dict(zip(names, row, strict=False)) for row in rows]

    # ── Hot / cold reads ─────────────────────────────────────────────

    def _query_ch(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query ClickHouse directly. Returns [] when unavailable/failing."""
        client = self._ch_client()
        if client is None:
            return []
        clauses = ["chain = %(chain)s"]
        params: dict[str, Any] = {"chain": chain}
        block_col = _block_col(table)
        if from_block is not None:
            clauses.append(f"{block_col} >= %(from_block)s")
            params["from_block"] = from_block
        if to_block is not None:
            clauses.append(f"{block_col} <= %(to_block)s")
            params["to_block"] = to_block
        self._append_ch_filters(clauses, params, filters)

        cols = ", ".join(TABLE_COLUMNS[table])
        order = TABLE_ORDER.get(table, "block_timestamp DESC, block_number DESC, log_index DESC")
        sql = (
            f"SELECT {cols} FROM {_CH_TABLE_REF[table]} "
            f"WHERE {' AND '.join(clauses)} ORDER BY {order} LIMIT {int(limit)}"
        )
        try:
            rows, column_types = client.execute(sql, params, with_column_types=True)
        except Exception as exc:  # noqa: BLE001 — hot reads are best-effort
            metrics_incr("ondemand_ch_errors")
            logger.debug("ondemand_ch_query_failed", table=table, error=str(exc)[:100])
            return []
        names = [name for name, _ in column_types]
        return [dict(zip(names, row, strict=False)) for row in rows]

    @staticmethod
    def _append_ch_filters(
        clauses: list[str],
        params: dict[str, Any],
        filters: dict[str, Any] | list[dict[str, Any]] | None,
    ) -> None:
        """Append AND/OR filter clauses to a ClickHouse SELECT.

        ``dict`` → AND equality on each (col → value). ``list`` of
        ``{"col": ..., "value": ...}`` → OR equality over the entries
        (used for "from_address = X OR to_address = X" address queries).
        """
        if not filters:
            return
        if isinstance(filters, dict):
            for col, value in filters.items():
                pname = f"f_{col}"
                clauses.append(f"{col} = %({pname})s")
                params[pname] = value
            return
        or_clauses: list[str] = []
        for i, item in enumerate(filters):
            col = str(item.get("col", ""))
            if not col:
                continue
            pname = f"f_{col}_{i}"
            or_clauses.append(f"{col} = %({pname})s")
            params[pname] = item.get("value")
        if or_clauses:
            clauses.append("(" + " OR ".join(or_clauses) + ")")

    def _query_cold(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query the B2 parquet cold store. Returns [] when unavailable."""
        if self.cold is None:
            return []
        where = self._build_cold_where(filters)
        try:
            return self.cold.query(
                table,
                chain,
                from_block=from_block,
                to_block=to_block,
                where=where,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001 — cold reads are best-effort
            metrics_incr("ondemand_cold_errors")
            logger.debug("ondemand_cold_query_failed", table=table, error=str(exc)[:100])
            return []

    @staticmethod
    def _build_cold_where(filters: dict[str, Any] | list[dict[str, Any]] | None) -> str:
        """Render filters as a DuckDB-compatible WHERE fragment."""
        if not filters:
            return ""
        if isinstance(filters, dict):
            return " AND ".join(f"{col} = {_cold_lit(value)}" for col, value in filters.items())
        return (
            "("
            + " OR ".join(f"{item['col']} = {_cold_lit(item['value'])}" for item in filters)
            + ")"
        )

    # ── Merge ────────────────────────────────────────────────────────

    def _merge_rows(
        self,
        table: str,
        *row_sets: list[dict[str, Any]],
        limit: int,
    ) -> list[dict[str, Any]]:
        """Merge + dedupe row sets (earlier sets win on overlap), newest first."""
        merged: dict[tuple[Any, ...], dict[str, Any]] = {}
        for rows in row_sets:
            for row in rows:
                key = self._row_key(table, row)
                if key not in merged:
                    merged[key] = row
        rows = list(merged.values())
        sort_cols = _SORT_COLS[table]
        rows.sort(key=lambda r: tuple(_sort_value(r, c) for c in sort_cols), reverse=True)
        return rows[:limit]

    @staticmethod
    def _row_key(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
        """Return the dedupe identity for a row."""
        key: list[Any] = []
        for col in _ROW_KEYS[table]:
            value = row.get(col)
            if isinstance(value, (list, tuple)):
                value = str(value)
            key.append(value)
        return tuple(key)

    # ── RPC fallback decision ────────────────────────────────────────

    def _needs_rpc_fallback(
        self,
        table: str,  # noqa: ARG002 — kept for future per-table policies
        ch_rows: list[dict[str, Any]],
        cold_rows: list[dict[str, Any]],
        from_block: int | None,
        to_block: int | None,
        limit: int,
    ) -> bool:
        """Decide whether the RPC leg should answer after the indexed stores.

        * Nothing indexed at all → yes (the "never says unavailable" rule).
        * Bounded narrow range with partial results → yes (may be a gap).
        * Bounded wide range with results → no (trust the indexed store).
        * Unbounded with results → no (hot-only API contract).
        """
        if not ch_rows and not cold_rows:
            return True
        if from_block is not None and to_block is not None:
            if (to_block - from_block) <= self.max_block_range:
                return len(ch_rows) + len(cold_rows) < limit
            return False
        return False

    # ── BigLake leg (AWS public Ethereum archive) ────────────────────

    @staticmethod
    def _filter_rows(
        table: str,  # noqa: ARG004 — kept for future per-table filter semantics
        rows: list[dict[str, Any]],
        filters: dict[str, Any] | list[dict[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        """Apply router filter semantics (dict=AND eq, list=OR eq) to rows."""
        if not filters:
            return rows

        def _matches(row: dict[str, Any], cond: dict[str, Any]) -> bool:
            return all(
                str(row.get(col, "")).lower() == str(val).lower() for col, val in cond.items()
            )

        if isinstance(filters, dict):
            return [r for r in rows if _matches(r, filters)]
        return [r for r in rows if any(_matches(r, item) for item in filters)]

    async def _biglake_route(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None,
        to_block: int | None,
        filters: dict[str, Any] | list[dict[str, Any]] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Try the AWS public lake for ranges the indexed tiers could not serve.

        Best-effort: any failure returns an empty list so routing falls
        through to the RPC leg as before.
        """
        if not self.biglake.available():
            return []
        address = self._address_from_filters(filters) or self._single_address(filters)
        try:
            rows = await asyncio.to_thread(
                self.biglake.query,
                table,
                chain,
                from_block=from_block,
                to_block=to_block,
                address=address,
                limit=limit,
            )
        except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
            logger.warning("biglake_route_failed", error=str(exc)[:120])
            return []
        filtered = self._filter_rows(table, rows, filters)
        if filtered:
            metrics_incr("biglake_routes")
        return filtered

    async def _envio_route(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None,
        to_block: int | None,
        filters: dict[str, Any] | list[dict[str, Any]] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Try Envio HyperRPC for bounded wallet queries the indexed tiers missed.

        Only serves ``evm_transfers`` address queries with an explicit
        block window; anything else falls through to the RPC leg.
        """
        if table != "evm_transfers" or from_block is None or to_block is None:
            return []
        if not self.envio.available() or chain not in EnvioSource.CHAIN_IDS:
            return []
        address = self._address_from_filters(filters) or self._single_address(filters)
        if not address:
            return []
        try:
            rows = await asyncio.to_thread(
                self.envio.address_transfers, chain, address, from_block, to_block, limit=limit
            )
        except Exception as exc:  # noqa: BLE001 — tier failures must not break routing
            logger.warning("envio_route_failed", error=str(exc)[:120])
            return []
        filtered = self._filter_rows(table, rows, filters)
        if filtered:
            metrics_incr("envio_routes")
        return filtered

    # ── RPC leg ──────────────────────────────────────────────────────

    def _rpc_cache_key(
        self,
        table: str,
        chain: str,
        from_block: int | None,
        to_block: int | None,
        filters: dict[str, Any] | list[dict[str, Any]] | None,
        limit: int,
    ) -> tuple[Any, ...]:
        """Stable cache key for a range query (filters normalized to tuples)."""
        if isinstance(filters, dict):
            fkey: Any = tuple(sorted(filters.items()))
        elif isinstance(filters, list):
            fkey = tuple(tuple(sorted(item.items())) for item in filters)
        else:
            fkey = ()
        return (chain, table, from_block, to_block, fkey, int(limit))

    @staticmethod
    def _address_from_filters(filters: dict[str, Any] | list[dict[str, Any]] | None) -> str | None:
        """Extract the shared address from an OR address filter list."""
        if isinstance(filters, list):
            cols = {str(item.get("col", "")) for item in filters}
            vals = {str(item.get("value", "")).lower() for item in filters if item.get("value")}
            if vals and len(vals) == 1 and ("from_address" in cols or "to_address" in cols):
                return next(iter(vals))
        return None

    @staticmethod
    def _single_address(filters: dict[str, Any] | list[dict[str, Any]] | None) -> str | None:
        """Extract the single shared address value from filters, if unambiguous."""
        if isinstance(filters, dict):
            if len(filters) == 1:
                return str(next(iter(filters.values()))).lower()
            return None
        if isinstance(filters, list):
            vals = {str(item.get("value", "")).lower() for item in filters if item.get("value")}
            if len(vals) == 1:
                return next(iter(vals))
        return None

    async def _rpc_route(
        self,
        table: str,
        chain: str,
        from_block: int | None,
        to_block: int | None,
        filters: dict[str, Any] | list[dict[str, Any]] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Route a query to the on-demand RPC fetcher, cached."""
        cache_key = self._rpc_cache_key(table, chain, from_block, to_block, filters, limit)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return list(cached)

        rows: list[dict[str, Any]] = []
        try:
            if table == "evm_transfers":
                if isinstance(filters, dict) and filters.get("tx_hash"):
                    row = await self.rpc.fetch_transfer(chain, str(filters["tx_hash"]))
                    rows = [row] if row else []
                else:
                    addr = self._address_from_filters(filters)
                    if addr is not None:
                        rows = await self.rpc.fetch_address_transfers(
                            chain, addr, from_block, to_block, limit=limit
                        )
            elif table == "deployer_events":
                contract = self._single_address(filters)
                if contract is not None:
                    row = await self.rpc.fetch_deployer(chain, contract)
                    rows = [row] if row else []
        except Exception as exc:  # noqa: BLE001 — RPC is best-effort, never a query-killer
            metrics_incr("ondemand_rpc_errors")
            logger.warning("ondemand_rpc_failed", table=table, chain=chain, error=str(exc)[:100])
            return []

        if rows:
            self.cache.set(cache_key, list(rows))
        return rows

    # ── Core routing ─────────────────────────────────────────────────

    async def _route_query(
        self,
        table: str,
        chain: str,
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        filters: dict[str, Any] | list[dict[str, Any]] | None = None,
        limit: int = 100,
        use_rpc: bool = True,
    ) -> list[dict[str, Any]]:
        """Core routing logic.

        1. Query ClickHouse (hot, already indexed).
        2. Query the B2 cold store (historical parquet).
        3. Merge hot + cold (dedupe by row key, ClickHouse wins).
        4. When the indexed stores cannot answer the range, fall through
           to RPC on-demand (cached, rate-limit protected).

        Args:
            table: Logical event table (``evm_transfers``,
                ``deployer_events``, ...).
            chain: Chain name.
            from_block/to_block: Inclusive block/slot bounds.
            filters: ``dict`` of equality filters (AND) or a ``list`` of
                ``{"col", "value"}`` entries (OR).
            limit: Cap on returned rows.
            use_rpc: Allow the RPC leg (False when the caller owns the
                RPC path, e.g. deployer hints).

        Returns:
            Merged rows as dicts, newest first.
        """
        self._check_table(table)
        limit = max(1, min(int(limit), 1000))

        ch_rows = self._query_ch(
            table, chain, from_block=from_block, to_block=to_block, filters=filters, limit=limit
        )
        cold_rows = self._query_cold(
            table, chain, from_block=from_block, to_block=to_block, filters=filters, limit=limit
        )
        merged = self._merge_rows(table, ch_rows, cold_rows, limit=limit)

        biglake_rows: list[dict[str, Any]] = []
        if (
            use_rpc
            and self.on_demand_enabled
            and self._needs_rpc_fallback(table, ch_rows, cold_rows, from_block, to_block, limit)
        ):
            biglake_rows = await self._biglake_route(
                table, chain, from_block=from_block, to_block=to_block, filters=filters, limit=limit
            )
            if biglake_rows:
                merged = self._merge_rows(table, ch_rows, cold_rows, biglake_rows, limit=limit)

        envio_rows: list[dict[str, Any]] = []
        if (
            use_rpc
            and self.on_demand_enabled
            and self._needs_rpc_fallback(
                table, ch_rows, cold_rows + biglake_rows, from_block, to_block, limit
            )
        ):
            envio_rows = await self._envio_route(
                table, chain, from_block=from_block, to_block=to_block, filters=filters, limit=limit
            )
            if envio_rows:
                merged = self._merge_rows(
                    table, ch_rows, cold_rows, biglake_rows, envio_rows, limit=limit
                )

        if (
            use_rpc
            and self.on_demand_enabled
            and self._needs_rpc_fallback(
                table, ch_rows, cold_rows + biglake_rows + envio_rows, from_block, to_block, limit
            )
        ):
            metrics_incr("ondemand_rpc_routes")
            rpc_rows = await self._rpc_route(table, chain, from_block, to_block, filters, limit)
            if rpc_rows:
                merged = self._merge_rows(
                    table, ch_rows, cold_rows, biglake_rows, envio_rows, rpc_rows, limit=limit
                )
        return merged

    # ── Public query API ─────────────────────────────────────────────

    async def get_transfer(self, chain: str, tx_hash: str) -> dict[str, Any] | None:
        """Get a specific transfer. Routes: ClickHouse → Cold → RPC.

        Args:
            chain: Chain name.
            tx_hash: 0x-prefixed transaction hash.

        Returns:
            A transfer row (``evm_transfers`` shape), or None when not
            found on any tier.
        """
        tx = str(tx_hash or "").strip().lower()
        if not tx:
            return None
        rows = await self._route_query("evm_transfers", chain, filters={"tx_hash": tx}, limit=1)
        return rows[0] if rows else None

    async def get_transfers(
        self,
        chain: str,
        address: str,
        from_block: int | None = None,
        to_block: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get transfers for an address. Uses the most efficient source.

        Matches ``from_address = X OR to_address = X``. Bounded ranges hit
        ClickHouse (hot) + the cold tier; when the range is not indexed,
        or an unbounded address query has no indexed rows, the RPC leg
        answers from the newest ``max_block_range`` blocks.

        Args:
            chain: Chain name.
            address: Wallet / contract address.
            from_block: Inclusive start block (None = any/recent).
            to_block: Inclusive end block (None = any/recent).
            limit: Max rows (newest first).

        Returns:
            Transfer rows (``evm_transfers`` shape), newest first.
        """
        addr = str(address or "").strip().lower()
        if not addr:
            return []
        limit = max(1, min(int(limit or self.limit_default), 1000))
        return await self._route_query(
            "evm_transfers",
            chain,
            from_block=from_block,
            to_block=to_block,
            filters=[{"col": "from_address", "value": addr}, {"col": "to_address", "value": addr}],
            limit=limit,
        )

    async def get_block(self, chain: str, block_number: int) -> dict[str, Any]:
        """Get block data. Routes: RPC (blocks are always live).

        Blocks are not stored as first-class rows in the warehouse, so the
        router always dials RPC (cached by block number).

        Args:
            chain: Chain name.
            block_number: Block number.

        Returns:
            The raw RPC block dict (full transactions), or {} when RPC is
            disabled / unavailable.
        """
        block = int(block_number)
        if not self.on_demand_enabled:
            return {}
        cache_key = ("block", chain, block)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        try:
            result = await self.rpc.fetch_block_info(chain, block)
        except Exception as exc:  # noqa: BLE001 — RPC is best-effort
            metrics_incr("ondemand_rpc_errors")
            logger.warning("ondemand_block_failed", chain=chain, block=block, error=str(exc)[:100])
            return {}
        if result:
            self.cache.set(cache_key, result)
        return result or {}

    async def get_deployer_info(self, chain: str, address: str) -> dict[str, Any] | None:
        """Get deployer info for a contract. Routes: CH → Cold → RPC.

        Matches ``deployer_events`` by pair/token/deployer address. When
        the indexed store has a row whose EOA was never resolved, the row's
        tx hash is passed to the RPC leg as the creation-tx hint so
        ``eth_getTransactionByHash`` can resolve the deployer on demand.

        Args:
            chain: Chain name.
            address: Contract / pair / deployer address.

        Returns:
            A deployer row (``deployer_events`` shape), or None when not
            found on any tier.
        """
        addr = str(address or "").strip().lower()
        if not addr:
            return None
        rows = await self._route_query(
            "deployer_events",
            chain,
            filters=[
                {"col": "pair_address", "value": addr},
                {"col": "token_address", "value": addr},
                {"col": "deployer_eoa", "value": addr},
            ],
            limit=5,
            use_rpc=False,
        )
        if rows:
            best = next((r for r in rows if r.get("deployer")), None)
            if best is not None:
                return best
            hint = rows[0].get("tx_hash") or None
        else:
            hint = None

        if not self.on_demand_enabled:
            return rows[0] if rows else None

        cache_key = ("deployer", chain, addr)
        cached = self.cache.get(cache_key)
        if cached is not None:
            return dict(cached)
        try:
            result = await self.rpc.fetch_deployer(chain, addr, creation_tx=hint)
        except Exception as exc:  # noqa: BLE001 — RPC is best-effort
            metrics_incr("ondemand_rpc_errors")
            logger.warning(
                "ondemand_deployer_failed", chain=chain, address=addr, error=str(exc)[:100]
            )
            return rows[0] if rows else None
        if result:
            self.cache.set(cache_key, result)
            return result
        return rows[0] if rows else None


__all__ = ["QueryRouter"]
