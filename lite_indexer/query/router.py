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

This module is the FACADE + core dispatch (``_route_query``) + the public
query API. The leg implementations live in sibling modules
(:mod:`legs_clickhouse`, :mod:`legs_address`, :mod:`legs_cold`,
:mod:`legs_lake`, :mod:`legs_portals`, :mod:`legs_rpc`) and are bound as
``QueryRouter`` methods so the public surface is unchanged. Shared table
maps / pure helpers live in :mod:`legs_common`.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    LAKE_READ_ENABLED,
    ON_DEMAND_RPC_ENABLED,
    ON_DEMAND_RPC_MAX_BLOCK_RANGE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query import (
    legs_address,
    legs_clickhouse,
    legs_cold,
    legs_lake,
    legs_portals,
    legs_rpc,
)
from lite_indexer.query.address_backfill import AddressBackfill
from lite_indexer.query.biglake_source import BigLakeSource
from lite_indexer.query.cache import QueryCache
from lite_indexer.query.deployer_lookup import get_deployer_info
from lite_indexer.query.envio_source import EnvioSource
from lite_indexer.query.indexed_source import IndexedXyzSource
from lite_indexer.query.lake_limits import reject_oversize_limit
from lite_indexer.query.lake_preference import should_try_lake
from lite_indexer.query.leg_gates import leg_enabled
from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.legs_common import ROW_KEYS, SORT_COLS, sort_value
from lite_indexer.query.rpc_fetcher import RPCFetcher
from lite_indexer.query.sqd_source import SQDSource
from lite_indexer.storage.cold_store import ColdStore

logger = get_logger(__name__)


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
        indexed: Optional :class:`IndexedXyzSource` (defaults to enabled —
            Indexed.xyz R2 lake: an independent third path for EVM chains).
        sqd: Optional :class:`SQDSource` (defaults to enabled — SQD portal
            full-history EVM traces for ``internal_transfers``).
        client_factory: Optional zero-arg callable returning a ClickHouse
            client lazily (used by the API so constructing the router does
            not force a connection at startup).
        on_demand_enabled: Gate for the PAID RPC leg only
            (``ON_DEMAND_RPC_ENABLED``). Stops RPC spend, nothing else.
        lake_read_enabled: Gate for the FREE external-lake legs (BigLake,
            Envio, indexed.xyz, SQD) — ``LAKE_READ_ENABLED``, independent of
            ``on_demand_enabled`` so a cost kill-switch never removes free
            historical reads.
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
        indexed: IndexedXyzSource | None = None,
        sqd: SQDSource | None = None,
        client_factory: Any | None = None,
        on_demand_enabled: bool = ON_DEMAND_RPC_ENABLED,
        lake_read_enabled: bool = LAKE_READ_ENABLED,
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
        self.indexed = indexed if indexed is not None else IndexedXyzSource()
        self.sqd = sqd if sqd is not None else SQDSource()
        self.on_demand_enabled = bool(on_demand_enabled)
        self.lake_read_enabled = bool(lake_read_enabled)
        # True when the last _route_query hit a lake page ceiling. query()
        # returns list[dict] and is public API, so truncation is reported
        # here and via the biglake_truncated metric, not by widening it.
        self.last_read_truncated = False
        self.max_block_range = int(max_block_range)
        self.limit_default = int(limit_default)

    # ── Legs (implemented in sibling query/legs_* modules) ───────────

    _ch_client = legs_clickhouse.ch_client
    _check_table = staticmethod(legs_clickhouse.check_table)
    _query_ch = legs_clickhouse.query_ch
    _append_ch_filters = staticmethod(legs_clickhouse.append_ch_filters)
    _get_backfill = legs_address.get_backfill
    _query_address_activity = legs_address.query_address_activity
    backfill_address = legs_address.backfill_address
    get_address_activity = legs_address.get_address_activity
    _query_cold = legs_cold.query_cold
    _build_cold_where = staticmethod(legs_cold.build_cold_where)
    _filter_rows = staticmethod(legs_lake.filter_rows)
    _lake_dates = legs_lake.lake_dates
    _biglake_route = legs_lake.biglake_route
    _envio_route = legs_portals.envio_route
    _indexed_route = legs_portals.indexed_route
    _sqd_traces_route = legs_portals.sqd_traces_route
    _rpc_route = legs_rpc.rpc_route

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
        sort_cols = SORT_COLS[table]
        rows.sort(key=lambda r: tuple(sort_value(r, c) for c in sort_cols), reverse=True)
        return rows[:limit]

    @staticmethod
    def _row_key(table: str, row: dict[str, Any]) -> tuple[Any, ...]:
        """Return the dedupe identity for a row."""
        key: list[Any] = []
        for col in ROW_KEYS[table]:
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

    def _should_try_lake(
        self,
        ch_rows: list[dict[str, Any]],
        cold_rows: list[dict[str, Any]],
        from_block: int | None,
        to_block: int | None,
    ) -> bool:
        """Whether the free lake should be read for this range."""
        return should_try_lake(ch_rows, cold_rows, from_block, to_block)

    # ── Filter helpers ───────────────────────────────────────────────

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
        # Reject, never silently clamp: a caller asking for 1,000,000 rows
        # used to get the 1000 newest with no signal the result was short.
        limit = reject_oversize_limit(limit)
        req = LegRequest(
            table=table,
            chain=chain,
            from_block=from_block,
            to_block=to_block,
            filters=filters,
            limit=limit,
        )

        ch_rows = self._query_ch(req)
        cold_rows = self._query_cold(req)
        merged = self._merge_rows(table, ch_rows, cold_rows, limit=limit)

        # Consult the free lake whenever the cold tier is empty and the
        # range is bounded — NOT only when the RPC-fallback predicate is
        # already true. That predicate returns False for a bounded WIDE
        # range, which is exactly the range the lake holds and RPC would
        # refuse or charge for.
        biglake_rows: list[dict[str, Any]] = []
        if leg_enabled(self, "biglake", use_rpc) and self._should_try_lake(ch_rows, cold_rows, from_block, to_block):
            biglake_rows, self.last_read_truncated = await self._biglake_route(req)
            if biglake_rows:
                merged = self._merge_rows(table, ch_rows, cold_rows, biglake_rows, limit=limit)

        envio_rows: list[dict[str, Any]] = []
        if leg_enabled(self, "envio", use_rpc) and self._needs_rpc_fallback(
            table, ch_rows, cold_rows + biglake_rows, from_block, to_block, limit
        ):
            envio_rows = await self._envio_route(req)
            if envio_rows:
                merged = self._merge_rows(table, ch_rows, cold_rows, biglake_rows, envio_rows, limit=limit)

        indexed_rows: list[dict[str, Any]] = []
        if leg_enabled(self, "indexed", use_rpc) and self._needs_rpc_fallback(
            table, ch_rows, cold_rows + biglake_rows + envio_rows, from_block, to_block, limit
        ):
            indexed_rows = await self._indexed_route(req)
            if indexed_rows:
                merged = self._merge_rows(
                    table, ch_rows, cold_rows, biglake_rows, envio_rows, indexed_rows, limit=limit
                )

        sqd_rows: list[dict[str, Any]] = []
        if leg_enabled(self, "sqd", use_rpc) and self._needs_rpc_fallback(
            table,
            ch_rows,
            cold_rows + biglake_rows + envio_rows + indexed_rows,
            from_block,
            to_block,
            limit,
        ):
            sqd_rows = await self._sqd_traces_route(req)
            if sqd_rows:
                merged = self._merge_rows(
                    table,
                    ch_rows,
                    cold_rows,
                    biglake_rows,
                    envio_rows,
                    indexed_rows,
                    sqd_rows,
                    limit=limit,
                )

        if leg_enabled(self, "rpc", use_rpc) and self._needs_rpc_fallback(
            table,
            ch_rows,
            cold_rows + biglake_rows + envio_rows + indexed_rows + sqd_rows,
            from_block,
            to_block,
            limit,
        ):
            metrics_incr("ondemand_rpc_routes")
            rpc_rows = await self._rpc_route(req)
            if rpc_rows:
                merged = self._merge_rows(
                    table,
                    ch_rows,
                    cold_rows,
                    biglake_rows,
                    envio_rows,
                    indexed_rows,
                    sqd_rows,
                    rpc_rows,
                    limit=limit,
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

        Delegates to :func:`query.deployer_lookup.get_deployer_info` (extracted
        for the size ratchet). See that function for the full contract.
        """
        return await get_deployer_info(self, chain, address)


__all__ = ["QueryRouter"]
