"""Hot + cold tier execution for Transparent Historical Access (mixin).

Extracted from ``storage/historical.py``: the unified ``query`` entry
point and its two tier readers — ClickHouse for hot rows, DuckDB/B2
parquet for cold rows. Both readers are best-effort and degrade to [].
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from lite_indexer.config import COLD_QUERY_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.cold_store import TABLE_COLUMNS
from lite_indexer.storage.historical_merge import HistoricalAccessMerge
from lite_indexer.storage.historical_tables import (
    _SORT_COLS,
    _TABLE_REF,
    _block_col,
    asyncio_to_thread_with_timeout,
)

logger = get_logger(__name__)


class HistoricalAccessQuery(HistoricalAccessMerge):
    """Unified hot/cold query mixin."""

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

        Returns:
            Merged rows as dicts, newest first.
        """
        self._check_table(table)
        limit = int(limit) if limit is not None else 1000
        split = self._plan(table, from_block, to_block, from_time, to_time)

        hot_rows: list[dict[str, Any]] = []
        cold_rows: list[dict[str, Any]] = []
        if split.hot:
            hot_rows = await self._query_hot(
                table, chain, from_block, to_block, from_time, to_time, where, limit
            )
        if split.cold:
            cold_rows = await self._query_cold(
                table, chain, from_block, to_block, from_time, to_time, where, limit
            )
        return self._smart_merge(table, hot_rows, cold_rows, limit)

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
