"""ClickHouse (hot tier) leg for :class:`QueryRouter`.

Client resolution, table validation, the direct hot SELECT and its filter
rendering. Bound as ``QueryRouter`` methods by the facade so the public
surface is unchanged.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.legs_common import CH_TABLE_REF, block_col
from lite_indexer.storage.cold_store import TABLE_COLUMNS, TABLE_KINDS, TABLE_ORDER

logger = get_logger(__name__)


def ch_client(router: Any) -> Any | None:
    """Resolve the ClickHouse client (lazily via ``client_factory``)."""
    if router.ch is None and router._client_factory is not None:
        try:
            router.ch = router._client_factory()
        except Exception as exc:  # noqa: BLE001 — lazy connect is best-effort
            logger.warning("ondemand_ch_resolve_failed", error=str(exc)[:100])
    return router.ch


def check_table(table: str) -> str:
    """Validate a logical table name against the routable set."""
    if table not in CH_TABLE_REF or table not in TABLE_KINDS:
        raise ValueError(f"unknown table for on-demand routing: {table}")
    return table


def query_ch(router: Any, req: LegRequest) -> list[dict[str, Any]]:
    """Query ClickHouse directly. Returns [] when unavailable/failing."""
    table, chain = req.table, req.chain
    from_block, to_block = req.from_block, req.to_block
    filters, limit = req.filters, req.limit
    client = router._ch_client()
    if client is None:
        return []
    clauses = ["chain = %(chain)s"]
    params: dict[str, Any] = {"chain": chain}
    column = block_col(table)
    if from_block is not None:
        clauses.append(f"{column} >= %(from_block)s")
        params["from_block"] = from_block
    if to_block is not None:
        clauses.append(f"{column} <= %(to_block)s")
        params["to_block"] = to_block
    router._append_ch_filters(clauses, params, filters)

    cols = ", ".join(TABLE_COLUMNS[table])
    order = TABLE_ORDER.get(table, "block_timestamp DESC, block_number DESC, log_index DESC")
    sql = f"SELECT {cols} FROM {CH_TABLE_REF[table]} WHERE {' AND '.join(clauses)} ORDER BY {order} LIMIT {int(limit)}"
    try:
        rows, column_types = client.execute(sql, params, with_column_types=True)
    except Exception as exc:  # noqa: BLE001 — hot reads are best-effort
        metrics_incr("ondemand_ch_errors")
        logger.debug("ondemand_ch_query_failed", table=table, error=str(exc)[:100])
        return []
    names = [name for name, _ in column_types]
    return [dict(zip(names, row, strict=False)) for row in rows]


def append_ch_filters(
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
