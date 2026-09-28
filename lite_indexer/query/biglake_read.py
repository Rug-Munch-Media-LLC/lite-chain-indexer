"""Build the lake read SQL and decide how the result was truncated.

A lake read that silently returns fewer rows than asked for is the worst
failure a data reader has: the caller cannot tell a short answer from a
complete one. So the SQL asks for one row past the ceiling — its existence
is the proof that older rows remain — and the caller discards it.

Separated from :mod:`lite_indexer.query.biglake_source` so the source stays
a thin reader and so the truncation rule is stated once.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr_labeled as metrics_labeled
from lite_indexer.query.lake_limits import reject_oversize_limit

logger = get_logger(__name__)


def build_lake_query(
    paths: list[str],
    *,
    from_block: int | None,
    to_block: int | None,
    address: str | None,
    limit: int,
) -> tuple[str, list[Any]]:
    """Build the DuckDB read for one BigLake table.

    Returns ``(sql, params)`` where the final param is ``limit + 1``: the
    probe row that makes truncation detectable.
    """
    page = reject_oversize_limit(limit)
    where: list[str] = []
    params: list[Any] = []
    if from_block is not None:
        where.append("block_number >= ?")
        params.append(int(from_block))
    if to_block is not None:
        where.append("block_number <= ?")
        params.append(int(to_block))
    if address:
        addr = str(address).strip().lower()
        addr40 = addr[2:] if addr.startswith("0x") else addr
        # AWS v1.0 stores from/to as 32-byte left-padded hex (log topics);
        # compare the trailing 20-byte address only.
        where.append("(lower(right(from_address, 40)) = ? OR lower(right(to_address, 40)) = ?)")
        params.extend([addr40, addr40])

    path_literals = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
    sql = (
        f"SELECT * FROM read_parquet({path_literals}, hive_partitioning=true, union_by_name=true)"
        + (f" WHERE {' AND '.join(where)}" if where else "")
        + " ORDER BY block_number DESC LIMIT ?"
    )
    params.append(page + 1)
    return sql, params


def probe_truncated(rows: list[dict[str, Any]], page: int, table: str) -> tuple[list[dict[str, Any]], bool]:
    """Split a probed read into ``(rows, truncated)``.

    Drops the probe row, so the returned set never exceeds the caller's
    limit, and emits ``biglake_truncated`` when a page was cut short.
    """
    truncated = len(rows) > page
    if truncated:
        metrics_labeled("biglake_truncated", table=table, limit=str(page))
        logger.warning("biglake_read_truncated", table=table, rows=len(rows), limit=page)
    return rows[:page], truncated


def lake_filters(
    *,
    address: str | None,
    from_block: int | None,
    to_block: int | None,
    dates: Sequence[str] | None,
    limit: int,
) -> dict[str, Any]:
    """Structured filters forwarded unchanged to a foreign (BigQuery) backend."""
    return {
        "address": address,
        "from_block": from_block,
        "to_block": to_block,
        "dates": dates,
        "limit": limit,
    }
