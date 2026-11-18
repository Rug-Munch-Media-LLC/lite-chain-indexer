"""Build the BTC lake read SQL and validate its date partitions.

Separated from :mod:`lite_indexer.query.btc_source` for the same reason
``biglake_read`` is separate from ``biglake_source``: the source stays a thin
reader, and the projection/validation rules are stated once, testable without a
connection.

The projection is not cosmetic. The lake stores ``inputs``/``outputs`` as lists
of structs; reading them whole would pull script_hex and witness data — the
bulk of the 0.5 GB/day — into every row. ``list_transform`` keeps only the
address, value, type and parent-hash fields the warehouse row needs.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: Logical table -> lake directory name.
TABLE_MAP: dict[str, str] = {
    "btc_transactions": "transactions",
    "btc_blocks": "blocks",
}

_TRANSACTION_COLUMNS = (
    "block_number, block_hash, block_timestamp, hash AS tx_hash,"
    ' "index" AS tx_index, is_coinbase, input_count, output_count, fee,'
    " list_transform(inputs, x -> x.address) AS input_addresses,"
    " list_transform(inputs, x -> x.value) AS input_values,"
    " list_transform(inputs, x -> x.spent_transaction_hash) AS input_spent_tx,"
    " list_transform(inputs, x -> x.spent_output_index) AS input_spent_index,"
    " list_transform(outputs, x -> x.address) AS output_addresses,"
    " list_transform(outputs, x -> x.value) AS output_values,"
    " list_transform(outputs, x -> x.type) AS output_types"
)

_BLOCK_COLUMNS = (
    "number AS block_number, hash AS block_hash, timestamp AS block_timestamp,"
    " previousblockhash, transaction_count, size, weight, difficulty, bits,"
    " chainwork, merkle_root, version, nonce"
)

COLUMNS: dict[str, str] = {
    "btc_transactions": _TRANSACTION_COLUMNS,
    "btc_blocks": _BLOCK_COLUMNS,
}


def validate_dates(dates: Sequence[str] | None) -> list[str] | None:
    """Validate ``YYYY-MM-DD`` partitions; ``None`` when none were given.

    Raises:
        ValueError: On any malformed date. A silently-skipped partition would
            return an empty result that looks like a quiet day.
    """
    if not dates:
        return None
    for d in dates:
        if not _DATE_RE.match(str(d)):
            raise ValueError(f"invalid date for btc query: {d!r} (expected YYYY-MM-DD)")
    return [str(d) for d in dates]


def paths(base: str, table: str, dates: Sequence[str] | None, scope: str) -> list[str]:
    """Parquet globs for one logical table over the requested days."""
    lake_table = TABLE_MAP[table]
    return [f"{base}/{lake_table}/date={d}/*.parquet" for d in (dates or [scope])]


def source_expr(path_globs: Sequence[str]) -> str:
    """``read_parquet(...)`` over validated globs.

    Globs are built only from validated ``YYYY-MM-DD`` text, so no caller input
    reaches the SQL string.
    """
    path_literals = "[" + ", ".join(f"'{p}'" for p in path_globs) + "]"
    return f"read_parquet({path_literals}, hive_partitioning=true, union_by_name=true)"


def build_query(
    path_globs: Sequence[str],
    *,
    columns: str,
    from_block: int | None = None,
    to_block: int | None = None,
    limit: int | None = None,
) -> tuple[str, list[object]]:
    """Build the DuckDB read for one BTC table.

    Asks for ``limit + 1`` rows when a limit is given: the probe row is what
    makes truncation detectable instead of a short result that looks complete.

    Args:
        path_globs: Parquet globs (already validated).
        columns: Projection from :data:`COLUMNS`.
        from_block/to_block: Inclusive block-number bounds.
        limit: Row cap, or ``None`` for a full-partition streaming read.

    Returns:
        ``(sql, params)``.
    """
    where: list[str] = []
    params: list[object] = []
    if from_block is not None:
        where.append("block_number >= ?")
        params.append(int(from_block))
    if to_block is not None:
        where.append("block_number <= ?")
        params.append(int(to_block))
    clause = f" WHERE {' AND '.join(where)}" if where else ""
    sql = f"SELECT {columns} FROM {source_expr(path_globs)}{clause} ORDER BY block_number"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit) + 1)
    return sql, params


def count_query(path_globs: Sequence[str]) -> str:
    """``count(*)`` over one partition, answered from parquet metadata."""
    return f"SELECT count(*) FROM {source_expr(path_globs)}"


def open_connection() -> object:
    """A DuckDB connection configured for anonymous reads of the public lake.

    An empty ``KEY_ID``/``SECRET`` pair is what makes httpfs skip request
    signing — the reason this bucket needs no credentials.
    """
    import duckdb  # pylint: disable=import-outside-toplevel

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET enable_progress_bar = false;")
    con.execute("CREATE OR REPLACE SECRET s3_anon (TYPE S3, PROVIDER config, REGION 'us-east-2', KEY_ID '', SECRET '')")
    return con


__all__ = [
    "COLUMNS",
    "TABLE_MAP",
    "build_query",
    "count_query",
    "open_connection",
    "paths",
    "source_expr",
    "validate_dates",
]
