"""Parquet read/verify and ClickHouse day-row fetch functions."""

from __future__ import annotations

import gzip
from datetime import date
from pathlib import Path
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.storage.parquet.schema import PYARROW_AVAILABLE, pq

logger = get_logger(__name__)


def _max_rows_per_day() -> int:
    """Resolve the per-day row cap at call time.

    Reads ``_MAX_ROWS_PER_DAY`` from the package namespace so tests can
    monkeypatch ``lite_indexer.storage.parquet._MAX_ROWS_PER_DAY``.
    """
    from lite_indexer.storage.parquet import _MAX_ROWS_PER_DAY  # noqa: PLC0415 — lazy to avoid circular import

    return _MAX_ROWS_PER_DAY


def _fetch_day_rows(
    client: Any,
    table: str,
    chain: str,
    day: date,
) -> list[dict[str, Any]]:
    """Fetch all rows for a chain/date from a ClickHouse table.

    Raises ``ValueError`` when the day exceeds ``_MAX_ROWS_PER_DAY``
    so a truncated export is never treated as complete (and the rows
    are therefore never deleted).

    Args:
        client: ClickHouse client.
        table: Fully-qualified table name.
        chain: Chain name.
        day: Export date.

    Returns:
        Rows as list of dicts (column name → value).

    Raises:
        ValueError: If the day has more than ``_MAX_ROWS_PER_DAY`` rows.
    """
    cap = _max_rows_per_day()
    query = f"SELECT * FROM {table} WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s LIMIT {cap + 1}"
    try:
        rows, columns = client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
            query,
            {"chain": chain, "day": day.isoformat()},
            with_column_types=True,
        )
    except Exception as exc:
        # Table may not exist yet for this chain — not an error worth
        # failing the run over.
        if "doesn't exist" in str(exc) or "UNKNOWN_TABLE" in str(exc):
            logger.debug("parquet_table_missing", table=table, chain=chain, error=str(exc)[:60])
            return []
        logger.warning("parquet_select_failed", table=table, chain=chain, error=str(exc)[:120])
        return []

    if len(rows) > cap:
        raise ValueError(f"day exceeds export cap of {cap} rows")

    col_names = [c[0] for c in columns]
    return [dict(zip(col_names, row, strict=False)) for row in rows]


def _verify_parquet(
    path: Path,
    expected_rows: int,
    expected_columns: set[str],
) -> bool:
    """Verify a written parquet file: row count + schema.

    Reads the file back and checks ``num_rows == expected_rows`` and
    that every expected column exists. Without pyarrow the JSONL
    fallback is verified by counting rows in the gzip stream (schema
    is not enforced).

    Args:
        path: Parquet (or fallback JSONL) file path.
        expected_rows: Number of rows that must be in the file.
        expected_columns: Column names that must be present.

    Returns:
        True when the file verifies.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False

    if not PYARROW_AVAILABLE:
        try:
            with gzip.open(str(path) + ".jsonl.gz", "rt", encoding="utf-8") as fh:
                count = sum(1 for _ in fh)
            return count == expected_rows
        except OSError:
            return False

    try:
        table = pq.read_table(path)  # type: ignore[no-untyped-call]
        if table.num_rows != expected_rows:
            logger.warning("parquet_verify_row_mismatch", path=str(path), expected=expected_rows, actual=table.num_rows)
            return False
        missing = expected_columns - set(table.column_names)
        if missing:
            logger.warning("parquet_verify_schema_missing", path=str(path), missing=sorted(missing))
            return False
        return True
    except Exception as exc:
        logger.warning("parquet_verify_failed", path=str(path), error=str(exc)[:120])
        return False


__all__ = ["_fetch_day_rows", "_verify_parquet"]
