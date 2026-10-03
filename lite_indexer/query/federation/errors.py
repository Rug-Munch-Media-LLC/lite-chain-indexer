"""Typed exceptions for the CryptoHouse federation layer.

Kept separate from the transport so callers can import and ``except`` these
without pulling in urllib/parsing machinery, and so the failure taxonomy is
readable in one place.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

#: Mirrors the server's own cap so we can reason about it client-side.
MAX_ROWS_TO_READ: Final[int] = 10_000_000_000
#: Mirrors ``max_execution_time`` on the endpoint (seconds).
MAX_EXECUTION_TIME_S: Final[int] = 60


class CryptoHouseError(Exception):
    """Base for every error raised by the federation layer."""


class UnboundedQueryError(CryptoHouseError):
    """A large table was referenced without a time/block bound.

    Raised locally, before any bytes leave the box. The server rejects such a
    query anyway with ``TOO_MANY_ROWS``; catching it here is cheaper and names
    the table and the missing predicate.
    """

    def __init__(self, tables: Sequence[str], sql: str) -> None:
        self.tables = tuple(tables)
        self.sql = sql
        joined = ", ".join(self.tables)
        super().__init__(
            f"refusing unbounded query over large table(s) [{joined}]: "
            f"needs a WHERE bound on a time/block column. Build one with "
            f"partition.time_range()/block_range() or wrap the call in "
            f"partition.bounded(). SQL was: {sql[:160]}"
        )


class TooManyRowsError(CryptoHouseError):
    """Server rejected the query for exceeding ``max_rows_to_read`` (10e9)."""

    def __init__(self, message: str, sql: str) -> None:
        self.sql = sql
        super().__init__(f"TOO_MANY_ROWS (max_rows_to_read={MAX_ROWS_TO_READ}): {message[:300]}")


class QueryTimeoutError(CryptoHouseError):
    """Execution exceeded the 60s server cap (or the local socket timeout)."""

    def __init__(self, message: str, sql: str, elapsed_s: float) -> None:
        self.sql = sql
        self.elapsed_s = elapsed_s
        super().__init__(f"TIMEOUT after {elapsed_s:.1f}s: {message[:300]}")


class TableNotFoundError(CryptoHouseError):
    """Referenced table does not exist on this endpoint."""

    def __init__(self, message: str, sql: str) -> None:
        self.sql = sql
        super().__init__(message[:300])


class UnknownColumnError(CryptoHouseError):
    """Referenced column does not exist on the introspected table."""

    def __init__(self, table: str, missing: Sequence[str]) -> None:
        self.table = table
        self.missing = tuple(missing)
        super().__init__(f"{table} has no column(s) {list(self.missing)} — introspect via schemas.describe()")


def classify_error(detail: str, sql: str, elapsed_s: float) -> CryptoHouseError:
    """Map a ClickHouse HTTP error body onto the right typed exception."""
    low = detail.lower()
    if "too_many_rows" in low or "max rows" in low or "max_rows_to_read" in low:
        return TooManyRowsError(detail, sql)
    if "timeout" in low or "max_execution_time" in low or "timed out" in low:
        return QueryTimeoutError(detail, sql, elapsed_s)
    if "unknown_table" in low or "does not exist" in low or "doesn't exist" in low:
        return TableNotFoundError(detail, sql)
    if "unknown_identifier" in low or "there's no column" in low:
        return CryptoHouseError(f"unknown column: {detail[:300]}")
    return CryptoHouseError(detail[:300] or "unknown ClickHouse error")
