"""BtcSource — on-demand Bitcoin history from the AWS Open Data lake.

``biglake_source`` maps Ethereum and states that ``btc/`` exists in the lake but
is not mapped. This module is that mapping: the same reader contract
(``available()`` + ``query() -> (rows, truncated)``) over
``aws-public-blockchain/v1.0/btc``, read in place with DuckDB httpfs.

Two tables, day-partitioned (``date=YYYY-MM-DD/``), snappy parquet:
``transactions`` -> ``btc_transactions`` (~1M rows/day, ~0.5 GB) and
``blocks`` -> ``btc_blocks`` (~144 rows/day).

Bitcoin is UTXO. The warehouse row keeps the UTXO shape (input/output lists,
values in integer sats) rather than fabricating a ``from_address`` /
``to_address`` pair the source does not contain; the lossy parts are enumerated
in :mod:`lite_indexer.query.btc_rowmap`. SQL and date validation live in
:mod:`lite_indexer.query.btc_read`.

Reads are bounded by ``date=`` partitions: pass explicit ``dates`` for exact
days, or rely on ``BTC_SCOPE``. Never glob ``date=*``.

Env:
    BTC_LAKE_ENABLED   set to 0/false to disable entirely (default enabled)
    BTC_SCOPE          month glob for reads without explicit dates
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query import btc_read
from lite_indexer.query.btc_rowmap import map_row
from lite_indexer.query.lake_limits import reject_oversize_limit

logger = get_logger(__name__)

#: Logical table -> lake directory name (re-exported for callers/tests).
TABLE_MAP = btc_read.TABLE_MAP

#: Errors meaning "this partition produced no rows" rather than "bug".
_READ_ERRORS = (RuntimeError, OSError, ValueError)


class BtcSource:
    """DuckDB-backed reader over the AWS public Bitcoin dataset.

    Args:
        enabled: Override ``BTC_LAKE_ENABLED`` (default enabled).
        scope: Month glob for reads without explicit ``dates``; defaults to
            ``BTC_SCOPE`` or the current month.
        connect: Optional zero-arg callable returning a DuckDB-like connection
            (tests use it to avoid the network).
    """

    S3_BASE = "s3://aws-public-blockchain/v1.0/btc"

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        scope: str | None = None,
        connect: Callable[[], Any] | None = None,
    ) -> None:
        raw = os.getenv("BTC_LAKE_ENABLED")
        self._enabled = (
            enabled if enabled is not None else raw is None or raw.strip().lower() not in {"0", "false", "no", "off"}
        )
        self._scope = scope or os.getenv("BTC_SCOPE") or f"{datetime.now(UTC):%Y-%m}-*"
        self._connect_impl = connect
        self._con: Any | None = None
        self._duckdb_missing = False

    def available(self) -> bool:
        """True when the tier can serve queries (enabled + DuckDB importable)."""
        if not self._enabled or self._duckdb_missing:
            return False
        if self._connect_impl is not None:
            return True
        try:
            import duckdb  # noqa: F401  # pylint: disable=import-outside-toplevel,unused-import
        except ImportError:
            self._duckdb_missing = True
            logger.warning("btc_source_disabled", reason="duckdb_missing")
            return False
        return True

    def connection(self) -> Any:
        """Public accessor for the DuckDB connection (backfill path)."""
        if self._con is None:
            self._con = self._connect_impl() if self._connect_impl is not None else btc_read.open_connection()
        return self._con

    @staticmethod
    def validate_dates(dates: Sequence[str] | None) -> list[str] | None:
        """Validate ``YYYY-MM-DD`` partitions; ``None`` when none were given.

        Raises:
            ValueError: On any malformed date.
        """
        return btc_read.validate_dates(dates)

    def paths(self, table: str, dates: Sequence[str] | None) -> list[str]:
        """Parquet globs for one logical table over the requested days."""
        return btc_read.paths(self.S3_BASE, table, dates, self._scope)

    def _guarded(self, table: str, date: str) -> list[str]:
        """Validated globs for a single day. Raises ``ValueError`` on bad input."""
        if table not in TABLE_MAP:
            raise ValueError(f"unmapped btc table: {table!r}")
        return self.paths(table, self.validate_dates([date]) or [])

    def count_query(self, table: str, date: str) -> tuple[str, list[object]]:
        """``count(*)`` for one day — cheap, answered from parquet metadata."""
        return btc_read.count_query(self._guarded(table, date)), []

    def query(
        self,
        table: str,
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        dates: Sequence[str] | None = None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Read rows in the warehouse row shape.

        Returns:
            ``(rows, truncated)``. ``truncated`` is True when the read hit the
            page ceiling and older rows definitely exist. Empty rows when the
            tier is disabled, the table is unknown, or the partition is absent
            — an absent partition is a fact, not a failure.

        Raises:
            ValueError: On an oversize ``limit`` or a malformed date.
        """
        if table not in TABLE_MAP:
            logger.warning("btc_source_unknown_table", table=table)
            return [], False
        if not self.available():
            return [], False
        page = reject_oversize_limit(limit)
        sql, params = btc_read.build_query(
            self.paths(table, self.validate_dates(dates)),
            columns=btc_read.COLUMNS[table],
            from_block=from_block,
            to_block=to_block,
            limit=page,
        )
        try:
            cursor = self.connection().execute(sql, params)
            cols = [d[0] for d in cursor.description]
            raw_rows = cursor.fetchall()
        except _READ_ERRORS as exc:
            # A missing partition surfaces from httpfs as an IO error; treating
            # it as fatal would make "no data that day" indistinguishable from
            # "the reader is broken".
            logger.warning("btc_source_read_failed", table=table, error=str(exc)[:200])
            return [], False
        rows = [map_row(table, dict(zip(cols, raw, strict=False))) for raw in raw_rows]
        if len(rows) > page:
            logger.warning("btc_read_truncated", table=table, rows=len(rows), limit=page)
            return rows[:page], True
        return rows, False

    def iter_rows(self, table: str, date: str, *, batch_size: int = 50_000) -> Iterator[dict[str, Any]]:
        """Stream one day's rows, one batch at a time (the backfill path).

        Nothing is materialised beyond one batch, so a 0.5 GB day never lands on
        local disk. ``date=<d>`` is exactly one archival unit.

        Raises:
            ValueError: On a malformed ``date`` or an unknown ``table``.
        """
        sql, params = btc_read.build_query(self._guarded(table, date), columns=btc_read.COLUMNS[table], limit=None)
        cursor = self.connection().execute(sql, params)
        cols = [d[0] for d in cursor.description]
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                return
            for raw in batch:
                yield map_row(table, dict(zip(cols, raw, strict=False)))


__all__ = ["TABLE_MAP", "BtcSource"]
