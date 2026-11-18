"""Backfill one BTC day into the verified B2 archive.

The adapter and the writer are already separate concerns:
:class:`~lite_indexer.query.btc_source.BtcSource` turns a ``date=`` partition
into warehouse rows; :func:`lite_indexer.archive.write_unit` publishes them and
refuses to report success unless the object exists at the size written with the
row count the source claimed.

What this module adds is the part that must not be re-implemented per source:
counting a unit *before* publishing it, so ``UnitSpec.expected_rows`` is a fact
rather than a guess, and keeping the stream bounded.

STREAMING
    The count is a ``count(*)`` over the same S3 glob (DuckDB answers it from
    parquet metadata), then rows are yielded batch by batch. ``write_unit``
    spools them through a TemporaryDirectory and deletes it on exit, so peak
    local footprint is one unit — never the day downloaded, never the range.

DAY GRANULARITY
    No ``hour`` is set on :class:`UnitSpec`: a BTC day is ~1M rows / ~0.5 GB,
    which fits one unit, and the lake's own partition *is* the day. Splitting by
    hour would invent a unit the source cannot address.

Usage:
    python -m lite_indexer.query.btc_backfill --dates 2026-11-15
    python -m lite_indexer.query.btc_backfill --dates 2026-11-15 --table btc_blocks
    python -m lite_indexer.query.btc_backfill --dates 2026-11-15 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from lite_indexer.archive import UnitResult, UnitSpec, WriteError, write_unit
from lite_indexer.archive.unit_writer import DEFAULT_REMOTE
from lite_indexer.logging import get_logger
from lite_indexer.query.btc_source import TABLE_MAP, BtcSource

logger = get_logger(__name__)


@dataclass(frozen=True)
class DayPlan:
    """One (table, day) unit and the evidence that it is worth publishing."""

    table: str
    date: str
    expected_rows: int

    def spec(self) -> UnitSpec:
        """Archive identity. No ``hour``: a BTC day is one unit."""
        return UnitSpec(chain="btc", table=self.table, date=self.date, expected_rows=self.expected_rows)


def count_rows(src: BtcSource, table: str, date: str) -> int:
    """Row count for one day partition, without materialising any row.

    Raises:
        ValueError: On an unknown table or a malformed date.
    """
    sql, params = src.count_query(table, date)
    row = src.connection().execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def plan_day(src: BtcSource, table: str, date: str) -> DayPlan:
    """Build the unit plan for one day, counting rows first."""
    return DayPlan(table=table, date=date, expected_rows=count_rows(src, table, date))


def stream_day(src: BtcSource, table: str, date: str) -> Iterator[dict[str, Any]]:
    """Iterator handed to ``write_unit``; consumed exactly once."""
    yield from src.iter_rows(table, date)


def write_day(
    src: BtcSource,
    table: str,
    date: str,
    *,
    remote: str = DEFAULT_REMOTE,
    skip_if_present: bool = True,
) -> UnitResult:
    """Publish one BTC day through the verified writer.

    Args:
        src: Reader.
        table: ``btc_transactions`` or ``btc_blocks``.
        date: ``YYYY-MM-DD``.
        remote: Archive root.
        skip_if_present: Let the writer short-circuit an object already there.

    Returns:
        The writer's :class:`UnitResult` (rows, bytes, key, ``verified``).

    Raises:
        WriteError: Propagated unchanged — a swallowed one is a corrupt archive.
        ValueError: On an unknown table or a malformed date.
    """
    if table not in TABLE_MAP:
        raise ValueError(f"unsupported btc unit table: {table!r}")
    plan = plan_day(src, table, date)
    if plan.expected_rows == 0:
        logger.warning("btc_unit_empty", table=table, date=date)
        raise WriteError("empty_unit", key=f"btc/{table}/{date}", detail="0 rows in partition")
    return write_unit(plan.spec(), stream_day(src, table, date), remote=remote, skip_if_present=skip_if_present)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = argparse.ArgumentParser(prog="btc_backfill", description=__doc__)
    parser.add_argument("--dates", nargs="+", required=True, help="YYYY-MM-DD partitions")
    parser.add_argument(
        "--table",
        default="btc_transactions",
        choices=sorted(TABLE_MAP),
        help="logical table (default: btc_transactions)",
    )
    parser.add_argument("--remote", default=DEFAULT_REMOTE, help="archive root")
    parser.add_argument("--dry-run", action="store_true", help="count rows, publish nothing")
    parser.add_argument("--no-skip", action="store_true", help="re-upload even if present")
    args = parser.parse_args(argv)

    src = BtcSource()
    if not src.available():
        print("btc lake unavailable (duckdb missing or BTC_LAKE_ENABLED=0)", file=sys.stderr)  # noqa: T201
        return 1
    rc = 0
    for date in src.validate_dates(args.dates) or []:
        try:
            if args.dry_run:
                plan = plan_day(src, args.table, date)
                print(f"{args.table} {date}: {plan.expected_rows} rows (dry run)")  # noqa: T201
                continue
            result = write_day(src, args.table, date, remote=args.remote, skip_if_present=not args.no_skip)
        except (WriteError, ValueError) as exc:
            print(f"{args.table} {date}: FAILED {exc}", file=sys.stderr)  # noqa: T201
            rc = 1
            continue
        state = "skipped (present)" if result.skipped else f"verified {result.bytes_written} B"
        print(f"{args.table} {date}: {result.rows} rows -> {result.key} [{state}]")  # noqa: T201
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
