"""CLI + publish step for :mod:`lite_indexer.query.sqd_backfill`.

Split out so the backfill module stays a reader (one primary function) and this
one stays a driver. ``publish_day`` lives here rather than in the reader because
materialising a day in memory is a PUBLISH concern: a caller that only wants a
count (:func:`count_rows` through ``--dry-run``) never pays for it.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Any

from lite_indexer.archive import UnitSpec, WriteError, write_unit
from lite_indexer.query.sqd_backfill import (
    DEFAULT_SLEEP_S,
    TABLES,
    MissingTableError,
    UnsupportedChainError,
    UnsupportedTableError,
    backfill_day,
)
from lite_indexer.query.sqd_day import iter_days
from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

__all__ = ["count_rows", "publish_day", "cli"]

#: Archive root. Overridable so an operator can target a scratch bucket.
DEFAULT_REMOTE = "b2-crypt-v2:chain-data/parquet"


def count_rows(
    chain: str,
    table: str,
    day: dt.date,
    *,
    src: SQDSource | None = None,
    sleep_s: float = DEFAULT_SLEEP_S,
) -> int:
    """Rows the portal holds for one day — the ``--dry-run`` answer."""
    return sum(1 for _ in backfill_day(chain, table, day, src=src, sleep_s=sleep_s))


def publish_day(
    chain: str,
    table: str,
    day: dt.date,
    *,
    remote: str = DEFAULT_REMOTE,
    src: SQDSource | None = None,
    sleep_s: float = DEFAULT_SLEEP_S,
) -> str:
    """Read one day once, then publish it as a verified unit.

    The portal is not cheaply replayable, so the day is materialised in memory
    once and handed to ``write_unit`` with ``expected_rows`` equal to what was
    read. The count is exact because it is the length of the list being
    published; the writer then re-derives it from the parquet and raises on any
    disagreement, so a lost or duplicated row cannot reach the archive.

    Args:
        chain: Chain name.
        table: One of :data:`lite_indexer.query.sqd_backfill.TABLES`.
        day: UTC day to publish.
        remote: B2 remote root.
        src: Injected portal client (tests).
        sleep_s: Seconds between portal requests.

    Returns:
        A one-line report. Raises ``WriteError`` when the unit cannot be proven.
    """
    rows = list(backfill_day(chain, table, day, src=src, sleep_s=sleep_s))
    if not rows:
        return f"empty {chain}/{table}/{day.isoformat()}"
    spec = UnitSpec(chain=chain, table=table, date=day.isoformat(), expected_rows=len(rows))
    result = write_unit(spec, rows, remote=remote)
    return (
        f"ok {chain}/{table}/{day.isoformat()} rows={result.rows} bytes={result.bytes_written} "
        f"key={result.key} skipped={result.skipped}"
    )


def _run_one(args: argparse.Namespace, day: dt.date, src: SQDSource) -> str:
    """Execute one day in the requested mode, returning the report line."""
    if args.dry_run:
        total = count_rows(args.chain, args.table, day, src=src, sleep_s=args.sleep)
        return f"dry-run {args.chain}/{args.table}/{day.isoformat()} rows={total}"
    return publish_day(args.chain, args.table, day, remote=args.remote, src=src, sleep_s=args.sleep)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="SQD portal → B2 day-unit backfill (one chain, one table, sequential by design)"
    )
    parser.add_argument("--chain", required=True, help="chain name, e.g. polygon")
    parser.add_argument("--table", default="internal_transfers", choices=list(TABLES))
    parser.add_argument("--days", type=int, default=1, help="UTC days ending today, oldest first")
    parser.add_argument("--day", help="a single YYYY-MM-DD instead of --days")
    parser.add_argument("--remote", default=DEFAULT_REMOTE)
    parser.add_argument("--dry-run", action="store_true", help="count rows, publish nothing")
    parser.add_argument("--sleep", type=float, default=float(os.getenv("SQD_BACKFILL_SLEEP", str(DEFAULT_SLEEP_S))))
    return parser


def cli(argv: list[str] | None = None) -> int:
    """Entry point for ``python -m lite_indexer.query.sqd_backfill``."""
    args: Any = _build_parser().parse_args(argv)
    src = SQDSource()
    if args.day:
        days = [dt.date.fromisoformat(args.day)]
    else:
        days = list(iter_days(dt.datetime.now(dt.UTC).date(), args.days))
    failures = 0
    for day in days:
        try:
            print(_run_one(args, day, src))  # noqa: T201
        except (
            WriteError,
            SQDPortalError,
            UnsupportedChainError,
            UnsupportedTableError,
            MissingTableError,
        ) as exc:
            failures += 1
            print(  # noqa: T201
                f"FAIL {args.chain}/{args.table}/{day.isoformat()}: {type(exc).__name__}: {exc}"
            )
    return 1 if failures else 0
