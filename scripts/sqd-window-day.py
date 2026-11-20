"""Run one SQD day as a sequence of sittings, then publish. Operator command.

WHY A SCRIPT AND NOT A FLAG
    `publish_day` fetches a whole day in one pass, which the portal refuses
    (measured: sheds at ~61k rows). This driver does the sitting loop and is the
    only supported way to materialise a real chain-day. It stays out of the
    CLI's default path on purpose: an operator should choose windowing
    explicitly, not get it silently.

USAGE
    python3 scripts/sqd-window-day.py --chain polygon --table evm_transfers \\
        --day 2026-11-16 --sit-seconds 100

    Repeat the SAME invocation after a cool-down to resume: the cursor file
    records progress and only fetches what is still owed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lite_indexer.archive import UnitSpec, write_unit  # noqa: E402
from lite_indexer.query.sqd_window import (  # noqa: E402
    DEFAULT_ROWS_PER_SITTING,
    DEFAULT_STATE_DIR,
    assemble_parts,
    clear_parts,
    open_day,
    run_sitting,
)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--chain", required=True)
    ap.add_argument("--table", default="internal_transfers")
    ap.add_argument("--day", required=True, help="YYYY-MM-DD (UTC)")
    ap.add_argument("--remote", default="b2-crypt-v2:chain-data/parquet")
    ap.add_argument("--rows-per-sitting", type=int, default=DEFAULT_ROWS_PER_SITTING)
    ap.add_argument("--sit-seconds", type=float, default=0.0, help="wall-clock cap for one sitting; 0 = rows cap only")
    ap.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    ap.add_argument("--part-dir", type=Path, default=None)
    ap.add_argument("--max-sittings", type=int, default=1, help="sittings to run now; resume with the same command")
    ap.add_argument("--publish", action="store_true", help="publish once the day is complete and verified")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="report the day window and state without fetching or uploading",
    )
    a = ap.parse_args(argv)

    day = dt.date.fromisoformat(a.day)
    part_dir = a.part_dir or (a.state_dir / "parts" / f"{a.chain}__{a.table}__{a.day}")
    state = open_day(a.chain, a.table, day, state_dir=a.state_dir)

    if a.dry_run:
        # BACKFILL-PLAN Phase 3: report the days the source actually offers.
        # Do not assume 90 -- SQD claims 2021+, but the dataset's coverage for
        # THAT table on THAT chain is the only number that matters.
        print(f"chain      : {a.chain}")
        print(f"table      : {a.table}")
        print(f"day        : {a.day} (UTC)")
        remaining = state.remaining_blocks
        print(f"block window: {state.next_block} .. {state.last_block} ({remaining} remaining)")
        print(f"state      : complete={state.complete} rows_so_far={state.rows_so_far} sittings={state.sittings}")
        print(f"remote     : {a.remote}")
        print("dry run: nothing fetched, nothing uploaded")
        return 0

    if state.complete:
        print(f"day already complete: {state.rows_so_far} rows in {state.sittings} sittings")
    else:
        for n in range(max(1, a.max_sittings)):
            started = time.monotonic()
            state = run_sitting(
                state,
                state_dir=a.state_dir,
                part_dir=part_dir,
                rows_per_sitting=a.rows_per_sitting,
            )
            took = time.monotonic() - started
            print(
                f"sitting {n + 1}: rows={state.rows_so_far} "
                f"next={state.next_block} remaining={state.remaining_blocks} "
                f"complete={state.complete} {took:.1f}s"
            )
            if state.complete:
                break
            if a.sit_seconds and took >= a.sit_seconds:
                break
            # Cool down between sittings: the ceiling is volume-based, so the
            # portal needs a pause before it will serve the next window set.
            time.sleep(30)

    if not state.complete:
        print("NOT complete — re-run the same command to resume")
        return 2

    if not a.publish:
        print("complete; pass --publish to upload")
        return 0

    rows = assemble_parts(part_dir)
    spec = UnitSpec(chain=a.chain, table=a.table, date=a.day, expected_rows=len(rows))
    result = write_unit(spec, rows, remote=a.remote)
    removed = clear_parts(part_dir)
    print(f"published rows={result.rows} bytes={result.bytes_written} key={result.key} parts_cleared={removed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
