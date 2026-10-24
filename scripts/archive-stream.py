#!/usr/bin/env python3
"""Stream-export one large unpartitioned table in a SINGLE ClickHouse pass.

Usage:
  archive-stream.py --table address_activity --cutoff-day yesterday
  archive-stream.py --table address_activity --dry-run   # what the pass would see
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lite_indexer.storage.archive_export import discover_units, spec_for  # noqa: E402
from lite_indexer.storage.archive_stream import export_stream  # noqa: E402


def parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True)
    ap.add_argument("--cutoff-day", default=(dt.datetime.now(dt.UTC).date() - dt.timedelta(days=1)).isoformat())
    ap.add_argument("--chain", action="append", default=[])
    ap.add_argument("--dry-run", action="store_true")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_cli(argv)
    spec = spec_for(args.table)
    units = [u for u in discover_units(spec, args.chain) if u.day < args.cutoff_day]
    expected = {(u.chain, u.day): u.rows for u in units}
    print(
        f"== stream {args.table}: {len(units)} closed units, {sum(expected.values())} rows, cutoff={args.cutoff_day}",
        flush=True,
    )
    if args.dry_run:
        print(json.dumps(sorted((f"{c}/{d}", r) for (c, d), r in expected.items())[:5], indent=1))
        return 0
    summary = export_stream(spec, expected, args.cutoff_day)
    print(json.dumps(summary, indent=1, sort_keys=True), flush=True)
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
