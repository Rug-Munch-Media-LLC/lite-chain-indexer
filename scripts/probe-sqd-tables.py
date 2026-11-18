#!/usr/bin/env python3
"""Re-measure which SQD portal tables each EVM chain actually serves.

The static maps in ``lite_indexer/query/sqd_tables.py`` are the LAST
measurement, kept so a normal run costs zero probe requests. This script is how
that measurement is reproduced — run it after SQD announces a dataset change, or
before trusting ``NO_TRACES`` for a chain nobody has backfilled yet.

Sequential and slow on purpose: the portal answers a burst with HTTP 529, and a
banned account loses all 14 chains. One request per (chain, table), with a sleep.

Usage:
  python3 scripts/probe-sqd-tables.py
  python3 scripts/probe-sqd-tables.py --sleep 4
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Run as a script: `scripts/` is not on sys.path, so the package would not
# import. Anchor to the repo root so `python3 scripts/probe-sqd-tables.py`
# works the way its own docstring says it does.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lite_indexer.query.sqd_source import SQDPortalError, SQDSource
from lite_indexer.query.sqd_tables import probe_logs, probe_traces


def main(argv: list[str] | None = None) -> int:
    """Probe every EVM chain for traces and logs; print a summary table."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--sleep", type=float, default=3.0, help="seconds between chains")
    args = parser.parse_args(argv)
    src = SQDSource(min_interval=args.sleep, retry_backoffs=(15.0, 40.0, 90.0))
    rows: list[tuple[str, str, str]] = []
    failures = 0
    for chain in sorted(src.EVM_DATASETS):
        try:
            traces = probe_traces(src, chain)
            time.sleep(args.sleep)
            logs = probe_logs(src, chain)
        except SQDPortalError as exc:
            failures += 1
            rows.append((chain, "ERR", str(exc)[:60]))
            print(f"{chain:<16} ERROR {exc}", file=sys.stderr)  # noqa: T201
            time.sleep(args.sleep)
            continue
        rows.append((chain, "yes" if traces else "NO", "yes" if logs else "NO"))
        print(f"{chain:<16} traces={'yes' if traces else 'NO':<4} logs={'yes' if logs else 'NO'}", flush=True)  # noqa: T201
        time.sleep(args.sleep)
    no_traces = [c for c, t, _ in rows if t == "NO"]
    no_logs = [c for c, _, lg in rows if lg == "NO"]
    print(f"\nNO_TRACES = {sorted(no_traces)}")  # noqa: T201
    print(f"NO_LOGS   = {sorted(no_logs)}")  # noqa: T201
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
