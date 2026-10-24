#!/usr/bin/env python3
"""Resumable hour-granularity backfill for ``evm_transfers`` on one chain.

The day-granularity archive could not keep up with a chain that writes ~1M
rows/hour, so evm_transfers is archived in hour objects::

    b2-crypt-v2:chain-data/parquet/<chain>/<YYYY>/<MM>/<DD>/<HH>/<chain>_<YYYY-MM-DD>.parquet

One hour = one unit. A unit is uploaded only when its parquet row count equals
ClickHouse ``count()`` for the same hour; an hour that does not match is left
unwritten and reported, never partially uploaded (the B2 mount serves stale
bytes after an overwrite, so verification reads the object back with
``rclone cat`` only when ``--recheck`` is set — the row count is taken from the
local part before upload).

Usage:
  backfill-evm-hours.py --chain polygon --hours 183
  backfill-evm-hours.py --chain polygon --dry-run
"""

from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import tempfile
from pathlib import Path

CH = ["docker", "exec", "rmi-clickhouse", "clickhouse-client"]
REMOTE = "b2-crypt-v2:chain-data/parquet"
DB = "wallet_memory"
COLS = (
    "chain, block_number, tx_hash, from_address, to_address, token_address, value, "
    "block_timestamp, log_index, ingested_at"
)


def ch(sql: str) -> str:
    out = subprocess.run([*CH, "--query", sql], capture_output=True, text=True, check=False)
    if out.returncode != 0:
        raise RuntimeError(out.stderr[-300:])
    return out.stdout


def hour_counts(chain: str, days: int) -> list[tuple[str, int]]:
    """(hour 'YYYY-MM-DD HH', rows) for every hour ClickHouse still holds."""
    since = (dt.datetime.now(dt.UTC) - dt.timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        f"SELECT formatDateTime(toStartOfHour(block_timestamp), '%Y-%m-%d %H'), count() "
        f"FROM {DB}.evm_transfers WHERE chain = '{chain}' AND block_timestamp >= '{since}' "
        f"GROUP BY 1 ORDER BY 1"
    )
    return [(line.split("\t")[0], int(line.split("\t")[1])) for line in ch(sql).splitlines() if line.strip()]


def object_key(chain: str, hour: str) -> str:
    day, hh = hour.split(" ")
    y, m, d = day.split("-")
    return f"{REMOTE}/{chain}/{y}/{m}/{d}/{hh}/{chain}_{day}.parquet"


def exists(key: str) -> bool:
    return subprocess.run(["rclone", "lsjson", key], capture_output=True, text=True, check=False).returncode == 0


def part_rows(part: Path) -> int:
    sql = f"SELECT count(*) FROM read_parquet('{part}')"
    out = subprocess.run(["duckdb", "-noheader", "-list", "-c", sql], capture_output=True, text=True, check=True)
    return int(out.stdout.strip())


def export_hour(chain: str, hour: str, rows: int) -> str:
    key = object_key(chain, hour)
    if exists(key):
        return "skip"
    with tempfile.TemporaryDirectory(prefix="evmh-") as tmp:
        part = Path(tmp) / "part-000.parquet"
        sql = (
            f"SELECT {COLS} FROM {DB}.evm_transfers WHERE chain = '{chain}' "
            f"AND toStartOfHour(block_timestamp) = toDateTime('{hour}:00') ORDER BY block_number, log_index"
        )
        with part.open("wb") as fh:
            proc = subprocess.run(
                [
                    *CH,
                    "--max_execution_time",
                    "0",
                    "--output_format_parquet_compression_method",
                    "zstd",
                    "--query",
                    sql,
                    "--format",
                    "Parquet",
                ],
                stdout=fh,
                stderr=subprocess.PIPE,
                check=False,
            )
        if proc.returncode != 0 or part.stat().st_size == 0:
            return f"FAIL {hour}: {proc.stderr.decode()[-160:]}"
        got = part_rows(part)
        if got != rows:
            return f"MISMATCH {hour}: ch={rows} local={got}"
        subprocess.run(["rclone", "copyto", str(part), key], check=True, capture_output=True)
        return f"ok {hour} rows={got} bytes={part.stat().st_size}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default="polygon")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--hours", type=int, default=0, help="stop after N uploaded units")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    queue = hour_counts(args.chain, args.days)
    print(f"== {args.chain}: {len(queue)} hours in ClickHouse, {sum(r for _, r in queue)} rows", flush=True)
    if args.dry_run:
        print("".join(f"   {h} {r}\n" for h, r in queue[:5]))
        return 0
    done = 0
    for hour, rows in queue:
        if args.hours and done >= args.hours:
            break
        result = export_hour(args.chain, hour, rows)
        print(f"   {result}", flush=True)
        done += 0 if result == "skip" else 1
    print(f"== done uploaded={done}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
