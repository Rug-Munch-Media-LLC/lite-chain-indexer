#!/usr/bin/env python3
"""HyperSync -> Parquet backfill PoC for Base (bounded slice, measured).

Reads Base ERC-20 Transfer logs via Envio HyperSync and writes Parquet
partitions under ./out/<chain>/<date>/. Never touches production paths and
never rewrites an existing partition (append-only).

Usage:
    ENVIO_API_TOKEN=<token> python backfill_base.py --days 7
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import hypersync
import pyarrow as pa
import pyarrow.parquet as pq
from hypersync import (
    BlockField,
    FieldSelection,
    HexOutput,
    LogField,
    LogSelection,
    Query,
    StreamConfig,
)

CHAIN = "base"
ENDPOINT = "https://base.hypersync.xyz"
BASE_SECONDS = 2  # Base post-merge: 2s block time
OUT_ROOT = Path(__file__).resolve().parent / "out"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

SCHEMA = pa.schema(
    [
        pa.field("chain", pa.string()),
        pa.field("block_number", pa.int64()),
        pa.field("block_timestamp", pa.int64()),
        pa.field("tx_hash", pa.string()),
        pa.field("log_index", pa.int64()),
        pa.field("contract_address", pa.string()),
        pa.field("from_addr", pa.string()),
        pa.field("to_addr", pa.string()),
        pa.field("value", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=7, help="days of history to pull")
    p.add_argument("--from-block", type=int, default=0, help="override start block")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--batch-size", type=int, default=100_000)
    p.add_argument("--max-rows", type=int, default=0, help="abort after N rows (0=no cap)")
    p.add_argument("--rows-per-file", type=int, default=250_000)
    p.add_argument("--out", type=Path, default=OUT_ROOT)
    return p.parse_args()


def addr(topic: object) -> str:
    return "0x" + str(topic)[-40:].lower()


def build_query(start: int, end: int) -> Query:
    fields = FieldSelection(
        block=[BlockField.NUMBER, BlockField.TIMESTAMP],
        log=[
            LogField.LOG_INDEX,
            LogField.TRANSACTION_HASH,
            LogField.ADDRESS,
            LogField.TOPIC1,
            LogField.TOPIC2,
            LogField.DATA,
        ],
    )
    return Query(
        from_block=start,
        to_block=end,
        field_selection=fields,
        logs=[LogSelection(topics=[[TRANSFER_TOPIC]])],
    )


def log_to_row(log, blocks: dict) -> dict:
    return {
        "chain": CHAIN,
        "block_number": int(log.block_number),
        "block_timestamp": int(blocks.get(log.block_number, 0)),
        "tx_hash": str(log.transaction_hash),
        "log_index": int(log.log_index),
        "contract_address": str(log.address).lower(),
        "from_addr": addr(log.topic1),
        "to_addr": addr(log.topic2),
        "value": str(int.from_bytes(bytes(log.data), "big")),
    }


def write_partition(rows: list[dict], out_root: Path) -> list[tuple[str, int, int]]:
    """Write rows to <chain>/<date>/<file>.parquet bucketed by block date."""
    buckets: dict[str, list[dict]] = {}
    for r in rows:
        ts = r["block_timestamp"]
        day = (
            datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%d")
            if ts
            else "unknown-date"
        )
        buckets.setdefault(day, []).append(r)

    written: list[tuple[str, int, int]] = []
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for day, day_rows in sorted(buckets.items()):
        part_dir = out_root / CHAIN / day
        part_dir.mkdir(parents=True, exist_ok=True)
        path = part_dir / f"transfers-{stamp}-{len(day_rows)}.parquet"
        pq.write_table(pa.Table.from_pylist(day_rows, schema=SCHEMA), path,
                       compression="zstd")
        written.append((str(path), len(day_rows), path.stat().st_size))
    return written


async def _bootstrap(client: hypersync.HypersyncClient) -> tuple[int, int]:
    """Fetch chain height + chain id (both async in hypersync 1.2.x)."""
    height, chain_id = await asyncio.gather(
        client.get_height(), client.get_chain_id()
    )
    return int(height), int(chain_id)


def main() -> int:
    args = parse_args()
    token = os.environ.get("ENVIO_API_TOKEN", "").strip()
    if not token:
        print(
            "FATAL: ENVIO_API_TOKEN not set. HyperSync rejects unauthenticated\n"
            "requests (HTTP 401 'Your token is malformed'). Create a free-package\n"
            "token at https://envio.dev/app/api-tokens and export it.",
            file=sys.stderr,
        )
        return 2

    client = hypersync.HypersyncClient(
        hypersync.ClientConfig(url=ENDPOINT, api_token=token)
    )
    try:
        height, chain_id = asyncio.run(_bootstrap(client))
    except Exception as exc:
        print(f"FATAL: cannot reach HyperSync: {exc}", file=sys.stderr)
        return 3

    end = height - 1
    start = args.from_block or max(0, end - (args.days * 86_400) // BASE_SECONDS)
    print(f"chain={CHAIN} endpoint={ENDPOINT} chain_id={chain_id} height={height:,}")
    print(f"window: {start:,}..{end:,} ({end - start:,} blocks, {args.days}d)")

    q = build_query(start, end)
    config = StreamConfig(
        hex_output=HexOutput.PREFIXED,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
    )

    t0 = time.monotonic()
    rows = 0
    files: list[tuple[str, int, int]] = []
    total_bytes = 0
    buf: list[dict] = []
    pages = 0
    error = None

    try:
        stream = client.stream(q, config)
        while True:
            res = stream.recv()
            if res is None:
                break
            pages += 1
            blocks = {b.number: b.timestamp for b in res.data.blocks}
            for log in res.data.logs:
                buf.append(log_to_row(log, blocks))
                rows += 1
            q.from_block = res.next_block
            while len(buf) >= args.rows_per_file:
                chunk, buf = buf[: args.rows_per_file], buf[args.rows_per_file:]
                w = write_partition(chunk, args.out)
                files.extend(w)
                total_bytes += sum(x[2] for x in w)
            if args.max_rows and rows >= args.max_rows:
                print(f"stopping early at max-rows={args.max_rows}")
                break
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"

    if buf and error is None:
        w = write_partition(buf, args.out)
        files.extend(w)
        total_bytes += sum(x[2] for x in w)

    elapsed = max(time.monotonic() - t0, 1e-9)
    rps = rows / elapsed
    blocks_done = q.from_block - start
    days_done = (blocks_done * BASE_SECONDS) / 86_400
    days_per_sec = days_done / elapsed if days_done else 0.0

    report = {
        "chain": CHAIN,
        "endpoint": ENDPOINT,
        "chain_id": chain_id,
        "height": height,
        "start_block": start,
        "end_block": end,
        "requested_days": args.days,
        "blocks_scanned": blocks_done,
        "days_scanned": round(days_done, 4),
        "rows": rows,
        "pages": pages,
        "elapsed_s": round(elapsed, 2),
        "rows_per_sec": round(rps, 1),
        "bytes_on_disk": total_bytes,
        "bytes_per_row": round(total_bytes / rows, 2) if rows else None,
        "mb_per_day": round(total_bytes / days_done / 1e6, 3) if days_done else None,
        "files": [{"path": p, "rows": n, "bytes": b} for p, n, b in files],
        "error": error,
    }
    if days_per_sec > 0:
        report["projected_90d_hours"] = round(90 / days_per_sec / 3600, 2)
        report["projected_90d_rows"] = int(rps * 90 / days_per_sec)
        report["projected_90d_gb"] = round(
            (total_bytes / days_done) * 90 / 1e9, 2
        )

    try:
        report["rate_limit_info"] = str(client.rate_limit_info())
    except Exception as exc:
        report["rate_limit_info"] = f"unavailable: {type(exc).__name__}: {exc}"

    args.out.mkdir(parents=True, exist_ok=True)
    out_json = args.out / "last_run_report.json"
    out_json.write_text(json.dumps(report, indent=2))
    print("\n=== RESULTS ===")
    print(json.dumps(report, indent=2))
    print(f"\nreport -> {out_json}")
    return 0 if error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
