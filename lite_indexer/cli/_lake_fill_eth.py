"""fill-eth command — BigQuery ``crypto_ethereum`` → ClickHouse (streaming).

One REST job per (day, table); result pages are free, so a day is scanned
once. Volume + scan-budget guarded exactly like ``fill-solana``; no RPC
anywhere and no local Parquet staging.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def add_fill_eth_parser(sub: Any) -> None:
    """Register ``fill-eth`` — BigQuery crypto_ethereum → ClickHouse fill."""
    fill = sub.add_parser(
        "fill-eth",
        help="Fill ethereum history from bigquery-public-data.crypto_ethereum into ClickHouse",
        description=(
            "Stream ethereum token_transfers / traces for a date range from the "
            "BigQuery public dataset into ClickHouse through the live writers. "
            "Day-level idempotent (present days are skipped unless --force); "
            "--max-scan-gib guards the 1 TiB/month free tier; --estimate is a free dry run."
        ),
    )
    fill.add_argument("--from-date", required=True, help="Inclusive start date YYYY-MM-DD")
    fill.add_argument("--to-date", required=True, help="Inclusive end date YYYY-MM-DD")
    fill.add_argument(
        "--tables",
        default="token_transfers",
        help="Comma list: token_transfers,traces (default token_transfers)",
    )
    fill.add_argument(
        "--batch-rows",
        type=int,
        default=5000,
        help="Rows per ClickHouse write batch (default 5000)",
    )
    fill.add_argument("--max-scan-gib", type=float, default=50.0, help="Refuse runs whose estimate exceeds this")
    fill.add_argument("--force-budget", action="store_true", help="Override the scan-budget guard")
    fill.add_argument("--force", action="store_true", help="Write even for days ClickHouse already covers")
    fill.add_argument("--dry-run", action="store_true", help="Count rows without writing")
    fill.add_argument("--estimate", action="store_true", help="FREE dryRun price-check only (no writes)")


def run_fill_eth(args: argparse.Namespace) -> int:
    """Run fill-eth — BigQuery → ClickHouse, volume + scan guarded, no RPC."""
    from lite_indexer.ingest.lake_backfill import expand_dates
    from lite_indexer.ingest.lake_bigquery_eth import EthFillRequest, backfill_eth_from_bigquery
    from lite_indexer.query.bigquery_common import BigQueryQuotaError

    try:
        dates = expand_dates(args.from_date, args.to_date)
    except ValueError:
        print("error: --from-date/--to-date must be YYYY-MM-DD", file=sys.stderr)  # noqa: T201
        return 2
    if not dates:
        print("error: --to-date is before --from-date", file=sys.stderr)  # noqa: T201
        return 2
    tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
    if getattr(args, "estimate", False):
        return _print_estimate(dates, tables)
    req = EthFillRequest(
        dates=dates,
        tables=tables,
        dry_run=args.dry_run,
        batch_rows=args.batch_rows,
        force=args.force,
        max_scan_gib=args.max_scan_gib,
        force_budget=args.force_budget,
    )
    try:
        res = backfill_eth_from_bigquery(req)
    except BigQueryQuotaError as exc:
        logger.warning("bigquery_eth_fill_quota_exceeded", error=str(exc)[:160])
        print(  # noqa: T201
            "error: BigQuery free-bytes-scanned quota exhausted on every configured account.\n"
            "  rotation state: /var/data/chain-indexer/state/bigquery_accounts.json",
            file=sys.stderr,
        )
        return 3
    except ValueError as exc:
        logger.warning("bigquery_eth_fill_bad_request", error=str(exc)[:120])
        print(f"error: {exc}", file=sys.stderr)  # noqa: T201
        return 2
    print(  # noqa: T201
        json.dumps(
            {
                "chain": "ethereum",
                "dates": len(dates),
                "dry_run": req.dry_run,
                "rows": res.rows,
                "scan_gib_estimate": round(res.scan_gib, 2),
                "counts": res.counts,
                "null_values": res.null_values,
            },
            indent=2,
        )
    )
    return 0


def _print_estimate(dates: list[str], tables: tuple[str, ...]) -> int:
    """FREE dryRun pricing: exact bytes each (day, table) would scan."""
    from lite_indexer.ingest.lake_bigquery_eth import day_sql
    from lite_indexer.query.bigquery_cli import bq_accounts
    from lite_indexer.query.bigquery_rest import estimate_scan_bytes

    project, creds = bq_accounts()[0]
    total = 0
    for day in dates:
        for table in tables:
            n = estimate_scan_bytes(day_sql(table, day), [], project=project, creds_path=creds)
            total += n
            print(json.dumps({"date": day, "table": table, "scan_mb": round(n / 1e6, 1)}))  # noqa: T201
    print(  # noqa: T201
        json.dumps(
            {
                "dates": len(dates),
                "tables": list(tables),
                "total_scan_gib": round(total / 1024**3, 2),
            }
        )
    )
    return 0
