"""fill-solana command — BigLake solana fill (split from cli/_lake).

Volume + scan-budget guarded; never unbounded; ClickHouse-only writes.
"""
from __future__ import annotations

import json
import sys
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def run_fill_solana(args: argparse.Namespace) -> int:
    """Run fill-solana — BigLake → ClickHouse, volume-guarded, no RPC."""
    from lite_indexer.ingest.lake_backfill import expand_dates
    from lite_indexer.query.bigquery_common import BigQueryQuotaError
    from lite_indexer.ingest.lake_bigquery_solana import (
        SolanaBigQueryFillRequest,
        backfill_solana_from_bigquery,
    )

    try:
        dates = expand_dates(args.from_date, args.to_date)
    except ValueError:
        logger.warning("bigquery_fill_bad_dates", from_date=args.from_date, to_date=args.to_date)
        print("error: --from-date/--to-date must be YYYY-MM-DD", file=sys.stderr)  # noqa: T201
        return 2
    req = SolanaBigQueryFillRequest(
        dates=dates, dry_run=args.dry_run, batch_rows=args.batch_rows,
        max_rows=args.max_rows, mints=tuple(args.mint), force=args.force,
        max_scan_gib=args.max_scan_gib, force_budget=args.force_budget,
        since_ts=getattr(args, "since_ts", None),
        until_ts=getattr(args, "until_ts", None),
    )
    if getattr(args, "estimate", False):
        return _print_estimate(dates)
    try:
        counts = backfill_solana_from_bigquery(req)
    except BigQueryQuotaError as exc:
        logger.warning("bigquery_fill_quota_exceeded", error=str(exc)[:160])
        print(
            "error: BigQuery free-bytes-scanned quota exhausted on every configured account.\n"
            "  rotation state: /var/data/chain-indexer/state/bigquery_accounts.json\n"
            "  options: wait for the monthly reset, enable billing (Blaze, $6.25/TiB after the free 1 TiB),\n"
            "  or point at a fresh project: BIGQUERY_PROJECT_ID=<new-id> + GOOGLE_APPLICATION_CREDENTIALS=<new key>.",
            file=sys.stderr,
        )  # noqa: T201
        return 3
    except ValueError as exc:
        logger.warning("bigquery_fill_bad_request", error=str(exc)[:120])
        print(f"error: {exc}", file=sys.stderr)  # noqa: T201
        return 2
    summary = {"chain": "solana", "dry_run": req.dry_run, "max_rows": req.max_rows,
               "rows": sum(counts.values()), "counts": counts}
    print(json.dumps(summary, indent=2))  # noqa: T201
    return 0



def _print_estimate(dates: list[str]) -> int:
    """FREE dryRun pricing: exact bytes the fill would scan, per day."""
    import os as _os

    from lite_indexer.query.bigquery_cli import bq_accounts
    from lite_indexer.query.bigquery_rest import estimate_scan_bytes
    from lite_indexer.query.bigquery_spec import BigQuerySpec
    from lite_indexer.query.bigquery_tables import build_query

    project, creds = bq_accounts()[0]
    total = 0
    for day in dates:
        spec = BigQuerySpec("token_transfers", chain="solana", dates=[day], limit=50_000)
        sql, params = build_query(spec)
        n = estimate_scan_bytes(sql, params, project=project, creds_path=creds)
        total += n
        print(json.dumps({"date": day, "scan_gib": round(n / 1024**3, 2)}))  # noqa: T201
    print(json.dumps({"dates": len(dates), "total_scan_gib": round(total / 1024**3, 2), "transport": _os.environ.get("BIGQUERY_TRANSPORT", "cli")}))  # noqa: T201
    return 0
