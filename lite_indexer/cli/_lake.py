"""lake-backfill subcommand handler — sonarx lake → ClickHouse."""

from __future__ import annotations

import argparse
import json
import sys


def add_lake_parser(sub: argparse._SubParsersAction) -> None:
    """Register the lake-backfill subparser (bulk history from the free
    sonarx lake — base/arbitrum transfers + traces, never RPC; --dry-run
    counts rows per partition without writing)."""
    lake_parser = sub.add_parser(
        "lake-backfill",
        help="Backfill base/arbitrum history from the AWS v1.1 sonarx lake",
        description=(
            "Ingest transfers + traces for a date range from "
            "s3://aws-public-blockchain/v1.1/sonarx into ClickHouse. "
            "No RPC. Missing partitions are logged and skipped."
        ),
    )
    lake_parser.add_argument(
        "--chain", default="base", choices=["base", "arbitrum"], help="Sonarx chain (default: base)"
    )
    lake_parser.add_argument("--from-date", required=True, help="Inclusive start date YYYY-MM-DD")
    lake_parser.add_argument("--to-date", required=True, help="Inclusive end date YYYY-MM-DD")
    lake_parser.add_argument("--tables", default="transfers,traces", help="Comma-separated: transfers,traces")
    lake_parser.add_argument("--dry-run", action="store_true", help="Count rows per partition without writing")


def run_lake_backfill(args: argparse.Namespace) -> int:
    """Run the lake-backfill subcommand (synchronous, no RPC).

    Args:
        args: Parsed CLI args (chain, from_date, to_date, tables, dry_run).

    Returns:
        Process exit code (0 = success, 2 = usage error).
    """
    from lite_indexer.ingest.lake_backfill import backfill_chain_from_lake, expand_dates

    try:
        dates = expand_dates(args.from_date, args.to_date)
    except ValueError:
        print("error: --from-date/--to-date must be YYYY-MM-DD", file=sys.stderr)  # noqa: T201
        return 2
    if not dates:
        print("error: --to-date is before --from-date", file=sys.stderr)  # noqa: T201
        return 2

    tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
    counts = backfill_chain_from_lake(args.chain, dates, tables=tables, dry_run=args.dry_run)
    print(json.dumps({"chain": args.chain, "dry_run": args.dry_run, "counts": counts}, indent=2))  # noqa: T201
    return 0
