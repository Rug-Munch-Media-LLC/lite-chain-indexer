"""Lake family subcommands — sonarx lake → ClickHouse (backfill + gap scan)."""

from __future__ import annotations

import argparse
import json
import sys

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def add_lake_parsers(sub: argparse._SubParsersAction) -> None:
    """Register the lake family subparsers (lake-backfill, lake-gaps, sqd-fill)."""
    _add_backfill_parser(sub)
    _add_gaps_parser(sub)
    _add_sqd_parser(sub)
    _add_fill_solana_parser(sub)


def _add_sqd_parser(sub: argparse._SubParsersAction) -> None:
    """Register sqd-fill — near-live block-range hole fill from the SQD portal."""
    sqd = sub.add_parser(
        "sqd-fill",
        help="Fill an exact block-range hole from the near-live SQD portal",
        description="Stream ERC-20 transfers for an exact block range from portal.sqd.dev into ClickHouse.",
    )
    sqd.add_argument("--chain", default="base", choices=["base", "ethereum", "arbitrum"])
    sqd.add_argument("--from-block", type=int, required=True, help="Inclusive start block")
    sqd.add_argument("--to-block", type=int, required=True, help="Inclusive end block")
    sqd.add_argument("--batch-rows", type=int, default=5000, help="Rows per write batch (default: 5000)")
    sqd.add_argument("--dry-run", action="store_true", help="Count matched rows without writing")


def run_sqd_fill(args: argparse.Namespace) -> int:
    """Run sqd-fill — stream a block range from the portal into ClickHouse."""
    from lite_indexer.ingest.lake_sqd import SqdFillRequest, fill_chain_from_sqd

    req = SqdFillRequest(
        chain=args.chain,
        from_block=args.from_block,
        to_block=args.to_block,
        dry_run=args.dry_run,
        batch_rows=args.batch_rows,
    )
    try:
        counts = fill_chain_from_sqd(req)
    except ValueError as exc:
        logger.warning("sqd_fill_bad_request", error=str(exc)[:120])
        print(f"error: {exc}", file=sys.stderr)  # noqa: T201
        return 2
    print(json.dumps({"chain": args.chain, "dry_run": args.dry_run, "counts": counts}, indent=2))  # noqa: T201
    return 0


def _add_backfill_parser(sub: argparse._SubParsersAction) -> None:
    """Register lake-backfill — bulk history from the free sonarx lake, never RPC."""
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


def _add_gaps_parser(sub: argparse._SubParsersAction) -> None:
    """Register lake-gaps — auto-detect (chain, date) partitions missing in CH."""
    gaps = sub.add_parser(
        "lake-gaps",
        help="Find (chain,date) partitions present in the lake but missing in ClickHouse",
        description=(
            "Scan a lookback window for dates the lake can serve but ClickHouse "
            "has zero rows for; --fill backfills them through the router-aware "
            "lake backfill. No RPC."
        ),
    )
    gaps.add_argument("--chains", default="base", help="Comma-separated sonarx chains (default: base)")
    gaps.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    gaps.add_argument("--fill", action="store_true", help="Backfill the detected gaps from the lake")


def _add_fill_solana_parser(sub: argparse._SubParsersAction) -> None:
    """Register fill-solana — BigLake (BigQuery public solana) gap fill."""
    fill = sub.add_parser(
        "fill-solana",
        help="Fill solana history from the GCP BigLake dataset into ClickHouse",
        description=(
            "Stream SPL token transfers for a date range from the BigQuery public "
            "solana dataset into ClickHouse through the live solana write path. "
            "Volume-guarded: --max-rows hard-stops; --dry-run counts."
        ),
    )
    fill.add_argument("--from-date", required=True, help="Inclusive start date YYYY-MM-DD")
    fill.add_argument("--to-date", required=True, help="Inclusive end date YYYY-MM-DD")
    fill.add_argument("--max-rows", type=int, default=200_000, help="Hard stop across the run (default: 200000)")
    fill.add_argument("--batch-rows", type=int, default=5000, help="Rows per write batch (default: 5000)")
    fill.add_argument("--mint", action="append", default=[], help="Optional mint filter (repeatable)")
    fill.add_argument("--max-scan-gib", type=float, default=60.0,
                      help="Refuse runs whose estimated scan exceeds this (default 60)")
    fill.add_argument("--force-budget", action="store_true", help="Override the scan-budget guard")
    fill.add_argument("--since-ts", default=None,
                      help="Drop fetched rows older than this ISO ts (surgical resume fills)")
    fill.add_argument("--force", action="store_true", help="Write even for dates CH already covers")
    fill.add_argument("--dry-run", action="store_true", help="Count rows without writing")




def run_lake_command(args: argparse.Namespace) -> int:
    """Dispatch the lake family subcommands."""
    if args.command == "sqd-fill":
        return run_sqd_fill(args)
    if args.command == "fill-solana":
        from lite_indexer.cli._lake_fill import run_fill_solana

        return run_fill_solana(args)
    if args.command == "lake-gaps":
        return run_lake_gaps(args)
    return run_lake_backfill(args)


def run_lake_backfill(args: argparse.Namespace) -> int:
    """Run the lake-backfill subcommand (synchronous, no RPC).

    Args:
        args: Parsed CLI args (chain, from_date, to_date, tables, dry_run).

    Returns:
        Process exit code (0 = success, 2 = usage error).
    """
    from lite_indexer.ingest.lake_backfill import (
        LakeBackfillRequest,
        backfill_chain_from_lake,
        expand_dates,
    )

    try:
        dates = expand_dates(args.from_date, args.to_date)
    except ValueError:
        logger.warning("lake_bad_dates", from_date=args.from_date, to_date=args.to_date)
        print("error: --from-date/--to-date must be YYYY-MM-DD", file=sys.stderr)  # noqa: T201
        return 2
    if not dates:
        print("error: --to-date is before --from-date", file=sys.stderr)  # noqa: T201
        return 2

    tables = tuple(t.strip() for t in args.tables.split(",") if t.strip())
    counts = backfill_chain_from_lake(
        LakeBackfillRequest(chain=args.chain, dates=dates, tables=tables, dry_run=args.dry_run)
    )
    print(json.dumps({"chain": args.chain, "dry_run": args.dry_run, "counts": counts}, indent=2))  # noqa: T201
    return 0


def run_lake_gaps(args: argparse.Namespace) -> int:
    """Run the lake-gaps subcommand (scan, optionally fill). No RPC."""
    from lite_indexer.ingest.lake_gaps import main as scan_chain

    chains = [c.strip() for c in args.chains.split(",") if c.strip()]
    bad = [c for c in chains if c not in ("base", "arbitrum")]
    if bad:
        print(f"error: unsupported chain(s): {','.join(bad)}", file=sys.stderr)  # noqa: T201
        return 2
    results = [scan_chain(c, args.days, fill=args.fill) for c in chains]
    print(json.dumps(results, indent=2))  # noqa: T201
    return 0
