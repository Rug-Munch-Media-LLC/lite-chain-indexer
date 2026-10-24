"""Lake runner commands — split out of _lake.py to hold the 200-line cap."""

from __future__ import annotations

import argparse
import json
import sys

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def run_lake_command(args: argparse.Namespace) -> int:
    """Dispatch the lake family subcommands."""
    if args.command == "orphan-sweep":
        from lite_indexer.cli._orphan_sweep import run_orphan_sweep

        return run_orphan_sweep(args)
    if args.command == "sqd-fill":
        from lite_indexer.cli._lake import run_sqd_fill

        return run_sqd_fill(args)
    if args.command == "fill-eth":
        from lite_indexer.cli._lake_fill_eth import run_fill_eth

        return run_fill_eth(args)
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
