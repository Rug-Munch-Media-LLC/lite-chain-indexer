"""Lake family subcommands — sonarx lake → ClickHouse (backfill + gap scan)."""

from __future__ import annotations

import argparse
import json
import sys

# Re-exported so `from lite_indexer.cli._lake import run_lake_*` keeps working.
from lite_indexer.cli._lake_runners import (  # noqa: F401
    run_lake_backfill,
    run_lake_command,
    run_lake_gaps,
)
from lite_indexer.cli._parser_types import SubParsers
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def add_lake_parsers(sub: SubParsers) -> None:
    """Register the maintenance family subparsers.

    The lake/sqd/fill group plus ``orphan-sweep``: both are read-first
    maintenance commands an operator runs by hand, and grouping them keeps
    ``cli/__init__.py`` (shrink-only) at one registration call.
    """
    _add_backfill_parser(sub)
    _add_gaps_parser(sub)
    _add_sqd_parser(sub)
    _add_fill_solana_parser(sub)
    from lite_indexer.cli._lake_fill_eth import add_fill_eth_parser

    add_fill_eth_parser(sub)
    from lite_indexer.cli._orphan_sweep import add_orphan_sweep_parser

    add_orphan_sweep_parser(sub)


def _add_sqd_parser(sub: SubParsers) -> None:
    """Register sqd-fill — near-live block-range hole fill from the SQD portal."""
    sqd = sub.add_parser(
        "sqd-fill",
        help="Fill an exact block-range hole from the near-live SQD portal",
        description="Stream ERC-20 transfers for an exact block range from portal.sqd.dev into ClickHouse.",
    )
    # 2026-10-11: was a hardcoded 3-chain subset — the SQD portal map supports
    # 13 EVM datasets (incl. gnosis/celo/zksync/scroll/linea/mantle/polygon/...).
    # Derive the choices from the map so a supported chain is never CLI-blocked.
    try:
        from lite_indexer.query.sqd_source import SQDSource as _SQDSource

        _sqd_choices = sorted(_SQDSource.EVM_DATASETS)
    except Exception:  # noqa: BLE001 — fall back to the historical subset
        _sqd_choices = ["arbitrum", "base", "ethereum"]
    sqd.add_argument("--chain", default="base", choices=_sqd_choices)
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


def _add_backfill_parser(sub: SubParsers) -> None:
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


def _add_gaps_parser(sub: SubParsers) -> None:
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


def _add_fill_solana_parser(sub: SubParsers) -> None:
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
    fill.add_argument(
        "--max-scan-gib", type=float, default=60.0, help="Refuse runs whose estimated scan exceeds this (default 60)"
    )
    fill.add_argument("--force-budget", action="store_true", help="Override the scan-budget guard")
    fill.add_argument(
        "--estimate", action="store_true", help="FREE dryRun price-check (REST transport); makes no writes"
    )
    fill.add_argument("--until-ts", default=None, help="Drop fetched rows newer than this ISO ts (surgical hole fills)")
    fill.add_argument(
        "--since-ts", default=None, help="Drop fetched rows older than this ISO ts (surgical resume fills)"
    )
    fill.add_argument("--force", action="store_true", help="Write even for dates CH already covers")
    fill.add_argument("--dry-run", action="store_true", help="Count rows without writing")
