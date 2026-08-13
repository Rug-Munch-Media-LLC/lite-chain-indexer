"""backfill-whales / backfill-dex-prices subcommand handlers."""

from __future__ import annotations

import argparse
import json

from lite_indexer.storage.clickhouse import ClickHouseWriter


def run_backfill_whales(args: argparse.Namespace) -> int:
    """Run the backfill-whales subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, days, limit, dry_run).

    Returns:
        Process exit code (0 = success, 1 = ClickHouse unavailable).
    """
    from lite_indexer.pricing import backfill_whales

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    result = backfill_whales(
        ch_client,
        days=args.days,
        chain=args.chain,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, default=str))  # noqa: T201
    return 0


def run_backfill_dex_prices(args: argparse.Namespace) -> int:
    """Run the backfill-dex-prices subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, days, limit, dry_run).

    Returns:
        Process exit code (0 = success, 1 = ClickHouse unavailable).
    """
    from lite_indexer.pricing import backfill_dex_prices

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    result = backfill_dex_prices(
        ch_client,
        days=args.days,
        chain=args.chain,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, default=str))  # noqa: T201
    return 0
