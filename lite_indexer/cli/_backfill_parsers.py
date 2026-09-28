"""backfill-whales / backfill-dex-prices subparsers — retroactive dex_events mutations."""

from __future__ import annotations

import argparse

from lite_indexer.strategies import SUPPORTED_CHAINS


def add_backfill_parsers(sub: argparse._SubParsersAction) -> None:
    """Register the dex_events backfill subparsers (whale flag + USD prices)."""
    _add_backfill_whales_parser(sub)
    _add_backfill_dex_prices_parser(sub)


def _add_backfill_whales_parser(sub: argparse._SubParsersAction) -> None:
    """Register the backfill-whales subparser (retroactive whale flag)."""
    # ── backfill-whales subcommand ───────────────────────────────────
    # Re-scans recent dex_events and marks whale rows retroactively using
    # the token_prices USD feed + per-chain raw thresholds + top-0.1%
    # per-pair amounts. Intended for an operator cron after the whale
    # feature ships (existing rows all have whale=0).
    backfill_parser = sub.add_parser(
    "backfill-whales",
    help="Re-scan recent dex_events and mark whale rows retroactively",
    description=(
        "Recomputes the whale flag for recent dex_events rows using "
        "token_prices USD values, per-chain raw amount thresholds, and "
        "the top-0.1% per-pair amount, then issues ALTER TABLE UPDATE "
        "mutations for the rows that changed. Idempotent — re-running "
        "is a no-op. Use --dry-run to preview counts without mutating."
    ),
    )
    backfill_parser.add_argument(
    "--chain",
    default=None,
    choices=sorted(SUPPORTED_CHAINS.keys()),
    help="Restrict to a single chain (default: all chains)",
    )
    backfill_parser.add_argument(
    "--days",
    type=int,
    default=90,
    help="How many days back to scan (default: 90, matches dex_events retention)",
    )
    backfill_parser.add_argument(
    "--limit",
    type=int,
    default=500_000,
    help="Max rows to scan (default: 500000)",
    )
    backfill_parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Preview whale_up/whale_down counts without issuing mutations",
    )


def _add_backfill_dex_prices_parser(sub: argparse._SubParsersAction) -> None:
    """Register the backfill-dex-prices subparser (retroactive USD prices)."""
    # ── backfill-dex-prices subcommand ───────────────────────────────
    # Fills the ~98.8%-zero price_usd column on dex_events retroactively
    # from the token_prices table. Ingestion writes price_usd=0 when the
    # price feed had no value yet; this job joins recent zero-price rows
    # to token_prices and issues ALTER TABLE UPDATE mutations in batches.
    price_parser = sub.add_parser(
    "backfill-dex-prices",
    help="Backfill price_usd/volume_usd on dex_events from token_prices",
    description=(
        "Re-scans recent dex_events rows with price_usd = 0, joins "
        "them against wallet_memory.token_prices, and issues "
        "ALTER TABLE UPDATE mutations that fill price_usd (token0 "
        "price) and volume_usd (max-leg notional) for rows that have "
        "a resolvable price. Idempotent — re-running is a no-op. Use "
        "--dry-run to preview counts without mutating."
    ),
    )
    price_parser.add_argument(
    "--chain",
    default=None,
    choices=sorted(SUPPORTED_CHAINS.keys()),
    help="Restrict to a single chain (default: all chains)",
    )
    price_parser.add_argument(
    "--days",
    type=int,
    default=90,
    help="How many days back to scan (default: 90, matches dex_events retention)",
    )
    price_parser.add_argument(
    "--limit",
    type=int,
    default=500_000,
    help="Max rows to scan (default: 500000)",
    )
    price_parser.add_argument(
    "--dry-run",
    action="store_true",
    help="Preview updated/unchanged/unpriced counts without issuing mutations",
    )
