"""query subparser — on-demand historical lookups routed CH → cold → RPC."""

from __future__ import annotations

import argparse

from lite_indexer.strategies import SUPPORTED_CHAINS


def add_query_parsers(sub: argparse._SubParsersAction) -> None:
    """Register the query subparser (on-demand historical lookup)."""
    _add_query_parser(sub)


def _add_query_parser(sub: argparse._SubParsersAction) -> None:
    """Register the query subparser (on-demand historical lookup)."""
    # ── query subcommand (on-demand historical RPC) ──────────────────
    # Answer a query about ANY block/tx/address historically WITHOUT
    # indexing the whole chain: the QueryRouter routes to ClickHouse
    # (indexed) → B2 parquet (cold) → live RPC (on-demand), so the CLI
    # never says "data not available". Exactly one of --tx / --address /
    # --block / --deployer must be given.
    query_parser = sub.add_parser(
    "query",
    help="On-demand historical RPC query (no full indexing required)",
    description=(
        "Answer a query about ANY block/tx/address historically by "
        "routing to the best data source: ClickHouse (indexed) → B2 "
        "parquet (cold) → live RPC (on-demand). Exactly one of "
        "--tx / --address / --block / --deployer must be given."
    ),
    )
    query_parser.add_argument(
    "--chain",
    default="ethereum",
    choices=sorted(SUPPORTED_CHAINS.keys()),
    help="Chain to query (default: ethereum)",
    )
    query_parser.add_argument(
    "--tx",
    type=str,
    default=None,
    help="Transaction hash to look up (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
    "--address",
    type=str,
    default=None,
    help="Wallet address for transfer history (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
    "--block",
    type=int,
    default=None,
    help="Block number to fetch (blocks are always live RPC)",
    )
    query_parser.add_argument(
    "--deployer",
    type=str,
    default=None,
    help="Contract address to resolve its deployer (routed CH → cold → RPC)",
    )
    query_parser.add_argument(
    "--from-block",
    type=int,
    default=None,
    help="Inclusive start block for --address queries",
    )
    query_parser.add_argument(
    "--to-block",
    type=int,
    default=None,
    help="Inclusive end block for --address queries",
    )
    query_parser.add_argument(
    "--limit",
    type=int,
    default=100,
    help="Max rows for --address queries (default: 100)",
    )
    query_parser.add_argument(
    "--no-rpc",
    action="store_true",
    help="Disable the on-demand RPC fallback (indexed data only)",
    )
