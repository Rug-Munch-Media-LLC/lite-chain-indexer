"""entities subparser — wallet-entity clustering run."""

from __future__ import annotations

from lite_indexer.cli._parser_types import SubParsers
from lite_indexer.entities.cex_deposit_writer import DEFAULT_CHAINS


def add_entities_parsers(sub: SubParsers) -> None:
    """Register the ``entities`` subcommand (wallet-entity clustering)."""
    parser = sub.add_parser(
        "entities",
        help="Build wallet-entity clusters (CSV-able evidence, not accusations)",
        description=(
            "Run a clustering heuristic over the warehouse and write scored rows "
            "to wallet_memory.wallet_entities. Idempotent: a re-run over unchanged "
            "data writes identical rows and ReplacingMergeTree collapses them. "
            "Every row carries a bounded confidence_score; nothing is promoted to "
            "a label."
        ),
    )
    parser.add_argument(
        "heuristic",
        choices=["cex-deposit"],
        help="Which clustering heuristic to run",
    )
    parser.add_argument(
        "--chain",
        action="append",
        dest="chains",
        metavar="CHAIN",
        help=f"Chain to process (repeatable; default: {', '.join(DEFAULT_CHAINS)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Score and report without writing to ClickHouse",
    )
