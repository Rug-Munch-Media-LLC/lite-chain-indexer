"""integrity-check / reconcile subparsers — durable-store audit + balance reconciliation."""

from __future__ import annotations

import argparse

from lite_indexer.config import RECONCILIATION_SAMPLE_SIZE
from lite_indexer.strategies import SUPPORTED_CHAINS


def add_integrity_parsers(sub: argparse._SubParsersAction) -> None:
    """Register the integrity family subparsers (audit + balance reconciliation)."""
    _add_integrity_check_parser(sub)
    _add_reconcile_parser(sub)


def _add_integrity_check_parser(sub: argparse._SubParsersAction) -> None:
    """Register the integrity-check subparser (daily durable-store audit)."""
    # ── integrity-check subcommand ──────────────────────────────────
    # Daily durable-store audit: per-chain min/max/distinct/count from
    # ClickHouse with a contiguity percentage. Flags chains whose row
    # counts do not match the expected block span so operators know
    # before missing data compounds. Intended for a daily cron.
    integrity_parser = sub.add_parser(
    "integrity-check",
    help="Audit per-chain ClickHouse row counts vs expected ranges",
    description=(
        "Daily integrity check: for each chain, compare the ClickHouse "
        "row/range stats (min/max block, distinct blocks, total rows) "
        "against the expected contiguous span and report a contiguity "
        "percentage. Chains with gaps or empty stores are flagged."
    ),
    )
    integrity_parser.add_argument(
    "--chains",
    type=str,
    default="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,linea,scroll,zksync,mantle,gnosis,solana,tron",
    help="Comma-separated chains to audit (default: all supported)",
    )


def _add_reconcile_parser(sub: argparse._SubParsersAction) -> None:
    """Register the reconcile subparser (TrueBlocks-style balance reconciliation)."""
    # ── reconcile subcommand (TrueBlocks-style balance reconciliation) ─
    # Runs a balance-completeness check on a random sample of wallets
    # (default 100 per chain) and caches results in
    # wallet_memory.reconciliation_results. Intended for a daily cron.
    reconcile_parser = sub.add_parser(
    "reconcile",
    help="Run balance reconciliation on a random wallet sample",
    description=(
        "TrueBlocks-style reconciliation: compare computed balances "
        "from indexed evm_transfers deltas against on-chain "
        "eth_getBalance for a random sample of wallets, then cache "
        "results in wallet_memory.reconciliation_results. "
        "Gated by RECONCILIATION_ENABLED (default on)."
    ),
    )
    reconcile_parser.add_argument(
    "--chain",
    default="ethereum",
    choices=sorted(SUPPORTED_CHAINS.keys()),
    help="Chain to reconcile (default: ethereum)",
    )
    reconcile_parser.add_argument(
    "--sample-size",
    type=int,
    default=RECONCILIATION_SAMPLE_SIZE,
    help=f"Wallets to sample per run (default: {RECONCILIATION_SAMPLE_SIZE})",
    )
