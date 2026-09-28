"""attest subparser — on-chain-verifiable index attestations."""

from __future__ import annotations

import argparse

from lite_indexer.strategies import SUPPORTED_CHAINS


def add_attest_parsers(sub: argparse._SubParsersAction) -> None:
    """Register the attest subparser (trustless dataset verification)."""
    _add_attest_parser(sub)


def _add_attest_parser(sub: argparse._SubParsersAction) -> None:
    """Register the attest subparser (verify / show range attestations)."""
    # ── attest subcommand (on-chain-verifiable index attestations) ───
    attest_parser = sub.add_parser(
    "attest",
    help="Verify / inspect on-chain-verifiable range attestations",
    description=(
        "Trustless dataset verification: prove an indexed range is a "
        "leaf of the persisted Merkle accumulator and (optionally) that "
        "its committed block hash matches a chain-supplied block hash. "
        "No provider or producer trust required."
    ),
    )
    attest_sub = attest_parser.add_subparsers(dest="attest_action")
    attest_verify = attest_sub.add_parser("verify", help="Verify a manifest file")
    attest_verify.add_argument("manifest", help="Path to a manifest JSON file")
    attest_verify.add_argument(
    "--expected-root",
    default=None,
    help="Trusted accumulator root (hex) the manifest must match",
    )
    attest_show = attest_sub.add_parser("show", help="Show a chain's accumulator state")
    attest_show.add_argument(
    "--chain",
    default="ethereum",
    choices=sorted(SUPPORTED_CHAINS.keys()),
    help="Chain whose attestation state to show (default: ethereum)",
    )
