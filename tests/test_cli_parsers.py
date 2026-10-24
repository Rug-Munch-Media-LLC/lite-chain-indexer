"""Guards the CLI argparse surface against drift during module extraction.

The subcommand parsers live in six ``lite_indexer.cli._*_parsers`` siblings
plus the lake/orphan family. A move that drops an ``add_argument``, renames a
flag, or forgets to call a family's ``add_*_parsers`` entry point fails here
rather than in a cron job.
"""

from __future__ import annotations

import argparse
import contextlib
import io
from typing import Any

import pytest

from lite_indexer.cli import parse_args

# Every subcommand registered at 2dc9bba, in the order `--help` prints them.
EXPECTED_SUBCOMMANDS = [
    "integrity-check",
    "reconcile",
    "attest",
    "export-parquet",
    "backfill-whales",
    "backfill-dex-prices",
    "bootstrap",
    "query",
    "lake-backfill",
    "lake-gaps",
    "sqd-fill",
    "fill-solana",
    "fill-eth",
    "orphan-sweep",
]

# One representative argv per subcommand: proves the parser, not just the name.
CANONICAL_ARGS = {
    "integrity-check": ["--chains", "ethereum"],
    "reconcile": ["--chain", "base", "--sample-size", "5"],
    "attest": ["show", "--chain", "base"],
    "export-parquet": ["--chain", "all", "--dry-run"],
    "backfill-whales": ["--days", "7", "--dry-run"],
    "backfill-dex-prices": ["--days", "7", "--dry-run"],
    "bootstrap": ["--chain", "base", "--list"],
    "query": ["--chain", "base", "--tx", "0xabc"],
    "lake-backfill": ["--from-date", "2026-01-01", "--to-date", "2026-01-02"],
    "lake-gaps": ["--chain", "base"],
    "sqd-fill": ["--chain", "base", "--from-block", "1", "--to-block", "2"],
    "fill-solana": ["--from-date", "2026-01-01", "--to-date", "2026-01-02"],
    "fill-eth": ["--from-date", "2026-01-01", "--to-date", "2026-01-02", "--estimate"],
    "orphan-sweep": ["--chain", "base"],
}


# A distinctive phrase from each subcommand's own description.
DESCRIPTION_MARKERS = {
    "integrity-check": "Daily integrity check",
    "reconcile": "TrueBlocks-style reconciliation",
    "attest": "Trustless dataset verification",
    "export-parquet": "per-chain/per-date Parquet files",
    "backfill-whales": "Recomputes the whale flag",
    "backfill-dex-prices": "Re-scans recent dex_events rows with price_usd = 0",
    "bootstrap": "Replay a public B2 parquet snapshot",
    "query": "Answer a query about ANY block/tx/address historically",
    "lake-backfill": "sonarx",
    "lake-gaps": "Scan a lookback window for dates the lake can serve",
    "sqd-fill": "--batch-rows BATCH_ROWS",
    "fill-solana": "--force-budget",
    "fill-eth": "--max-scan-gib",
    "orphan-sweep": "Reconcile historical orphan rows",
}


def build_parser() -> argparse.ArgumentParser:
    """Return the root parser by intercepting parse_args' constructed instance."""
    captured: list[argparse.ArgumentParser] = []
    real = argparse.ArgumentParser.parse_args

    def capture(self: argparse.ArgumentParser, argv: Any = None) -> Any:
        captured.append(self)
        return real(self, argv)

    argparse.ArgumentParser.parse_args = capture  # type: ignore[method-assign]
    try:
        # No subcommand is required, so this returns cleanly and captures the root.
        assert parse_args([]).command is None
    finally:
        argparse.ArgumentParser.parse_args = real  # type: ignore[method-assign]
    return captured[0]


def test_subcommand_set_and_order_is_exactly_the_registered_fleet() -> None:
    """No subcommand added, renamed, dropped or reordered during extraction."""
    choices = build_parser()._subparsers._group_actions[0].choices
    assert list(choices) == EXPECTED_SUBCOMMANDS


@pytest.mark.parametrize("command", EXPECTED_SUBCOMMANDS)
def test_parser_accepts_canonical_argv(command: str) -> None:
    """Each subcommand parses a representative argv (no SystemExit)."""
    args = parse_args([command, *CANONICAL_ARGS[command]])
    assert args.command == command


@pytest.mark.parametrize("command", EXPECTED_SUBCOMMANDS)
def test_help_exits_zero(command: str) -> None:
    """`lite-indexer <command> --help` prints usage and exits 0.

    The usage prog is argv[0] (``-c`` under pytest), so identity is asserted on
    the description that reaches the parser's own subcommand — a swapped
    registration would print a different description.
    """
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        with pytest.raises(SystemExit) as exc:
            parse_args([command, "--help"])
    assert exc.value.code == 0
    text = buf.getvalue()
    assert "usage:" in text
    assert DESCRIPTION_MARKERS[command] in text


def test_root_help_exits_zero_and_lists_every_subcommand() -> None:
    """`lite-indexer --help` exits 0 and advertises the whole subcommand set."""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        with pytest.raises(SystemExit) as exc:
            parse_args(["--help"])
    assert exc.value.code == 0
    text = buf.getvalue()
    assert "{" + ",".join(EXPECTED_SUBCOMMANDS) + "}" in text


def test_attest_action_subcommands_parse() -> None:
    """The nested attest verify/show actions still parse their own flags."""
    # bandit B108: argv only, never opened
    assert parse_args(["attest", "verify", "/tmp/m.json"]).attest_action == "verify"  # nosec B108
    assert parse_args(["attest", "show", "--chain", "base"]).attest_action == "show"
