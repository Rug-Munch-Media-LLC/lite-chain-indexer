"""
CLI entry point for LiteChainIndexer.

Usage:
    lite-indexer --chain ethereum
    lite-indexer --chain ethereum --from-block 25645000
    lite-indexer bootstrap --all

The subcommand handlers live in focused sibling modules (``_daemon``,
``_export``, ``_integrity``, ``_backfill``, ``_bootstrap``, ``_query``)
and each subcommand family's argparse surface in a ``_*_parsers`` sibling
(``add_<family>_parsers(sub)``). This module keeps the entrypoints
(``main`` / ``async_main``) and the argparse surface identical to the
pre-split ``lite_indexer.cli`` module.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from lite_indexer.cli._attest import run_attest_cli
from lite_indexer.cli._attest_parsers import add_attest_parsers
from lite_indexer.cli._backfill import run_backfill_dex_prices, run_backfill_whales
from lite_indexer.cli._backfill_parsers import add_backfill_parsers
from lite_indexer.cli._bootstrap import run_bootstrap_cli
from lite_indexer.cli._bootstrap_parsers import add_bootstrap_parsers
from lite_indexer.cli._daemon import main_daemon
from lite_indexer.cli._export import run_export_parquet
from lite_indexer.cli._export_parsers import add_export_parsers
from lite_indexer.cli._integrity import run_integrity_check_cli, run_reconcile_cli, run_recover_cli
from lite_indexer.cli._integrity_parsers import add_integrity_parsers
from lite_indexer.cli._lake import add_lake_parsers, run_lake_command
from lite_indexer.cli._query import run_query_cli
from lite_indexer.cli._query_parsers import add_query_parsers
from lite_indexer.config import (
    BATCH_SIZE,
    DATA_DIR,
    DISK_CAP_BYTES,
    MAX_CONCURRENT_CHAINS,
    PRICING_ENABLED,
)
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger, setup_logging
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.strategies import SUPPORTED_CHAINS

__all__ = [
    "main",
    "async_main",
    "main_daemon",
    "parse_args",
    "run_attest_cli",
    "run_backfill_dex_prices",
    "run_backfill_whales",
    "run_bootstrap_cli",
    "run_export_parquet",
    "run_integrity_check_cli",
    "run_lake_command",
    "run_query_cli",
    "run_reconcile_cli",
    "run_recover_cli",
]

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="LiteChainIndexer — lightweight multi-chain blockchain indexer using free RPCs",
    )
    parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to index (default: ethereum). All chains: " + ", ".join(sorted(SUPPORTED_CHAINS.keys())),
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="Override starting block (testing / backfill)",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="Override ending block (testing / backfill)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Max blocks per batch (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--disk-cap",
        type=int,
        default=DISK_CAP_BYTES,
        help=f"Disk cap in bytes (default: {DISK_CAP_BYTES} = 10 GB)",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run in daemon mode — continuously index all chains",
    )
    parser.add_argument(
        "--chains",
        type=str,
        default="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,linea,scroll,zksync,mantle,gnosis,solana,tron",
        help="Comma-separated list of chains to index in daemon mode",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help=f"Max chains syncing concurrently in daemon mode (default: {MAX_CONCURRENT_CHAINS}). "
        "Caps peak memory — 14 parallel chains is what caused the 08/03 OOM kills.",
    )
    parser.add_argument(
        "--dex",
        action="store_true",
        help="Enable DEX swap event indexing (V2 + V3)",
    )
    parser.add_argument(
        "--deployers",
        action="store_true",
        help="Enable deployer tracking via factory PairCreated/PoolCreated events",
    )
    parser.add_argument(
        "--traces",
        action="store_true",
        help="Enable internal-transfer trace collection (WETH unwraps, bridge deposits, "
        "fee flows) via free trace_* RPCs. Off by default — trace endpoints are "
        "rate-limited on free tiers. Also settable via TRACE_ENABLED=true.",
    )
    parser.add_argument(
        "--start-from-head",
        action="store_true",
        help="Start indexing from chain tip instead of backfilling from genesis. "
        "Ignores any existing state file and begins from (tip - 100).",
    )
    parser.add_argument(
        "--recover",
        action="store_true",
        help="Recover state files from ClickHouse: query the actual max block per "
        "chain, reset the state file + high-water mark to it, and (with "
        "--backfill) rewind to the earliest missing range so the next sync "
        "re-fetches it. Fixes the 08/03+08/04 state-regression class of bugs "
        "even when the in-process guard never ran.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="With --recover: also rewind state to the earliest missing range "
        "detected in ClickHouse so the next sync backfills the gap. "
        "Without --recover this is a no-op.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help=f"Data directory for NDJSON + state (default: {DATA_DIR})",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO from LOG_LEVEL env var)",
    )
    sub = parser.add_subparsers(dest="command")

    # Registration order is load-bearing: it fixes the subparser order that
    # `--help` prints. Keep it matching the pre-extraction sequence.
    add_integrity_parsers(sub)
    add_attest_parsers(sub)
    add_export_parsers(sub)
    add_backfill_parsers(sub)
    add_bootstrap_parsers(sub)
    add_query_parsers(sub)
    add_lake_parsers(sub)
    return parser.parse_args(argv)


async def async_main(args: argparse.Namespace) -> int:
    """Async entry point."""
    if args.log_level:
        setup_logging(args.log_level)

    data_path = Path(args.data_dir) if args.data_dir else None

    # ── export-parquet subcommand ────────────────────────────────────
    if args.command == "export-parquet":
        return run_export_parquet(args)

    if args.command and args.command.startswith(("lake-", "sqd-", "fill-", "orphan-")):
        return run_lake_command(args)  # lake/sqd/fill + orphan-sweep

    # ── integrity-check subcommand ───────────────────────────────────
    if args.command == "integrity-check":
        return run_integrity_check_cli(args)

    # ── recover mode ─────────────────────────────────────────────────
    # Recover state files from the ClickHouse durable store BEFORE any
    # indexing. Works in single-chain mode (--chain) and daemon mode
    # (--chains). With --backfill the state is additionally rewound to
    # the earliest missing range so the next sync re-fetches it.
    if args.recover:
        chains_list = [c.strip() for c in args.chains.split(",") if c.strip()]
        if args.chain and args.chain != "ethereum":
            chains_list = [args.chain]
        return await run_recover_cli(chains_list, args.data_dir, args.backfill)

    # ── backfill-whales subcommand ───────────────────────────────────
    if args.command == "backfill-whales":
        return run_backfill_whales(args)

    # ── backfill-dex-prices subcommand ───────────────────────────────
    if args.command == "backfill-dex-prices":
        return run_backfill_dex_prices(args)

    # ── bootstrap subcommand ─────────────────────────────────────────
    if args.command == "bootstrap":
        return await run_bootstrap_cli(args)

    # ── query subcommand (on-demand historical RPC) ──────────────────
    if args.command == "query":
        return await run_query_cli(args)

    # ── reconcile subcommand (TrueBlocks-style) ──────────────────────
    if args.command == "reconcile":
        return await run_reconcile_cli(args)

    # ── attest subcommand (trustless dataset verification) ───────────
    if args.command == "attest":
        return run_attest_cli(args)

    if args.daemon:
        chains_list = [c.strip() for c in args.chains.split(",") if c.strip()]
        return await main_daemon(
            chains_list,
            dex_enabled=args.dex,
            deployers_enabled=args.deployers,
            traces_enabled=args.traces,
            disk_cap=args.disk_cap,
            start_from_head=args.start_from_head,
            data_dir=args.data_dir,
            max_concurrent=args.max_concurrent or MAX_CONCURRENT_CHAINS,
        )

    # Single-chain mode
    indexer = LiteIndexer(
        chain=args.chain,
        from_block=args.from_block,
        to_block=args.to_block,
        batch_size=args.batch_size,
        disk_cap=args.disk_cap,
        start_from_head=args.start_from_head,
        data_dir=data_path if data_path else None,
    )
    indexer.dex_enabled = args.dex
    indexer.deployers_enabled = args.deployers
    indexer.traces_enabled = args.traces

    # Startup USD price refresh (best-effort, non-blocking): populate the
    # token_prices feed + cache so the whale flag and USD columns on the
    # first batch are computed with prices instead of raw-amount fallbacks.
    if PRICING_ENABLED:
        try:
            from lite_indexer.pricing import refresh_prices

            startup_client = ClickHouseWriter().client()
            if startup_client is not None:
                stats = await refresh_prices(startup_client, chains=[args.chain])
                logger.info("prices_refreshed_startup", stats=stats)
        except Exception as exc:  # noqa: BLE001 — refresh is best-effort
            logger.warning("price_refresh_startup_failed", error=str(exc)[:100])

    try:
        summary = await indexer.sync_loop()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("sync_interrupted", chain=args.chain)
        return 1
    except Exception as exc:
        logger.error("sync_fatal", chain=args.chain, error=str(exc), exc_info=True)
        print(json.dumps({"status": "error", "error": str(exc)}))  # noqa: T201
        return 1

    print(json.dumps(summary, default=str))  # noqa: T201
    return 0 if summary["status"] in ("ok", "no_events", "caught_up") else 1


def main() -> None:
    """CLI entry point.

    Installs the optional uvloop event-loop policy first (graceful fallback
    to the default asyncio loop when uvloop is absent/unsupported), then
    runs the async entrypoint.
    """
    from lite_indexer.runtime import install_event_loop_policy

    setup_logging()
    install_event_loop_policy()
    args = parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
