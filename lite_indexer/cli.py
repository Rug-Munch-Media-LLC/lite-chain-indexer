"""
CLI entry point for LiteChainIndexer.

Usage:
    lite-indexer --chain ethereum
    lite-indexer --chain ethereum --from-block 25645000
    lite-indexer --daemon --chains ethereum,base,bsc
    lite-indexer --daemon --dex --deployers --start-from-head
    lite-indexer export-parquet --chain ethereum --days 7 --upload --delete
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

from lite_indexer.chains import CHAIN_POLL_INTERVALS, effective_poll_interval, log_chain_resources
from lite_indexer.config import BATCH_SIZE, DATA_DIR, DISK_CAP_BYTES
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger, setup_logging
from lite_indexer.strategies import SUPPORTED_CHAINS

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
        "--start-from-head",
        action="store_true",
        help="Start indexing from chain tip instead of backfilling from genesis. "
        "Ignores any existing state file and begins from (tip - 100).",
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

    # ── export-parquet subcommand ────────────────────────────────────
    export_parser = sub.add_parser(
        "export-parquet",
        help="Export old ClickHouse rows to Parquet (cold storage tier)",
        description=(
            "Aggregate rows older than --days into per-chain/per-date Parquet files, "
            "optionally upload them to B2 and delete them from ClickHouse. "
            "Idempotent: dates already exported are skipped."
        ),
    )
    export_parser.add_argument(
        "--chain",
        default="ethereum",
        choices=sorted(SUPPORTED_CHAINS.keys()),
        help="Chain to export (default: ethereum)",
    )
    export_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Min row age in days before export (default: PARQUET_COLD_DAYS env or 7)",
    )
    export_parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload Parquet files to the B2 remote via rclone",
    )
    export_parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete exported rows from ClickHouse (reclaim disk)",
    )
    export_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview eligible dates without writing, uploading, or deleting",
    )
    export_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for local Parquet output + state (default: DATA_DIR)",
    )
    return parser.parse_args(argv)


async def main_daemon(
    chains: list[str],
    dex_enabled: bool = False,
    deployers_enabled: bool = False,
    disk_cap: int = DISK_CAP_BYTES,
    start_from_head: bool = False,
    data_dir: str | None = None,
) -> int:
    """Daemon mode: continuously index all configured chains in PARALLEL with
    PER-CHAIN independent polling intervals.

    Each chain gets its own LiteIndexer instance with independent state
    tracking AND its own asyncio task polling at its own optimal interval.
    This prevents fast chains (Base 2s, Mantle 2s) from being throttled
    by slow chains (Ethereum 12s, Linea 12s).

    Previously, ALL chains shared a single max_poll_interval which caused
    65min lag on Base and 20min lag on Mantle when Ethereum backlogged.
    Now each chain runs independently. Runs until SIGTERM/SIGINT.
    """
    logger.info(
        "daemon_start",
        chains=chains,
        dex=dex_enabled,
        deployers=deployers_enabled,
        start_from_head=start_from_head,
    )

    data_path = Path(data_dir) if data_dir else DATA_DIR

    shutdown_event = asyncio.Event()
    error_count = 0

    def _handle_signal(sig: int, _frame: Any = None) -> None:
        sig_name = signal.Signals(sig).name
        logger.info("daemon_signal_received", signal=sig_name)
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s))
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _handle_signal)

    async def _chain_loop(chain: str) -> None:
        """Independent per-chain polling loop with its own poll interval.

        The poll interval is priority-aware: high-value chains (Ethereum,
        Solana) poll at their base interval while lower-value chains poll
        less often, cutting wasted RPC calls. The per-chain memory budget
        and (optionally) CPU affinity are logged for observability.
        """
        nonlocal error_count
        indexer = LiteIndexer(
            chain=chain,
            disk_cap=disk_cap,
            start_from_head=start_from_head,
            data_dir=data_path,
        )
        indexer.dex_enabled = dex_enabled
        indexer.deployers_enabled = deployers_enabled

        base_interval = CHAIN_POLL_INTERVALS.get(chain, 15.0)
        poll_interval = effective_poll_interval(chain, base_interval)
        log_chain_resources(chain, poll_interval=base_interval)

        while not shutdown_event.is_set():
            loop_start = time.monotonic()
            try:
                result = await indexer.sync_loop()
                if isinstance(result, dict) and result.get("status") == "error":
                    error_count += 1
            except Exception as exc:
                error_count += 1
                logger.error("daemon_chain_failed", chain=chain, error=str(exc))

            elapsed = time.monotonic() - loop_start
            remaining = poll_interval - elapsed
            if remaining > 0:
                try:
                    # Use sleep for the remaining interval; cancelled by shutdown_event
                    await asyncio.sleep(remaining)
                except asyncio.CancelledError:
                    break

    # Launch one independent task per chain
    chain_tasks = [asyncio.create_task(_chain_loop(c), name=f"daemon-chain-{c}") for c in chains]

    # Periodic endpoint health monitor — probes every configured EVM RPC,
    # logs up/down, and feeds the adaptive pool scoring so dead endpoints
    # are deprioritized between sync cycles.
    from lite_indexer.rpc.health import monitor_endpoint_health

    health_task = asyncio.create_task(
        monitor_endpoint_health(),
        name="endpoint-health-monitor",
    )

    # Periodic free-RPC auto-discovery (chainlist.org) + usage logging.
    discovery_task = asyncio.create_task(
        _discovery_and_usage_loop(chains),
        name="rpc-discovery-monitor",
    )

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cancel the health monitor, discovery loop, and all chain tasks
    health_task.cancel()
    discovery_task.cancel()
    for task in chain_tasks:
        task.cancel()
    await asyncio.gather(health_task, discovery_task, *chain_tasks, return_exceptions=True)

    logger.info("daemon_shutdown")
    return 0 if error_count == 0 else 1


async def _discovery_and_usage_loop(chains: list[str]) -> None:
    """Periodically log RPC usage and auto-discover new free endpoints.

    Scans chainlist.org for new free RPCs for each EVM chain at
    ``RPC_DISCOVERY_INTERVAL`` (default: daily) and appends healthy
    endpoints to the in-memory pool. Also emits a periodic usage/cost
    summary so operators can see which endpoints actually serve data.

    Args:
        chains: Chains to keep discovering for.
    """
    from lite_indexer.config import RPC_DISCOVERY_INTERVAL
    from lite_indexer.rpc.discovery import auto_discover
    from lite_indexer.rpc.routing import log_rpc_usage

    interval = max(300, RPC_DISCOVERY_INTERVAL)
    evm_chains = [c for c in chains if c != "solana" and c != "tron"]

    while True:
        try:
            log_rpc_usage()
            if interval and evm_chains:
                for chain in evm_chains:
                    try:
                        await auto_discover(chain, add_to_pool=True, validate=True)
                    except Exception as exc:
                        logger.debug("rpc_discovery_failed", chain=chain, error=str(exc)[:80])
        except Exception as exc:
            logger.warning("discovery_loop_error", error=str(exc)[:100])
        await asyncio.sleep(interval)


async def async_main(args: argparse.Namespace) -> int:
    """Async entry point."""
    if args.log_level:
        setup_logging(args.log_level)

    data_path = Path(args.data_dir) if args.data_dir else None

    # ── export-parquet subcommand ────────────────────────────────────
    if args.command == "export-parquet":
        return run_export_parquet(args)

    if args.daemon:
        chains_list = [c.strip() for c in args.chains.split(",") if c.strip()]
        return await main_daemon(
            chains_list,
            dex_enabled=args.dex,
            deployers_enabled=args.deployers,
            disk_cap=args.disk_cap,
            start_from_head=args.start_from_head,
            data_dir=args.data_dir,
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


def run_export_parquet(args: argparse.Namespace) -> int:
    """Run the export-parquet cold-storage subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, days, upload, delete, dry_run).

    Returns:
        Process exit code (0 = success).
    """
    from lite_indexer.config import PARQUET_COLD_DAYS, PARQUET_DIR
    from lite_indexer.storage.parquet import ParquetExporter

    days = args.days if args.days and args.days > 0 else PARQUET_COLD_DAYS
    data_dir = Path(args.data_dir) if args.data_dir else PARQUET_DIR
    exporter = ParquetExporter(data_dir=data_dir)

    summary = exporter.export_chain(
        args.chain,
        cold_days=days,
        upload=args.upload,
        delete=args.delete,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary.to_dict(), default=str))  # noqa: T201
    return 0 if not summary.errors else 1


def main() -> None:
    """CLI entry point."""
    setup_logging()
    args = parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
