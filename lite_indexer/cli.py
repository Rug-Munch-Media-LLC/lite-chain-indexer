"""
CLI entry point for LiteChainIndexer.

Usage:
    lite-indexer --chain ethereum
    lite-indexer --chain ethereum --from-block 25645000
    lite-indexer --daemon --chains ethereum,base,bsc
    lite-indexer --daemon --dex --deployers --start-from-head
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

from lite_indexer.chains import CHAIN_POLL_INTERVALS
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
        """Independent per-chain polling loop with its own poll interval."""
        nonlocal error_count
        indexer = LiteIndexer(
            chain=chain,
            disk_cap=disk_cap,
            start_from_head=start_from_head,
            data_dir=data_path,
        )
        indexer.dex_enabled = dex_enabled
        indexer.deployers_enabled = deployers_enabled

        poll_interval = CHAIN_POLL_INTERVALS.get(chain, 15.0)

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

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cancel the health monitor and all chain tasks
    health_task.cancel()
    for task in chain_tasks:
        task.cancel()
    await asyncio.gather(health_task, *chain_tasks, return_exceptions=True)

    logger.info("daemon_shutdown")
    return 0 if error_count == 0 else 1


async def async_main(args: argparse.Namespace) -> int:
    """Async entry point."""
    if args.log_level:
        setup_logging(args.log_level)

    data_path = Path(args.data_dir) if args.data_dir else None

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


def main() -> None:
    """CLI entry point."""
    setup_logging()
    args = parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
