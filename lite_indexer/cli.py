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
import contextlib
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

from lite_indexer.chains import CHAIN_POLL_INTERVALS, GET_LOGS_ENDPOINTS
from lite_indexer.config import BATCH_SIZE, DATA_DIR, DISK_CAP_BYTES
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger, setup_logging

logger = get_logger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="LiteChainIndexer — lightweight multi-chain blockchain indexer using free RPCs",
    )
    parser.add_argument(
        "--chain",
        default="ethereum",
        choices=list(GET_LOGS_ENDPOINTS.keys()),
        help="Chain to index (default: ethereum)",
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
        default="ethereum,base,bsc,polygon,arbitrum,optimism,avalanche,linea,scroll,zksync,mantle,gnosis",
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
    """Daemon mode: continuously index all configured chains in parallel.

    Each chain gets its own LiteIndexer instance with independent state
    tracking and polling interval. Runs until SIGTERM/SIGINT.
    """
    _time = time  # local ref to avoid shadowing

    logger.info(
        "daemon_start",
        chains=chains,
        dex=dex_enabled,
        deployers=deployers_enabled,
        start_from_head=start_from_head,
    )

    data_path = Path(data_dir) if data_dir else DATA_DIR

    indexers = {}
    for chain in chains:
        indexer = LiteIndexer(
            chain=chain,
            disk_cap=disk_cap,
            start_from_head=start_from_head,
            data_dir=data_path,
        )
        indexer.dex_enabled = dex_enabled
        indexer.deployers_enabled = deployers_enabled
        indexers[chain] = indexer

    running = True
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: int, _frame: Any = None) -> None:
        nonlocal running
        sig_name = signal.Signals(sig).name
        logger.info("daemon_signal_received", signal=sig_name)
        running = False
        shutdown_event.set()

    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _handle_signal(s))
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _handle_signal)

    max_poll_interval = max(CHAIN_POLL_INTERVALS.get(c, 15) for c in chains) if chains else 15

    while running:
        cycle_start = _time.monotonic()

        async def _sync_chain(chain: str) -> dict[str, Any]:
            idx = indexers[chain]
            try:
                return await idx.sync_loop()
            except Exception as exc:
                logger.error("daemon_chain_failed", chain=chain, error=str(exc))
                return {"chain": chain, "status": "error", "error": str(exc)}

        tasks = {}
        for c in chains:
            tasks[c] = asyncio.create_task(_sync_chain(c), name=f"sync-{c}")

        results = []
        for c in chains:
            try:
                result = await tasks[c]
                results.append(result)
            except Exception as exc:
                results.append({"chain": c, "status": "error", "error": str(exc)})

        ok_count = sum(
            1 for r in results if isinstance(r, dict) and r.get("status") in ("ok", "no_events", "caught_up")
        )
        error_count = sum(
            1 for r in results
            if isinstance(r, Exception) or (isinstance(r, dict) and r.get("status") == "error")
        )

        elapsed = _time.monotonic() - cycle_start
        logger.info(
            "daemon_cycle_complete",
            chains=len(chains),
            ok=ok_count,
            errors=error_count,
            cycle_s=round(elapsed, 2),
        )

        remaining = max_poll_interval - elapsed
        if remaining > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shutdown_event.wait(), timeout=remaining)

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
