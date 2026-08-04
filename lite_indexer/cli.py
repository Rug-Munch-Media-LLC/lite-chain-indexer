"""
CLI entry point for LiteChainIndexer.

Usage:
    lite-indexer --chain ethereum
    lite-indexer --chain ethereum --from-block 25645000
    lite-indexer --daemon --chains ethereum,base,bsc
    lite-indexer --daemon --dex --deployers --start-from-head
    lite-indexer export-parquet --chain ethereum --date 2026-07-30 --upload --delete-after-upload
    lite-indexer export-parquet --chain all --date 2026-07-30 --upload --delete-after-upload
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import httpx

from lite_indexer.chains import CHAIN_POLL_INTERVALS, effective_poll_interval, log_chain_resources
from lite_indexer.chains.parallelism import chain_priority
from lite_indexer.config import (
    BATCH_SIZE,
    DATA_DIR,
    DISK_CAP_BYTES,
    GC_EVERY_N_CYCLES,
    MAX_CONCURRENT_CHAINS,
    PRICING_ENABLED,
    RECONCILIATION_SAMPLE_SIZE,
)
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger, setup_logging
from lite_indexer.storage.clickhouse import ClickHouseWriter
from lite_indexer.storage.reconciliation import run_reconciliation
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

    # ── export-parquet subcommand ────────────────────────────────────
    export_parser = sub.add_parser(
        "export-parquet",
        help="Export ClickHouse rows to Parquet (cold storage tier)",
        description=(
            "Export a (chain, date) into per-chain/per-date Parquet files, "
            "optionally upload them to the B2 permanent tier and delete them "
            "from ClickHouse only after the upload is verified. "
            "Idempotent: dates already exported are skipped."
        ),
    )
    export_parser.add_argument(
        "--chain",
        default="all",
        choices=sorted(SUPPORTED_CHAINS.keys()) + ["all"],
        help="Chain to export or 'all' (default: all)",
    )
    export_parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Date to export as YYYY-MM-DD (default: yesterday)",
    )
    export_parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Backfill mode: min row age in days before export (default: PARQUET_COLD_DAYS env or 7). "
        "Mutually exclusive with --date.",
    )
    export_parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload Parquet files to the B2 remote via rclone",
    )
    export_parser.add_argument(
        "--delete-after-upload",
        dest="delete_after_upload",
        action="store_true",
        help="Delete exported rows from ClickHouse only after the upload is verified",
    )
    export_parser.add_argument(
        "--delete",
        dest="delete_after_upload",
        action="store_true",
        help=argparse.SUPPRESS,  # legacy alias for --delete-after-upload
    )
    export_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the export without writing, uploading, or deleting",
    )
    export_parser.add_argument(
        "--remote",
        type=str,
        default=None,
        help="rclone remote root (default: PARQUET_REMOTE env or b2-crypt:chain-data/parquet)",
    )
    export_parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Data directory for local Parquet output + state (default: DATA_DIR)",
    )
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
    return parser.parse_args(argv)


async def main_daemon(
    chains: list[str],
    dex_enabled: bool = False,
    deployers_enabled: bool = False,
    traces_enabled: bool = False,
    disk_cap: int = DISK_CAP_BYTES,
    start_from_head: bool = False,
    data_dir: str | None = None,
    max_concurrent: int = MAX_CONCURRENT_CHAINS,
) -> int:
    """Daemon mode: continuously index all configured chains in PARALLEL with
    PER-CHAIN independent polling intervals.

    Each chain gets its own LiteIndexer instance with independent state
    tracking AND its own asyncio task polling at its own optimal interval.
    This prevents fast chains (Base 2s, Mantle 2s) from being throttled
    by slow chains (Ethereum 12s, Linea 12s).

    Concurrency is capped at ``max_concurrent`` chains syncing at once
    (default 6, see ``MAX_CONCURRENT_CHAINS``): every chain task shares
    one semaphore, so at most that many ``sync_loop`` calls run in
    parallel. High-priority chains are scheduled first (tasks are created
    in descending priority order); lower-priority chains rotate in as
    slots free. This bounds peak RSS — 14 chains running simultaneously
    caused repeated cgroup OOM kills on 2026-08-03.

    Previously, ALL chains shared a single max_poll_interval which caused
    65min lag on Base and 20min lag on Mantle when Ethereum backlogged.
    Now each chain runs independently. Runs until SIGTERM/SIGINT.
    """
    logger.info(
        "daemon_start",
        chains=chains,
        dex=dex_enabled,
        deployers=deployers_enabled,
        traces=traces_enabled,
        start_from_head=start_from_head,
        max_concurrent=max_concurrent,
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
            loop.add_signal_handler(sig, partial(_handle_signal, sig))
        except (NotImplementedError, RuntimeError):
            signal.signal(sig, _handle_signal)

    # Shared concurrency cap: at most `max_concurrent` chains sync at once.
    concurrency_slot = asyncio.Semaphore(max_concurrent) if max_concurrent and max_concurrent > 0 else None

    async def _chain_loop(chain: str) -> None:
        """Independent per-chain polling loop with its own poll interval.

        The poll interval is priority-aware: high-value chains (Ethereum,
        Solana) poll at their base interval while lower-value chains poll
        less often, cutting wasted RPC calls. The per-chain memory budget
        and (optionally) CPU affinity are logged for observability.

        The shared semaphore is acquired around each sync cycle, so at
        most ``max_concurrent`` chains hold event batches in memory at
        once. When the semaphore is contended, lower-priority chains wait
        for a free slot instead of piling up parallel fetches — this is
        what prevents the cgroup OOM kills.
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
        indexer.traces_enabled = traces_enabled

        base_interval = CHAIN_POLL_INTERVALS.get(chain, 15.0)
        poll_interval = effective_poll_interval(chain, base_interval)
        log_chain_resources(chain, poll_interval=base_interval)

        cycles_since_gc = 0
        while not shutdown_event.is_set():
            loop_start = time.monotonic()
            try:
                if concurrency_slot is not None:
                    async with concurrency_slot:
                        result = await indexer.sync_loop()
                else:
                    result = await indexer.sync_loop()
                if isinstance(result, dict) and result.get("status") == "error":
                    error_count += 1

                # Periodic GC: release reference cycles (event dicts, bloom
                # filters, httpx responses) that cyclic GC misses under
                # cgroup memory pressure.
                cycles_since_gc += 1
                if cycles_since_gc >= GC_EVERY_N_CYCLES:
                    cycles_since_gc = 0
                    import gc

                    collected = gc.collect()
                    logger.debug("gc_collected", chain=chain, objects=collected)
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

    # Launch one independent task per chain, high-priority chains first so
    # they claim concurrency slots before low-priority chains.
    ordered_chains = sorted(chains, key=lambda c: chain_priority(c), reverse=True)
    chain_tasks = [asyncio.create_task(_chain_loop(c), name=f"daemon-chain-{c}") for c in ordered_chains]

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

    # Periodic USD price refresh (CoinGecko/DefiLlama free tiers): keeps the
    # token_prices feed + in-memory cache current so the whale flag and USD
    # columns on DEX rows are computed with prices. Best-effort — a failed
    # refresh keeps the previous cached prices.
    price_task: asyncio.Task[Any] | None = None
    if PRICING_ENABLED:
        price_task = asyncio.create_task(
            _price_refresh_loop(chains),
            name="price-refresh",
        )

    # Wait for shutdown signal
    await shutdown_event.wait()

    # Cancel the health monitor, discovery loop, price refresh, and all
    # chain tasks
    health_task.cancel()
    discovery_task.cancel()
    if price_task is not None:
        price_task.cancel()
    for task in chain_tasks:
        task.cancel()
    await asyncio.gather(health_task, discovery_task, *chain_tasks, return_exceptions=True)
    if price_task is not None:
        await asyncio.gather(price_task, return_exceptions=True)

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
                    except (
                        httpx.HTTPError,
                        ConnectionError,
                        OSError,
                        TimeoutError,
                        ValueError,
                        KeyError,
                        TypeError,
                    ) as exc:
                        logger.warning("rpc_discovery_failed", chain=chain, error=str(exc)[:80])
        except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            logger.warning("discovery_loop_error", error=str(exc)[:100])
        await asyncio.sleep(interval)


async def _price_refresh_loop(chains: list[str]) -> None:
    """Periodically refresh USD prices for tracked DEX tokens.

    Refreshes the ``token_prices`` feed (CoinGecko + DefiLlama free tiers)
    on startup and every ``PRICING_REFRESH_INTERVAL`` seconds (min 5 min).
    The in-memory price cache feeds the whale flag + USD columns written
    on DEX rows. Best-effort: provider failures keep the previous cache.

    Args:
        chains: Chains to refresh prices for (tracked-token scan filter).
    """
    from lite_indexer.config import PRICING_REFRESH_INTERVAL
    from lite_indexer.pricing import refresh_prices

    interval = max(300, PRICING_REFRESH_INTERVAL)
    while True:
        try:
            writer = ClickHouseWriter()
            ch_client = writer.client()
            if ch_client is not None:
                stats = await refresh_prices(ch_client, chains=chains)
                logger.info("prices_refreshed", stats=stats)
            else:
                logger.warning("price_refresh_skipped_no_ch")
        except Exception as exc:  # noqa: BLE001 — refresh is best-effort
            logger.warning("price_refresh_failed", error=str(exc)[:100])
        await asyncio.sleep(interval)


async def async_main(args: argparse.Namespace) -> int:
    """Async entry point."""
    if args.log_level:
        setup_logging(args.log_level)

    data_path = Path(args.data_dir) if args.data_dir else None

    # ── export-parquet subcommand ────────────────────────────────────
    if args.command == "export-parquet":
        return run_export_parquet(args)

    # ── backfill-whales subcommand ───────────────────────────────────
    if args.command == "backfill-whales":
        return run_backfill_whales(args)

    # ── reconcile subcommand (TrueBlocks-style) ──────────────────────
    if args.command == "reconcile":
        from lite_indexer.rpc import RpcClient

        writer = ClickHouseWriter()
        ch_client = writer.client()
        if ch_client is None:
            print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
            return 1
        results = await run_reconciliation(
            args.chain,
            RpcClient(),
            ch_client,
            limit=args.sample_size,
        )
        status = "ok"
        if results and any(i["status"] == "MISMATCH" for i in results.values()):
            status = "mismatches_found"
        print(json.dumps({"status": status, "checked": len(results), "results": results}, default=str))  # noqa: T201
        return 0 if status == "ok" else 2

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


def run_export_parquet(args: argparse.Namespace) -> int:
    """Run the export-parquet cold-storage subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, date, days, upload,
            delete_after_upload, dry_run, remote, data_dir).

    Returns:
        Process exit code (0 = success, 2 = usage error).
    """
    from lite_indexer.config import PARQUET_COLD_DAYS, PARQUET_DIR, PARQUET_REMOTE
    from lite_indexer.storage.parquet import ParquetExporter
    from lite_indexer.strategies import SUPPORTED_CHAINS

    if args.delete_after_upload and not args.upload:
        print("error: --delete-after-upload requires --upload", file=sys.stderr)  # noqa: T201
        return 2
    if args.date and args.days:
        print("error: --date and --days are mutually exclusive", file=sys.stderr)  # noqa: T201
        return 2

    day: date | None = None
    if args.date:
        try:
            day = date.fromisoformat(args.date)
        except ValueError:
            print(f"error: invalid --date {args.date!r}; expected YYYY-MM-DD", file=sys.stderr)  # noqa: T201
            return 2
    elif not args.days:
        # Default: yesterday.
        day = datetime.now(UTC).date() - timedelta(days=1)

    data_dir = Path(args.data_dir) if args.data_dir else PARQUET_DIR
    remote = args.remote or PARQUET_REMOTE
    exporter = ParquetExporter(data_dir=data_dir, remote=remote)

    chains = sorted(SUPPORTED_CHAINS.keys()) if args.chain == "all" else [args.chain]

    summaries: list[dict[str, Any]] = []
    failed = False
    for chain in chains:
        summary = exporter.export_chain(
            chain,
            dates=[day] if day is not None else None,
            cold_days=args.days if args.days and args.days > 0 else PARQUET_COLD_DAYS,
            upload=args.upload,
            delete=args.delete_after_upload,
            dry_run=args.dry_run,
        )
        summaries.append(summary.to_dict())
        if summary.errors:
            failed = True

    out: dict[str, Any] | list[dict[str, Any]] = summaries[0] if len(summaries) == 1 else {"chains": summaries}
    print(json.dumps(out, default=str))  # noqa: T201
    return 1 if failed else 0


def run_backfill_whales(args: argparse.Namespace) -> int:
    """Run the backfill-whales subcommand (synchronous).

    Args:
        args: Parsed CLI args (chain, days, limit, dry_run).

    Returns:
        Process exit code (0 = success, 1 = ClickHouse unavailable).
    """
    from lite_indexer.pricing import backfill_whales

    writer = ClickHouseWriter()
    ch_client = writer.client()
    if ch_client is None:
        print(json.dumps({"status": "error", "error": "clickhouse_unavailable"}))  # noqa: T201
        return 1

    result = backfill_whales(
        ch_client,
        days=args.days,
        chain=args.chain,
        limit=args.limit,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, default=str))  # noqa: T201
    return 0


def main() -> None:
    """CLI entry point."""
    setup_logging()
    args = parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
