"""Daemon-mode CLI handlers: ``main_daemon`` and its background loops."""

from __future__ import annotations

import asyncio
import os
import signal
import time
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any

import httpx

from lite_indexer.chains import CHAIN_POLL_INTERVALS, effective_poll_interval, log_chain_resources
from lite_indexer.chains.parallelism import chain_priority
from lite_indexer.config import (
    CHAIN_AUTO_DISABLE_ENABLED,
    CHAIN_DISABLED_RECHECK_SECONDS,
    CHAIN_STALL_DISABLE_SECONDS,
    DATA_DIR,
    DISK_CAP_BYTES,
    MAX_CONCURRENT_CHAINS,
    MEMORY_PAUSE_SLEEP,
    MEMORY_RECYCLE_MB,
    PRICING_ENABLED,
)
from lite_indexer.health import ChainStallTracker
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger
from lite_indexer.storage.clickhouse import ClickHouseWriter

logger = get_logger(__name__)


def memory_recycle_needed(mem_mb: int, threshold_mb: int = MEMORY_RECYCLE_MB) -> bool:
    """Whether guarded memory has reached the hard self-recycle ceiling.

    A ``threshold_mb`` of 0 disables recycling (unbounded mode).
    """
    return threshold_mb > 0 and mem_mb >= threshold_mb


async def recycle_if_over_budget(
    mem_mb: int,
    chain: str,
    indexers: Sequence[LiteIndexer] = (),
) -> None:
    """Log and exit the process when guarded memory hits the ceiling.

    Called AFTER a sync cycle completes (state is saved each cycle by
    ``sync_loop``), so no in-flight work is lost. The clients are closed
    best-effort before ``os._exit(0)``; systemd ``Restart=always`` then
    restarts the daemon with a fresh heap, capping max RSS at roughly
    ``MEMORY_RECYCLE_MB``. A no-op below the threshold.
    """
    if not memory_recycle_needed(mem_mb):
        return
    logger.warning("memory_recycle", chain=chain, rss_mb=mem_mb, recycle_mb=MEMORY_RECYCLE_MB)
    await asyncio.gather(*(ix.aclose() for ix in indexers), return_exceptions=True)
    logger.info("memory_recycle_exit", rss_mb=mem_mb, recycle_mb=MEMORY_RECYCLE_MB)
    os._exit(0)


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

    # Live indexers, kept so shutdown can close their HTTP clients.
    indexers: list[LiteIndexer] = []

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

        After each completed cycle (state already persisted by
        ``sync_loop``) the loop checks guarded memory against the hard
        ``MEMORY_RECYCLE_MB`` ceiling; at or above it the process exits
        cleanly for systemd to restart with a fresh heap.
        """
        nonlocal error_count
        indexer = LiteIndexer(
            chain=chain,
            disk_cap=disk_cap,
            start_from_head=start_from_head,
            data_dir=data_path,
        )
        indexers.append(indexer)
        indexer.dex_enabled = dex_enabled
        indexer.deployers_enabled = deployers_enabled
        indexer.traces_enabled = traces_enabled

        base_interval = CHAIN_POLL_INTERVALS.get(chain, 15.0)
        poll_interval = effective_poll_interval(chain, base_interval)
        log_chain_resources(chain, poll_interval=base_interval)

        # Chain-level health: track when the state last advanced. A chain
        # whose state has not moved for CHAIN_STALL_DISABLE_SECONDS (2h)
        # is auto-disabled and re-probed slowly, so one dead chain never
        # burns RPC budget or holds concurrency slots while the others
        # keep indexing.
        stall_tracker = ChainStallTracker(stall_seconds=CHAIN_STALL_DISABLE_SECONDS)

        while not shutdown_event.is_set():
            loop_start = time.monotonic()
            try:
                # Chain health: skip sync attempts for a disabled chain,
                # but re-probe it at a slow interval so it re-enables as
                # soon as its RPC pool recovers.
                if stall_tracker.disabled:
                    await asyncio.sleep(CHAIN_DISABLED_RECHECK_SECONDS)

                state_before = indexer.state.load().get("last_block")
                # In-process memory watchdog: when RSS exceeds
                # MEMORY_HIGH_MB, pause NEW chain ingestion until memory
                # drops instead of piling another sync cycle onto the
                # cgroup pressure (which caused the 08/03 OOM kills).
                # Per-chain periodic GC is handled inside
                # LiteIndexer.sync_loop (GC_EVERY_N_CYCLES).
                if not indexer.memory_gate_ok():
                    logger.warning(
                        "daemon_memory_paused",
                        chain=chain,
                        pause_s=MEMORY_PAUSE_SLEEP,
                    )
                    await asyncio.sleep(MEMORY_PAUSE_SLEEP)
                    continue
                if concurrency_slot is not None:
                    async with concurrency_slot:
                        result = await indexer.sync_loop()
                else:
                    result = await indexer.sync_loop()
                if isinstance(result, dict) and result.get("status") == "error":
                    error_count += 1

                # Chain health bookkeeping: did the state move?
                state_after = indexer.state.load().get("last_block")
                if CHAIN_AUTO_DISABLE_ENABLED:
                    became_disabled = stall_tracker.observe(state_before, state_after)
                    if became_disabled:
                        logger.warning(
                            "chain_disabled_stalled",
                            chain=chain,
                            stalled_s=int(stall_tracker.stalled_seconds),
                            last_block=state_before,
                        )
                    elif stall_tracker.disabled:
                        logger.info("chain_reenabled", chain=chain, block=state_after)

                # Hard memory ceiling: AFTER this cycle's state has been
                # persisted by sync_loop, recycle the process if guarded
                # memory has reached MEMORY_RECYCLE_MB. systemd
                # Restart=always gives it a fresh heap, capping max RSS.
                # Never reached mid-cycle.
                await recycle_if_over_budget(indexer.guarded_memory_mb(), chain, indexers)
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

    # Close every per-chain HTTP client so a clean shutdown (or restart)
    # does not leak sockets to the next process.
    await asyncio.gather(*(ix.aclose() for ix in indexers), return_exceptions=True)

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
