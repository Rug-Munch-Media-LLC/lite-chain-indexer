"""Daemon-mode CLI handlers: ``main_daemon`` and its background loops."""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time
from collections.abc import Sequence
from functools import partial
from pathlib import Path
from typing import Any

import httpx

from lite_indexer import metrics
from lite_indexer.chains import CHAIN_POLL_INTERVALS, effective_poll_interval, log_chain_resources
from lite_indexer.chains.parallelism import chain_priority
from lite_indexer.config import (
    CHAIN_AUTO_DISABLE_ENABLED,
    CHAIN_DISABLED_RECHECK_SECONDS,
    CHAIN_STALL_DISABLE_SECONDS,
    DATA_DIR,
    DISK_CAP_BYTES,
    MAX_CONCURRENT_CHAINS,
    MEMORY_HIGH_MB,
    MEMORY_PAUSE_SLEEP,
    MEMORY_RECYCLE_MB,
    MEMORY_WATCHDOG_ENABLED,
    MEMORY_WATCHDOG_INTERVAL_S,
    METRICS_GAUGE_INTERVAL_SECONDS,
    PRICING_ENABLED,
)
from lite_indexer.health import ChainStallTracker
from lite_indexer.indexer import LiteIndexer
from lite_indexer.indexer._memory_cap import cgroup_memory_max_mb, resolve_recycle_mb
from lite_indexer.indexer._resource_guards import get_guarded_memory_mb
from lite_indexer.logging import get_logger
from lite_indexer.metrics_server import start_metrics_server, stop_metrics_server
from lite_indexer.storage.clickhouse import ClickHouseWriter

logger = get_logger(__name__)


def memory_recycle_needed(mem_mb: int, threshold_mb: int | None = None) -> bool:
    """Whether guarded memory reached the hard self-recycle ceiling.

    ``threshold_mb=None`` resolves the effective ceiling via
    :func:`resolve_recycle_mb` (declared MEMORY_RECYCLE_MB clamped below
    the cgroup memory.max hard cap). An explicit ``threshold_mb`` still
    wins for callers/tests; 0 disables recycling (unbounded mode).
    """
    threshold = resolve_recycle_mb() if threshold_mb is None else threshold_mb
    return threshold > 0 and mem_mb >= threshold


async def recycle_if_over_budget(
    mem_mb: int,
    chain: str,
    indexers: Sequence[LiteIndexer] = (),
) -> None:
    """Log and exit the process when guarded memory hits the ceiling.

    Called AFTER a sync cycle completes (state already saved), so no
    in-flight work is lost; systemd ``Restart=always`` then restarts the
    daemon with a fresh heap capped at roughly the effective ceiling.
    """
    threshold = resolve_recycle_mb()
    if not memory_recycle_needed(mem_mb, threshold):
        return
    logger.warning("memory_recycle", chain=chain, rss_mb=mem_mb, recycle_mb=threshold)
    await asyncio.gather(*(ix.aclose() for ix in indexers), return_exceptions=True)
    logger.info("memory_recycle_exit", rss_mb=mem_mb, recycle_mb=threshold)
    os._exit(0)


# 2026-09-21: chains exempt from the shared MAX_CONCURRENT_CHAINS
# semaphore (dedicated lane). Solana measured ~14 slots/min effective
# because it queued FIFO behind multi-minute EVM cycles; the chain
# produces ~150 slots/min. Memory gating is unchanged for these chains.
_SEMAPHORE_EXEMPT_CHAINS: frozenset[str] = frozenset({"solana"})


def _watchdog_sleep(seconds: int) -> None:
    """Sleep helper for the watchdog loop (patch point for tests)."""
    time.sleep(seconds)


async def _aclose_all(indexers: Sequence[LiteIndexer]) -> None:
    """Close every indexer client, ignoring individual failures."""
    await asyncio.gather(*(ix.aclose() for ix in indexers), return_exceptions=True)


def _close_indexers_best_effort(indexers: Sequence[LiteIndexer]) -> None:
    """Close indexer clients from the watchdog thread, best-effort.

    Runs its own short-lived event loop (never touches the daemon's main
    loop); any failure is swallowed — ``os._exit`` follows regardless.
    """
    if not indexers:
        return
    try:
        asyncio.run(_aclose_all(indexers))
    except Exception as exc:  # noqa: BLE001 — watchdog must never raise
        logger.warning("memory_watchdog_close_failed", error=str(exc)[:100])


def start_memory_watchdog(indexers: Sequence[LiteIndexer]) -> threading.Thread | None:
    """Start the continuous memory watchdog thread; returns it (or None).

    2026-09-14 wedge post-mortem: the post-cycle recycle check only runs
    AFTER a sync cycle completes, so a mid-cycle memory burst wedged the
    task in D-state for 5h45m and the check never ran. This thread is
    independent of the asyncio sync loop: every
    :data:`MEMORY_WATCHDOG_INTERVAL_S` it reads guarded memory and, at or
    above the effective recycle ceiling (:func:`resolve_recycle_mb`,
    2026-09-19 OOM post-mortem), logs ``memory_recycle_watchdog``, closes
    clients best-effort and ``os._exit(0)``s (idempotent via an event) so
    systemd ``Restart=always`` restarts with a fresh heap. At
    :data:`MEMORY_HIGH_MB` it logs ``memory_high_watchdog`` but does NOT
    exit.

    Disabled (returns None) when ``MEMORY_WATCHDOG_ENABLED=0`` or
    ``MEMORY_RECYCLE_MB <= 0``.
    """
    if not MEMORY_WATCHDOG_ENABLED or MEMORY_RECYCLE_MB <= 0:
        return None

    logger.info(
        "memory_ladder",
        high_mb=MEMORY_HIGH_MB,
        recycle_declared_mb=MEMORY_RECYCLE_MB,
        recycle_effective_mb=resolve_recycle_mb(),
        cgroup_cap_mb=cgroup_memory_max_mb(),
    )

    exit_once = threading.Event()
    stop = threading.Event()

    def _loop() -> None:
        while not stop.is_set():
            mem = get_guarded_memory_mb()
            # Re-resolve every tick: auto-adapts if the cgroup cap changes.
            threshold = resolve_recycle_mb()
            if memory_recycle_needed(mem, threshold):
                logger.warning(
                    "memory_recycle_watchdog",
                    rss_mb=mem,
                    recycle_mb=threshold,
                    interval_s=MEMORY_WATCHDOG_INTERVAL_S,
                )
                _close_indexers_best_effort(indexers)
                if not exit_once.is_set():
                    exit_once.set()
                    logger.info("memory_recycle_watchdog_exit", rss_mb=mem, recycle_mb=threshold)
                    os._exit(0)
                return
            if mem >= MEMORY_HIGH_MB:
                logger.warning(
                    "memory_high_watchdog",
                    rss_mb=mem,
                    high_mb=MEMORY_HIGH_MB,
                    recycle_mb=threshold,
                )
            _watchdog_sleep(MEMORY_WATCHDOG_INTERVAL_S)

    thread = threading.Thread(target=_loop, name="memory-watchdog", daemon=True)
    thread.stop_event = stop  # type: ignore[attr-defined]  # test seam
    thread.start()
    return thread


def export_global_metrics() -> None:
    """Publish the process / quota / RPC-pool gauges the alert rules scrape.

    Best-effort and synchronous: reads local RSS/cgroup files, the quota
    ledger, and the endpoint pool. Called periodically from
    ``_metrics_gauge_loop`` — never raises into the daemon.
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.indexer._resource_guards import get_cgroup_memory_mb, get_process_rss_mb
    from lite_indexer.rpc.health import is_endpoint_healthy
    from lite_indexer.rpc.ledger import LEDGER
    from lite_indexer.rpc.pool import is_endpoint_banned

    metrics.set_gauge("lite_indexer_rss_bytes", get_process_rss_mb() * 1024 * 1024)
    metrics.set_gauge("lite_indexer_cgroup_memory_bytes", get_cgroup_memory_mb() * 1024 * 1024)

    for provider, state in LEDGER.snapshot().items():
        consumed = state.get("consumed_pct")
        if consumed is not None:
            metrics.set_labeled_gauge("lite_indexer_quota_consumed_pct", float(consumed), provider=provider)

    for chain, endpoints in GET_LOGS_ENDPOINTS.items():
        healthy = sum(1 for endpoint in endpoints if not is_endpoint_banned(endpoint) and is_endpoint_healthy(endpoint))
        metrics.set_labeled_gauge("lite_indexer_rpc_pool_healthy_endpoints", healthy, chain=chain)


def export_chain_metrics(indexer: LiteIndexer) -> None:
    """Publish the per-chain state gauges for one running indexer."""
    last_block = indexer.state.load().get("last_block")
    if last_block is not None:
        metrics.set_labeled_gauge("lite_indexer_state_last_block", int(last_block), chain=indexer.chain)
        metrics.set_labeled_gauge("lite_indexer_last_block", int(last_block), chain=indexer.chain)
    publisher = getattr(indexer, "_nats_publisher", None)
    if publisher is not None:
        circuit_open = 1.0 if getattr(publisher, "circuit_state", "closed") == "open" else 0.0
        metrics.set_labeled_gauge("lite_indexer_nats_circuit_open", circuit_open, chain=indexer.chain)


async def _metrics_gauge_loop(indexers: list[LiteIndexer], interval: float) -> None:
    """Refresh every alert-referenced gauge on a fixed interval.

    Runs as its own task so the exporter's data stays current without
    touching the sync cycles. Any failure is logged and swallowed — metrics
    are observability, never a reason to stop indexing.
    """
    while True:
        try:
            export_global_metrics()
            for indexer in list(indexers):
                export_chain_metrics(indexer)
        except Exception as exc:  # noqa: BLE001 — metrics must never kill the daemon
            logger.warning("metrics_gauge_update_failed", error=str(exc)[:120])
        await asyncio.sleep(max(1.0, interval))


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
    tracking AND its own asyncio task polling at its own optimal interval,
    so fast chains (Base 2s) are not throttled by slow ones (Ethereum 12s).

    Concurrency is capped at ``max_concurrent`` chains syncing at once
    (default 6, see ``MAX_CONCURRENT_CHAINS``): every chain task shares
    one semaphore. High-priority chains are scheduled first; lower-priority
    chains rotate in as slots free. This bounds peak RSS — 14 chains
    running simultaneously caused repeated cgroup OOM kills on 2026-08-03.
    Runs until SIGTERM/SIGINT.
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

    # Prometheus exporter: start once on a background thread; a busy port
    # or missing library is a logged no-op, never blocking the daemon.
    start_metrics_server()

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

    # Continuous memory watchdog (2026-09-14 wedge fix): independent of the
    # sync loop so a mid-cycle cgroup throttle cannot prevent the recycle
    # check. Secondary path: post-cycle recycle_if_over_budget.
    start_memory_watchdog(indexers)

    async def _chain_loop(chain: str) -> None:
        """Independent per-chain polling loop with its own poll interval.

        The poll interval is priority-aware: high-value chains (Ethereum,
        Solana) poll at their base interval while lower-value chains poll
        less often, cutting wasted RPC calls. The per-chain memory budget
        and (optionally) CPU affinity are logged for observability.

        The shared semaphore is acquired around each sync cycle, so at
        most ``max_concurrent`` chains hold event batches in memory at
        once; contended lower-priority chains wait for a free slot — this
        is what prevents the cgroup OOM kills. After each completed cycle
        (state already persisted) the loop checks guarded memory against
        the effective recycle ceiling; at or above it the process exits
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

        # EVM WebSocket head pool: push the tip so the next sync pass starts
        # on a new head instead of waiting out the poll interval. Overlay-
        # only; HTTP polling remains the heartbeat/fallback.
        await indexer.start_head_pool()

        base_interval = CHAIN_POLL_INTERVALS.get(chain, 15.0)
        poll_interval = effective_poll_interval(chain, base_interval)
        log_chain_resources(chain, poll_interval=base_interval)

        # Chain-level health: a chain whose state has not moved for
        # CHAIN_STALL_DISABLE_SECONDS (2h) is auto-disabled and re-probed
        # slowly, so one dead chain never burns RPC budget or holds
        # concurrency slots while the others keep indexing.
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
                # 2026-09-21 solana real-time fix: the shared FIFO
                # semaphore made the solana lane wait ~6.5 min behind long
                # EVM cycles between 54s sync passes (~14 slots/min
                # effective vs ~150 slots/min chain production). Exempt
                # chains run on a dedicated lane; their own memory gate
                # (solana budget 1024MB) still applies every cycle.
                if concurrency_slot is not None and chain not in _SEMAPHORE_EXEMPT_CHAINS:
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
                    # Wait out the remaining interval, but wake early when a
                    # WS head is pushed (HTTP polling is the fallback when no
                    # pool is running). Cancelled by shutdown.
                    await indexer.wait_for_head(remaining)
                except asyncio.CancelledError:
                    break

    # Launch one task per chain, high-priority chains first so they claim
    # concurrency slots before low-priority chains.
    ordered_chains = sorted(chains, key=lambda c: chain_priority(c), reverse=True)

    # ── Parallel startup endpoint verification ──────────────────────────
    # Verification used to run inside each chain's sync loop, serialized by
    # MAX_CONCURRENT_CHAINS, so only the first few chains got a log pool
    # (observed base 15:20 -> polygon 15:27). This pass runs ALL chains
    # concurrently (ENDPOINT_VERIFY_CONCURRENCY) as a background task; each
    # chain loop waits only on ITS OWN result via wait_startup_result, so
    # it starts as soon as its verification lands. Fail-soft: any failure
    # is logged and the loop still starts, falling back to the in-loop
    # verification path.
    from lite_indexer.rpc.verify import verify_all_chains

    startup_verify_task = asyncio.create_task(
        verify_all_chains(ordered_chains),
        name="endpoint-verify-startup",
    )

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

    # Periodic alert-gauge refresh (RSS/cgroup, per-chain last block, quota
    # consumed pct, RPC pool health, NATS circuit). Keeps the /metrics
    # endpoint current for the loaded alert rules.
    metrics_task = asyncio.create_task(
        _metrics_gauge_loop(indexers, METRICS_GAUGE_INTERVAL_SECONDS),
        name="metrics-gauges",
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
    metrics_task.cancel()
    startup_verify_task.cancel()
    if price_task is not None:
        price_task.cancel()
    for task in chain_tasks:
        task.cancel()
    await asyncio.gather(
        health_task, discovery_task, metrics_task, startup_verify_task, *chain_tasks, return_exceptions=True
    )
    if price_task is not None:
        await asyncio.gather(price_task, return_exceptions=True)

    # Close every per-chain HTTP client so a clean shutdown (or restart)
    # does not leak sockets to the next process.
    await asyncio.gather(*(ix.aclose() for ix in indexers), return_exceptions=True)
    stop_metrics_server()

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
