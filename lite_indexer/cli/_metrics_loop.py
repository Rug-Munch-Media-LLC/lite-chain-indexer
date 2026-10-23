"""Alert-gauge exporters and their refresh loop.

Moved verbatim out of ``cli/_daemon.py`` (2026-10-24) so the daemon CLI module
keeps one concern: startup and supervision. BLOCKING CALLS MUST NOT RUN ON THE
EVENT LOOP: both exporters use the synchronous clickhouse_driver Client;
measured 2026-10-23 in production, 76 queries / 69.3 s of ClickHouse time in a
120 s window (~58% of wall clock) ran inline and starved every chain loop (bsc
never got an event-loop slot, its high-water froze for 48 days while the
container looked healthy). They run on a worker thread.
"""

from __future__ import annotations

import asyncio

from lite_indexer import metrics
from lite_indexer.indexer import LiteIndexer
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def export_global_metrics() -> None:
    """Publish the process / quota / RPC-pool gauges the alert rules scrape.

    Best-effort and synchronous: reads local RSS/cgroup files, the quota
    ledger, and the endpoint pool. Called periodically from
    ``_metrics_gauge_loop`` — never raises into the daemon.
    """
    from lite_indexer.indexer._resource_guards import get_cgroup_memory_mb, get_process_rss_mb
    from lite_indexer.rpc.ledger import LEDGER

    metrics.set_gauge("lite_indexer_rss_bytes", get_process_rss_mb() * 1024 * 1024)
    metrics.set_gauge("lite_indexer_cgroup_memory_bytes", get_cgroup_memory_mb() * 1024 * 1024)

    for provider, state in LEDGER.snapshot().items():
        consumed = state.get("consumed_pct")
        if consumed is not None:
            metrics.set_labeled_gauge("lite_indexer_quota_consumed_pct", float(consumed), provider=provider)

    # Pool gauges live in rpc/pool_metrics.py (producer fix, 2026-10-23):
    # "healthy" is USABLE (proven to serve logs) and carries the real span a
    # working endpoint serves, instead of counting liveness or an unprobed
    # shape as zero/healthy.
    from lite_indexer.rpc.pool_metrics import publish_pool_metrics

    publish_pool_metrics()


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


async def metrics_gauge_loop(
    indexers: list[LiteIndexer],
    interval: float,
    chains: list[str],
    parked: set[str],
) -> None:
    """Refresh every alert-referenced gauge on a fixed interval.

    Runs as its own task so the exporter's data stays current without
    touching the sync cycles. Any failure is logged and swallowed — metrics
    are observability, never a reason to stop indexing.
    """
    from lite_indexer.metrics_daily_health import export_daily_health
    from lite_indexer.metrics_slot import export_slot_metrics

    while True:
        try:
            # BLOCKING CALLS MUST NOT RUN ON THE EVENT LOOP. Both exporters use
            # the synchronous clickhouse_driver Client; measured 2026-10-23 on
            # production: 76 queries / 69.3 s of ClickHouse time in a 120 s
            # window (~58% of wall clock) executed inline here, which starved
            # every chain loop - bsc never got an event-loop slot to sync, so
            # its high-water froze for 48 days while the container looked
            # healthy. Run them on a worker thread instead.
            await asyncio.to_thread(export_global_metrics)
            # ALL CONFIGURED CHAINS, not only live indexers: a chain that
            # never reached a cycle used to have no freshness series at all,
            # which is why the stall alert stayed silent for 12 of 16 chains.
            await asyncio.to_thread(export_daily_health, chains, parked)
            for indexer in list(indexers):
                export_chain_metrics(indexer)
            export_slot_metrics()
        except Exception as exc:  # noqa: BLE001 — metrics must never kill the daemon
            logger.warning("metrics_gauge_update_failed", error=str(exc)[:120])
        await asyncio.sleep(max(1.0, interval))
