"""
LiteIndexer — Core indexing engine.

Orchestrates the complete index cycle using the strategy pattern:
  1. Determine block range (from state or override)
  2. Fetch Transfer events via chain-specific strategy
  3. Fetch DEX Swap events (EVM only, if enabled)
  4. Fetch PairCreated/PoolCreated events (EVM only, if enabled)
   5. Enrich with block timestamps
   6. Write NDJSON (gated by NDJSON_ENABLED, default off) + ClickHouse (if configured)
   7. Save state

Each chain gets its own LiteIndexer instance with independent state tracking.
Non-EVM chains (Solana, Tron) use their own strategy with chain-specific RPC.

Recovery: the durable store is ClickHouse. The NDJSON write-ahead log is
redundant and disabled by default (NDJSON_ENABLED=false). Without NDJSON,
crash recovery is restart-from-state: ``state.to_block`` is the resume
point, so at most one sync cycle (~30s) is lost and re-fetched from RPC.
"""

from __future__ import annotations

import asyncio
import gc
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.chains.demand import (
    default_enabled_events,
    get_chain_demand,
    read_demand_files,
    record_scan_demand,
)
from lite_indexer.chunks import chunk_size_for_chain, sync_batch_for_chain
from lite_indexer.config import (
    DATA_DIR,
    DATA_RECONCILIATION_ENABLED,
    DATA_RECONCILIATION_EVERY_N_CYCLES,
    DATA_RECONCILIATION_MAX_DISCREPANCY_PCT,
    DISK_CAP_BYTES,
    GC_EVERY_N_CYCLES,
    HEAD_BUFFER,
    LAZY_INDEXING_ENABLED,
    NATS_ENABLED,
    NDJSON_ENABLED,
    PLUGIN_ENABLED,
    REORG_PROTECTION_ENABLED,
    STATE_BACKWARD_PROTECTION,
    STATE_REGRESSION_TOLERANCE_BLOCKS,
    TRACE_CONCURRENCY,
    TRACE_ENABLED,
    TRACE_MAX_BLOCKS_PER_CYCLE,
    VECTOR_INGESTION_ENABLED,
    WEBHOOK_ENABLED,
)
from lite_indexer.indexer._maintenance import MaintenanceMixin
from lite_indexer.indexer._plugins import PluginsMixin
from lite_indexer.indexer._sidecars import SidecarsMixin
from lite_indexer.indexer._rpc_fetch import RpcFetchMixin
from lite_indexer.indexer._resource_guards import (
    ClickHouseHealth,
    MemoryGate,
)
from lite_indexer.indexer._resource_guards import (
    get_process_rss_mb as get_process_rss_mb,
)
from lite_indexer.indexer._writers import (
    build_nats_payloads,
    flush_solana_launch_bundles,
    flush_tron_launch,
    maybe_run_vector_ingestion,
    write_deployer_events,
    write_ndjson,
)
from lite_indexer.logging import get_logger
from lite_indexer.plugin import BasePlugin, PluginContext, PluginLoader
from lite_indexer.rpc import RpcClient, RpcFetchFailedError, get_best_endpoint
from lite_indexer.rpc.verify import (
    EndpointVerifyResult,
    apply_verified_endpoints,
    restore_original_endpoints,
    verify_chain_endpoints,
)
from lite_indexer.state import StateHighWater, StateManager
from lite_indexer.storage import (
    ClickHouseWriter,
    NDJSONWriter,
    check_disk_cap_async,
    check_disk_space,
)
from lite_indexer.strategies import (
    SUPPORTED_CHAINS,
    EVMChainStrategy,
    SolanaStrategy,
    TronStrategy,
    create_strategy,
)
from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.streaming import NATSPublisher
from lite_indexer.webhooks import WebhookDelivery

logger = get_logger(__name__)

# Sentinel value returned by strategies when a chain reorg is detected
_REORG_SENTINEL = "__CHAIN_REORG__"


def _check_reorg(events: list[Any]) -> bool:
    """Check if the events list contains a chain reorg sentinel.

    Strategies return ["__CHAIN_REORG__"] when from_block > to_block,
    indicating the chain experienced a reorganization.

    Args:
        events: Events list returned by a strategy method.

    Returns:
        True if a reorg was detected.
    """
    return bool(events and len(events) == 1 and isinstance(events[0], str) and events[0] == _REORG_SENTINEL)


# ── Memory / ClickHouse watchdog helpers ────────────────────────────────
# Extracted into ``lite_indexer/indexer/_resource_guards.py``:
#   - get_process_rss_mb
#   - MemoryGate
#   - ClickHouseHealth
# Re-exported above so ``from lite_indexer.indexer import ...`` is unchanged.


class LiteIndexer(
    MaintenanceMixin,
    RpcFetchMixin,
    PluginsMixin,
    SidecarsMixin,
):
    """Multi-chain blockchain indexer using the strategy pattern.

    Chain-specific logic (RPC calls, event parsing) is delegated to a
    strategy object created based on the chain's type:
      - evm    → EVMChainStrategy (eth_getLogs, DEX swaps, pair creation)
      - solana → SolanaStrategy (getSignaturesForAddress, token balance changes)
      - tron   → TronStrategy (TronGrid event API, TRC20 transfers)

    Cross-cutting concerns (disk management, state, NDJSON, ClickHouse)
    are handled here in the indexer core.
    """

    def __init__(
        self,
        chain: str,
        from_block: int | None = None,
        to_block: int | None = None,
        batch_size: int = 1000,
        disk_cap: int = DISK_CAP_BYTES,
        start_from_head: bool = False,
        data_dir: Path | None = None,
        chunk_size: int | None = None,
        ndjson_enabled: bool | None = None,
        lazy_indexing: bool | None = None,
        traces_enabled: bool | None = None,
        vector_ingestion: bool | None = None,
        nats_enabled: bool | None = None,
        webhook_enabled: bool | None = None,
    ) -> None:
        self.chain = chain
        self.batch_size = batch_size
        self.disk_cap = disk_cap
        self.dex_enabled = False
        self.deployers_enabled = False
        # Internal-transfer trace collection (optional, TRACE_ENABLED /
        # --traces). Off by default because free-tier trace RPCs are
        # aggressively rate-limited; an explicit value overrides the env
        # flag. When enabled, value-moving internal calls (WETH unwraps,
        # bridge deposits, fee flows) land in
        # wallet_memory.internal_transfers alongside transfers.
        self.traces_enabled = TRACE_ENABLED if traces_enabled is None else traces_enabled
        # Lazy indexing: per-chain event type gating. When enabled (default),
        # top chains always index DEX + deployer and long-tail chains only
        # after the scanner signals demand (see chains/demand.py). When
        # disabled, CLI flags are the sole authority (legacy behavior).
        self.lazy_indexing_enabled = lazy_indexing if lazy_indexing is not None else LAZY_INDEXING_ENABLED

        # Optional LanceDB vector ingestion (VECTOR_INGESTION_ENABLED, default
        # off). When enabled, the sync loop periodically reads the freshly
        # synced block range from ClickHouse, embeds wallet/contract text
        # representations via Ollama, and stores them in LanceDB collections.
        # Best-effort: a failure logs a warning and never blocks indexing.
        self.vector_ingestion_enabled = VECTOR_INGESTION_ENABLED if vector_ingestion is None else vector_ingestion
        # Lazy LanceDB store (one per indexer; only created when enabled).
        self._vector_store: Any = None
        # Last block covered by vector ingestion (None = never run → first
        # cycle ingests regardless of the block interval).
        self._last_vector_ingestion_block: int | None = None
        self.start_from_head = start_from_head
        # User-requested head mode (--start-from-head): never auto-cleared.
        # Reorg-triggered head mode is cleared after a successful cycle.
        self.start_from_head_user = start_from_head
        self._reorg_recovery = False
        self.data_dir = data_dir or DATA_DIR

        # NDJSON write-ahead log (redundant with ClickHouse). Defaults to
        # the NDJSON_ENABLED env flag (default off); an explicit value
        # overrides it. When disabled the writer is never constructed and
        # step 6 of the sync loop is skipped entirely.
        self.ndjson_enabled = NDJSON_ENABLED if ndjson_enabled is None else ndjson_enabled

        # NATS JetStream real-time event publishing. Best-effort fan-out
        # after ClickHouse writes — NATS downtime never blocks indexing.
        # Defaults to the NATS_ENABLED env flag (default off); an explicit
        # value overrides it. The publisher itself is lazy: created on
        # first use, connection opened on first publish.
        self.nats_enabled = NATS_ENABLED if nats_enabled is None else nats_enabled
        self._nats_publisher: NATSPublisher | None = None

        # Webhook delivery (lite_indexer.webhooks). Turns the NATS
        # real-time stream into a push platform — registered webhooks
        # receive every matching event as a signed POST. Defaults to the
        # WEBHOOK_ENABLED env flag (default on); an explicit value
        # overrides it. The delivery service only runs when NATS is also
        # enabled (a NATS subscriber has nothing to consume otherwise).
        # It is a process-wide singleton, so per-chain daemon tasks share
        # ONE NATS subscription — never one per chain.
        self.webhook_enabled = WEBHOOK_ENABLED if webhook_enabled is None else webhook_enabled
        self._webhook_delivery: WebhookDelivery | None = None

        # Chain type detection
        chain_info = SUPPORTED_CHAINS.get(chain, {"type": "evm"})
        self._chain_type: str = chain_info.get("type", "evm")

        # Chunk-based indexing: batch ranges into chunk_size-wide chunks
        # fetched in parallel. Defaults per chain type (50 EVM, 500 Solana).
        self.chunk_size = chunk_size if chunk_size and chunk_size > 0 else chunk_size_for_chain(chain)

        # State manager (generic — works for all chain types)
        self.state = StateManager(chain)
        # Monotonic high-water mark for the backward-state guard
        # (STATE_BACKWARD_PROTECTION). Updated after every successful
        # forward sync; consulted on load to detect state regressions.
        self._highwater = StateHighWater(chain)
        # Per-indexer cycle counter for periodic GC + data reconciliation.
        self._cycles_since_gc = 0
        self._cycles_since_reconcile = 0
        # In-process memory watchdog (RSS) — pauses ingestion when high.
        self._memory_gate = MemoryGate()
        # Cached ClickHouse reachability probe — graceful CH pause.
        self._ch_health = ClickHouseHealth()

        # Event types persisted as enabled in the state file (lazy indexing).
        # DEX/deployer stay enabled across restarts once demand is observed.
        persisted_events = self.state.load().get("enabled_events") or []
        self._persisted_enabled_events: set[str] = {str(event_type) for event_type in persisted_events}

        # Optional override blocks (for testing / backfill)
        self._override_from = from_block
        self._override_to = to_block

        # RPC client (used by EVM strategy, others create their own)
        self.rpc = RpcClient()

        # Latest endpoint verification result (EVM only) — restored after sync
        self._verify_result: EndpointVerifyResult | None = None

        # Storage writers
        self._ndjson_writer: NDJSONWriter | None = (
            NDJSONWriter(data_dir=self.data_dir, disk_cap=self.disk_cap) if self.ndjson_enabled else None
        )
        self._ch_writer: ClickHouseWriter | None = None

        # Strategy — chain-specific logic
        self._strategy: AbstractChainStrategy | None = None

        # Plugins — lazily loaded (chain-scoped) on first sync cycle.
        # None means "not yet loaded"; an empty list means no plugins.
        self._plugins: list[BasePlugin] | None = None

    # ── Strategy ─────────────────────────────────────────────────────

    @property
    def strategy(self) -> AbstractChainStrategy:
        """Lazy-initialized chain strategy.

        Creates the appropriate strategy based on chain type:
          evm → EVMChainStrategy (with RPC client + batch size)
          solana → SolanaStrategy
          tron → TronStrategy
        """
        if self._strategy is None:
            self._strategy = create_strategy(
                chain=self.chain,
                rpc_client=self.rpc,
                batch_size=self.batch_size,
                chunk_size=self.chunk_size,
            )
        return self._strategy

    # ── Helpers ──────────────────────────────────────────────────────
    def _save_current_state(self) -> None:
        """Save current state without advancing (for error/pause paths).

        Loads the current state file and re-saves it unchanged, so the
        last_block and enabled_events persist across error/pause cycles.
        This prevents state loss when sync_loop returns early due to
        transient failures (CH unavailable, fetch failed, etc.).
        """
        try:
            current = self.state.load()
            last_block = current.get("last_block")
            enabled_events = current.get("enabled_events")
            if last_block is not None:
                self.state.save(last_block, 0, enabled_events=enabled_events)
                logger.debug("state_saved_current", chain=self.chain, last_block=last_block)
        except Exception as exc:
            logger.warning("state_save_current_failed", chain=self.chain, error=str(exc)[:100])

    def _is_evm(self) -> bool:
        """Check if this chain uses EVM strategy."""
        return self._chain_type == "evm"

    # ── Lazy Indexing ────────────────────────────────────────────────

    def _enable_event_type(self, event_type: str) -> None:
        """Enable an event type flag (sticky — never disabled once on).

        Args:
            event_type: ``'dex'`` or ``'deployer'``. Unknown types ignored.
        """
        if event_type == "dex":
            self.dex_enabled = True
        elif event_type == "deployer":
            self.deployers_enabled = True

    def _enabled_event_types(self) -> list[str]:
        """Event types currently enabled for this chain (for state persistence)."""
        enabled: list[str] = []
        if self.dex_enabled:
            enabled.append("dex")
        if self.deployers_enabled:
            enabled.append("deployer")
        return enabled

    def _refresh_demand_enabled(self) -> None:
        """Apply lazy-indexing defaults + scanner demand signals.

        Called at the start of each sync cycle, after transfers are
        fetched. Under lazy indexing:

          - Top chains (ethereum, bsc, polygon, arbitrum, base, solana)
            always get DEX + deployer enabled (high traffic).
          - Long-tail chains get DEX/deployer enabled when the scanner
            touched the demand file (``{INDEXER_DEMAND_DIR}/{chain}/dex``
            or ``deployer``).
          - Event types persisted as enabled in the state file stay
            enabled across restarts.

        Flags are sticky: once enabled they are never disabled for the
        process lifetime, satisfying "once enabled, keep enabled". The
        enabled set is persisted to the state file at the end of the
        cycle via :meth:`_enabled_event_types`.

        No-op when lazy indexing is disabled (legacy: CLI flags are the
        sole authority).
        """
        if not self.lazy_indexing_enabled:
            return

        for event_type in default_enabled_events(self.chain):
            self._enable_event_type(event_type)
        for event_type in self._persisted_enabled_events:
            self._enable_event_type(event_type)
        for event_type in read_demand_files(self.chain):
            record_scan_demand(self.chain, event_type)
            self._enable_event_type(event_type)
        demand = get_chain_demand(self.chain)
        if demand:
            logger.debug("demand_enabled", chain=self.chain, events=sorted(demand))

    @property
    def ch_writer(self) -> ClickHouseWriter:
        """Lazy-initialized ClickHouse writer."""
        if self._ch_writer is None:
            self._ch_writer = ClickHouseWriter()
        return self._ch_writer

    def _ch_table_for_chain(self) -> tuple[str, str]:
        """ClickHouse table + block column for this chain's range checks.

        EVM + Tron write to ``evm_transfers`` keyed by ``chain``;
        Solana writes to ``solana_spl_balance_changes`` (slot-based, no
        chain column — the table is solana-only).

        Returns:
            ``(table, column)`` tuple for range completeness queries.
        """
        if self._chain_type == "solana":
            from lite_indexer.config import SOLANA_CH_TABLE

            return SOLANA_CH_TABLE, "slot"
        from lite_indexer.config import CH_TABLE

        return CH_TABLE, "block_number"

    def _fetch_ch_max_block(self) -> int | None:
        """Query ClickHouse for the actual max indexed block for this chain.

        Best-effort: returns None when ClickHouse is unreachable or the
        query fails (the caller then falls back to the high-water mark).

        Returns:
            Max block/slot present in ClickHouse for this chain, or None.
        """
        try:
            client = self.ch_writer.client()
            if client is None:
                return None
            table, column = self._ch_table_for_chain()
            if self._chain_type == "solana":
                rows = client.execute(f"SELECT max({column}) FROM {table}")
            else:
                rows = client.execute(
                    f"SELECT max({column}) FROM {table} WHERE chain = %(chain)s",
                    {"chain": self.chain},
                )
            if rows and rows[0] and rows[0][0] is not None:
                return int(rows[0][0])
            return None
        except Exception as exc:  # noqa: BLE001 — guard is best-effort
            logger.debug("ch_max_block_query_failed", chain=self.chain, error=str(exc)[:80])
            return None

    async def _guard_state_regression(
        self,
        state: dict[str, Any],
        from_block: int,
        summary: dict[str, Any],
    ) -> int | None:
        """Detect + recover from a state-file regression.

        A regression is when the state file's resume block is more than
        ``STATE_REGRESSION_TOLERANCE_BLOCKS`` below the monotonic
        high-water mark for this chain. Reorg rollbacks rewind state by a
        few blocks (within tolerance) and are left alone.

        Recovery: query ClickHouse for the actual max block and resume
        from ``max(ch_max, highwater)`` — never from genesis. If both the
        CH query and the high-water mark are unavailable the state file
        is trusted as-is.

        Args:
            state: The loaded state dict (may be ``{}``).
            from_block: The resume block derived from the state file.
            summary: Sync summary dict; recovery is recorded on it.

        Returns:
            A corrected resume block, or None when no correction applies.
        """
        highwater = self._highwater.load()
        if highwater is not None and from_block < highwater - STATE_REGRESSION_TOLERANCE_BLOCKS:
            ch_max = self._fetch_ch_max_block()
            resume = max(highwater, ch_max or 0) + 1
            if resume > from_block:
                logger.warning(
                    "state_regression_recovered",
                    chain=self.chain,
                    state_last_block=state.get("last_block", 0),
                    highwater=highwater,
                    ch_max=ch_max,
                    resume_block=resume,
                )
                summary["state_regression"] = True
                summary["state_regression_recovered_to"] = resume
                self._highwater.update(resume - 1)
                return resume
            return None

        # No high-water mark yet (first run with protection enabled): if
        # ClickHouse already has data far ahead of the state file, that is
        # also a regression (state file was clobbered before the mark was
        # ever written). Only trust this when CH is actually ahead — a
        # healthy genesis backfill has ch_max <= state anyway.
        ch_max = self._fetch_ch_max_block()
        if ch_max is not None and from_block < ch_max - STATE_REGRESSION_TOLERANCE_BLOCKS:
            resume = ch_max + 1
            logger.warning(
                "state_regression_recovered_ch",
                chain=self.chain,
                state_last_block=state.get("last_block", 0),
                ch_max=ch_max,
                resume_block=resume,
            )
            summary["state_regression"] = True
            summary["state_regression_recovered_to"] = resume
            self._highwater.update(resume - 1)
            return resume

        return None
        self.state.save(from_block - 1, 0, enabled_events=self._enabled_event_types())

    async def _determine_block_range(self, summary: dict[str, Any]) -> tuple[int, int] | None:
        """Determine the block range for this sync cycle.

        When --start-from-head is active, ignore state file and start
        from (chain_tip - HEAD_BUFFER). No backfill from genesis.

        Returns (from_block, to_block) or None if we should abort.
        """
        if self.start_from_head:
            tip = await self.fetch_block_number()
            if tip is None:
                summary["status"] = "no_rpc_available"
                summary["errors"].append("fetch_block_number failed")
                logger.error("sync_aborted_no_rpc", chain=self.chain)
                return None
            from_block = max(0, tip - HEAD_BUFFER)
            to_block = tip
            return from_block, to_block

        # Normal path: resume from state file or override
        state = self.state.load()
        from_block = self._override_from if self._override_from is not None else state.get("last_block", 0) + 1

        # Backward-state guard (STATE_BACKWARD_PROTECTION): a state file
        # that regressed below the monotonic high-water mark (or below
        # the ClickHouse max block) is corruption — a torn write, an OOM
        # mid-write, or a companion writer clobbering last_block. Resume
        # from the actual max block in ClickHouse instead of re-indexing
        # from genesis (observed 08/03 + 08/04: ethereum state reset to
        # block ~93K / 100 while the CH max was 25.6M).
        if STATE_BACKWARD_PROTECTION and self._override_from is None:
            guarded = await self._guard_state_regression(state, from_block, summary)
            if guarded is not None:
                from_block = guarded

        if from_block <= 1:
            from_block = 0

        tip = await self.fetch_block_number()
        if tip is None:
            summary["status"] = "no_rpc_available"
            summary["errors"].append("fetch_block_number failed")
            logger.error("sync_aborted_no_rpc", chain=self.chain)
            return None

        to_block = self._override_to if self._override_to is not None else tip

        if from_block >= to_block:
            logger.info("caught_up", chain=self.chain, block=tip)
            summary["status"] = "caught_up"
            return None

        cycle_cap = sync_batch_for_chain(self.chain, self.batch_size)
        if to_block - from_block > cycle_cap:
            to_block = from_block + cycle_cap

        return from_block, to_block

    # ── Main Sync Loop ─────────────────────────────────────────────

    async def sync_loop(self) -> dict[str, Any]:
        """Main sync loop: load state → fetch → persist → save.

        Returns summary dict with stats.
        """
        # Periodic GC: force gc.collect() every GC_EVERY_N_CYCLES cycles.
        # Runs at the START so even aborted cycles count toward the budget
        # (cyclic GC misses event dict / bloom / httpx reference cycles
        # under cgroup memory pressure).
        self._maybe_run_gc()

        start_time = time.monotonic()
        summary: dict[str, Any] = {
            "chain": self.chain,
            "status": "ok",
            "events_fetched": 0,
            "events_written_ndjson": 0,
            "events_written_ch": 0,
            "nats_events_published": 0,
            "bytes_written": 0,
            "duration_seconds": 0,
            "errors": [],
        }

        # 1. Disk cap + disk space checks (generic — all chains)
        # When NDJSON is disabled, exclude its files (*.jsonl / *.jsonl.zst)
        # from the cap so it only counts the durable Parquet/state data.
        disk_excludes: tuple[str, ...] = ()
        if not self.ndjson_enabled:
            disk_excludes = ("*.jsonl", "*.jsonl.zst")
        if not await check_disk_cap_async(self.data_dir, self.disk_cap, exclude_patterns=disk_excludes):
            summary["status"] = "disk_cap_exceeded"
            summary["errors"].append("disk cap exceeded")
            self._save_current_state()
            return summary

        if not check_disk_space():
            summary["status"] = "disk_full"
            summary["errors"].append("disk space critically low")
            self._save_current_state()
            return summary

        # 2. Endpoint verification (EVM only — uses eth_getLogs probe)
        if self._is_evm():
            evm_result = await self._evm_verify_endpoints(summary)
            if evm_result is not None:
                return evm_result

        # 3. Inner sync loop
        return await self._sync_loop_inner(summary, start_time)

    async def _evm_verify_endpoints(self, summary: dict[str, Any]) -> dict[str, Any] | None:
        """Verify EVM endpoints are healthy before syncing.

        This is EVM-specific because it uses eth_getLogs for verification.
        Non-EVM chains skip this step.

        WARNING: Concurrent access. This method temporarily mutates the
        module-global ``chains_mod.GET_LOGS_ENDPOINTS`` dict and restores
        it at the start of ``_sync_loop_inner``. It is NOT safe to run two
        sync loops for the SAME chain concurrently — parallel daemon tasks
        must target distinct chains. The swap is only visible to code that
        reads the dict at call time.

        Returns a summary dict if we should abort, or None to proceed.
        """
        result = await verify_chain_endpoints(self.chain, self.rpc)
        if result is None:
            summary["status"] = "no_endpoints"
            summary["errors"].append(f"no endpoints configured for {self.chain}")
            self._save_current_state()
            return summary

        summary["endpoints_total"] = result.total
        summary["endpoints_verified"] = result.verified

        # Temporarily use verified endpoints for this sync cycle
        self._verify_result = result
        apply_verified_endpoints(result)
        return None  # Proceed to sync

    def _clear_reorg_recovery(self) -> None:
        """Clear the reorg-recovery head start after a successful sync.

        A reorg flips ``start_from_head`` on to re-index from near the tip.
        That override is one-shot: once the range is synced successfully it
        is cleared so subsequent cycles resume from saved state. A head
        start explicitly requested by the user (``--start-from-head``) is
        preserved.
        """
        if self._reorg_recovery:
            self.start_from_head = self.start_from_head_user
            self._reorg_recovery = False

    async def _sync_loop_inner(
        self,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any]:
        """Inner sync loop after endpoint verification.

        Handles all chain types via strategy delegation:
          - Transfer events: strategy.fetch_transfers()
          - DEX/deployer: strategy.supports_dex() / supports_deployers()
          - Timestamps: strategy.fetch_block_timestamps()
          - ClickHouse: strategy.write_clickhouse()

        The verified endpoint swap applied by ``_evm_verify_endpoints`` is
        in effect for the whole cycle; the originals are restored on exit
        (finally) so the next cycle starts from the configured list.
        """
        try:
            return await self._sync_loop_body(summary, start_time)
        finally:
            # Restore EVM endpoints after the sync cycle (verified swap is
            # only valid for one cycle).
            try:
                if self._is_evm() and self._verify_result is not None:
                    restore_original_endpoints(self._verify_result)
            except (KeyError, TypeError) as exc:
                logger.warning("endpoint_restore_failed", chain=self.chain, error=str(exc)[:80])

    async def _sync_loop_body(
        self,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any]:
        """Shared body of the inner sync loop (restore handled in finally)."""
        # 2. Determine block range
        block_range = await self._determine_block_range(summary)
        if block_range is None:
            self._save_current_state()
            return summary
        from_block, to_block = block_range

        # 2a. Graceful ClickHouse pause: probe CH BEFORE fetching events
        # from RPC. When CH is unreachable the cycle aborts here with
        # status "ch_unavailable" — no RPC budget burned on fetches that
        # cannot be persisted, no state movement, no crash. The daemon
        # treats this as a pause (retry next poll interval).
        if not self._ch_health.ok():
            summary["status"] = "ch_unavailable"
            summary["errors"].append("clickhouse unreachable — paused indexing")
            summary["duration_seconds"] = time.monotonic() - start_time
            logger.warning(
                "sync_paused_ch_unavailable",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
            )
            self._save_current_state()
            return summary

        # 3. Fetch transfer events via strategy
        logger.info("sync_start", chain=self.chain, from_block=from_block, to_block=to_block)

        try:
            events = await asyncio.wait_for(self.fetch_transfers(from_block, to_block), timeout=120.0)
        except (TimeoutError, RpcFetchFailedError) as exc:
            # Data-loss guard: all endpoints failed for this range. Do NOT
            # advance state — the range is retried on the next cycle.
            logger.error(
                "sync_aborted_fetch_failed",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
                error=str(exc)[:120],
            )
            summary["status"] = "fetch_failed"
            summary["errors"].append(f"all endpoints failed for range {from_block}-{to_block}")
            summary["duration_seconds"] = time.monotonic() - start_time
            self._save_current_state()
            return summary

        if events:
            summary["events_fetched"] = len(events)

        # 3a. Handle chain reorg
        if _check_reorg(events):
            logger.warning("chain_reorg_during_sync", chain=self.chain)
            self.start_from_head = True
            self._reorg_recovery = True
            summary["status"] = "reorg_detected"
            summary["duration_seconds"] = time.monotonic() - start_time
            self._save_current_state()
            return summary

        # 3b. Lazy indexing: apply top-chain defaults + scanner demand
        # signals before deciding whether to fetch DEX/deployer events.
        # Runs after transfers so demand files touched this cycle (by the
        # rmi-backend scanner) are observed. Flags are sticky once enabled.
        self._refresh_demand_enabled()

        # 4. Fetch DEX swap events (EVM only, if enabled)
        dex_events: list[dict[str, Any]] = []
        if self.dex_enabled and self.strategy.supports_dex():
            dex_events = await self.fetch_swap_events(from_block, to_block)
            if _check_reorg(dex_events):
                logger.warning("chain_reorg_during_dex_sync", chain=self.chain)
                self.start_from_head = True
                self._reorg_recovery = True
                summary["status"] = "reorg_detected"
                summary["duration_seconds"] = time.monotonic() - start_time
                self._save_current_state()
                return summary
            summary["dex_events_fetched"] = len(dex_events)

        # 4a. Fetch pair created events for deployer tracking (EVM only).
        # Runs even when DEX is enabled — dex_handler fetches pair-created
        # events to build its token cache, but deployer events are only
        # written from the strategy's _last_pair_events, which is populated
        # here.
        #
        # FIXED (2026-08-02, commit 835a9ff): the guard previously read
        #   `if self.deployers_enabled and not self.dex_enabled`
        # which silently disabled deployer tracking whenever DEX indexing
        # was on (production runs `--dex --deployers`). That caused a
        # ~2.9-day deployer-event gap. The guard below is correct: deployer
        # events must run whenever deployers are enabled, independent of the
        # DEX flag. Do NOT re-add `and not self.dex_enabled`.
        if self.deployers_enabled and self.strategy.supports_deployers():
            try:
                await self._fetch_pair_created_events(from_block, to_block)
            except Exception as exc:
                logger.warning("deployer_pair_fetch_failed", chain=self.chain, error=str(exc)[:100])

        # 4b. Internal-transfer traces (EVM only, optional). The #1
        # analytical blind spot: WETH unwraps, bridge deposits, fee flows
        # and SELFDESTRUCT payouts never emit ERC-20 Transfer logs. When
        # traces_enabled, sweep the sync range via free trace_* RPCs.
        # Best-effort + rate-limit-aware: a trace failure logs a warning
        # and returns [] — it never blocks or aborts the transfer sync.
        #
        # Runs BEFORE the "no events" short-circuit below: a block can
        # contain internal transfers without any ERC-20 Transfer logs, so
        # trace collection must not be skipped just because transfers were
        # empty.
        trace_events: list[dict[str, Any]] = []
        if self.traces_enabled:
            trace_events = await self._collect_internal_transfers(from_block, to_block)
            if trace_events:
                summary["trace_events_fetched"] = len(trace_events)

        # Check if we have any events or deployer tracking
        if not events and not dex_events and not self.deployers_enabled and not trace_events:
            self._clear_reorg_recovery()
            self.state.save(to_block, 0, enabled_events=self._enabled_event_types())
            self._highwater.update(to_block)
            summary["status"] = "no_events"
            summary["duration_seconds"] = time.monotonic() - start_time
            return summary

        # 5. Enrich with timestamps (skip reorg sentinel)
        if events and not _check_reorg(events):
            events = await self._enrich_timestamps(events)
        if dex_events and not _check_reorg(dex_events):
            dex_events = await self._enrich_timestamps(dex_events)

        # 5a. Plugin dispatch — registered plugins receive decoded events
        # for their declared contracts (EVM only, best-effort). Runs after
        # timestamp enrichment so plugin handlers observe the same block
        # context as the core pipeline. Failures are contained per plugin.
        plugin_stats = await self._dispatch_plugin_events(from_block, to_block)
        if plugin_stats:
            summary.update(plugin_stats)

        # 5b. Write deployer events (EVM + Solana)
        # write_deployer_events resolves the real deployer EOA (tx `from`)
        # via eth_getTransactionByHash before inserting, so the async await
        # is required. Solana deployer events are captured structurally by
        # the strategy (creation tx fee payer) and written as-is.
        deployer_written = 0
        evm_strategy = self.strategy if isinstance(self.strategy, EVMChainStrategy) else None
        solana_strategy = self.strategy if isinstance(self.strategy, SolanaStrategy) else None
        tron_strategy = self.strategy if isinstance(self.strategy, TronStrategy) else None
        if self.deployers_enabled:
            deployer_written = await write_deployer_events(
                self.chain,
                evm_strategy,
                solana_strategy,
                self.ch_writer,
                self._enrich_timestamps,
                nats=self.nats_publisher,
            )
            if deployer_written:
                summary["deployer_events_written"] = deployer_written

        # 5c. Write launch + Jito bundle events (Solana + Tron). Each write
        # function persists to ClickHouse AND fans out to NATS, mirroring the
        # deployer-event path. Best-effort: a write failure logs and keeps the
        # event buffered for the next cycle. Buffers are drained below.
        launch_written = 0
        if solana_strategy is not None:
            launch_written += await flush_solana_launch_bundles(solana_strategy, self.ch_writer, self.nats_publisher)
        if tron_strategy is not None:
            launch_written += await flush_tron_launch(tron_strategy, self.ch_writer, self.nats_publisher)
        if launch_written:
            summary["launch_events_written"] = launch_written

        # 6. Write NDJSON (gated by NDJSON_ENABLED — redundant with
        # ClickHouse, which is the durable store). When disabled, recovery
        # is restart-from-state: at most one sync cycle is lost on crash
        # and re-fetched from RPC on the next cycle.
        if self.ndjson_enabled and self._ndjson_writer is not None:
            # NDJSON write runs check_write_ok → check_disk_cap → a blocking
            # `du` subprocess; move it off the event loop so one chain's disk
            # check never stalls the other chains' RPC/state work.
            await asyncio.to_thread(write_ndjson, self._ndjson_writer, self.chain, events, dex_events, summary)

        # 7. Write ClickHouse via strategy
        # ch_available tracks whether ClickHouse is confirmed writable for
        # this cycle: transfers insert OK, or there were no transfers to
        # write (then CH availability is only probed by the trace write
        # itself, which degrades gracefully).
        ch_available = not events
        if events:
            ch_inserted = self.strategy.write_clickhouse(events, self.ch_writer)
            summary["events_written_ch"] = ch_inserted
            ch_available = ch_inserted > 0

            # Data-loss guard: if events were fetched but NOT written to
            # ClickHouse (CH unavailable / insert failed), do NOT advance
            # state — the range is retried on the next cycle. Previously
            # the state advanced unconditionally, silently skipping blocks
            # whenever CH hiccupped (observed 08/03: bsc +500 blocks and
            # tron +35 blocks ahead of the ClickHouse max).
            if ch_inserted == 0:
                logger.error(
                    "sync_aborted_ch_write_failed",
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                    events=len(events),
                )
                summary["status"] = "ch_write_failed"
                summary["duration_seconds"] = time.monotonic() - start_time
                self._save_current_state()
                return summary

            # 7a. Wallet-access optimizations — address-first projections,
            # token metadata reference table, incremental balance cache,
            # and the auto-maintained holder-balances materialized view.
            # Each is gated by its own config flag; only run after the
            # transfer insert succeeded so the caches never diverge from
            # evm_transfers. Stats land in the sync summary (e.g.
            # token_meta_upserted / holder_balance_updates).
            wallet_stats = await self.ch_writer.write_wallet_features(self.chain, events)
            if wallet_stats:
                summary.update(wallet_stats)

        if dex_events:
            dex_ch_inserted = self.ch_writer.write_dex_events(self.chain, dex_events)
            summary["dex_events_written_ch"] = dex_ch_inserted

        # 7a2. Write internal-transfer traces (optional, best-effort).
        # Unlike transfers, a trace write failure does NOT abort the sync:
        # the trace table is enrichment (rate-limited free-tier RPCs may
        # skip blocks) and the durable transfer record has already been
        # written above. Rows are only written once the transfer insert
        # succeeded, so evm_transfers and internal_transfers stay aligned
        # on block ranges.
        if trace_events and ch_available:
            trace_written = await self.ch_writer.write_internal_transfers(self.chain, trace_events)
            if trace_written:
                summary["traces_written_ch"] = trace_written

        # 7b. Reorg staging (EVM only, gated by REORG_PROTECTION_ENABLED):
        # checkpoint the batch tip block hash, then verify the block at
        # to_block - MAX_REORG_DEPTH[chain] against its checkpoint. On
        # mismatch the chain reorganized underneath us: roll back
        # evm_transfers + dex_events above the fork point and re-fetch
        # from the fork on the next cycle (state is rewound to the fork).
        if self._is_evm() and REORG_PROTECTION_ENABLED and isinstance(self.strategy, EVMChainStrategy):
            try:
                ch_client = self.ch_writer.client()
            except Exception as exc:  # noqa: BLE001 — staging is best-effort
                ch_client = None
                logger.debug("reorg_ch_client_failed", chain=self.chain, error=str(exc)[:80])

            if ch_client is not None:
                fork_block = await self.strategy.reorg_stage(from_block, to_block, ch_client)
                if fork_block is not None:
                    logger.warning(
                        "reorg_rollback_scheduled",
                        chain=self.chain,
                        fork_block=fork_block,
                    )
                    summary["status"] = "reorg_rollback"
                    summary["reorg_fork_block"] = fork_block
                    # Re-fetch from fork: next cycle starts at fork_block.
                    self.state.save(fork_block - 1, 0, enabled_events=self._enabled_event_types())
                    summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
                    return summary

        # 7c. NATS JetStream real-time event publishing (best-effort).
        # After the durable ClickHouse writes succeed, fan events out to
        # NATS for real-time consumers (scanner alerts, dashboards, bots).
        # NATS is never the durable store: failures are logged and
        # swallowed, and the publisher's circuit breaker opens after
        # repeated failures so a dead NATS server costs ~one attempt per
        # cycle instead of one per event. Deployer events are read from
        # the strategy's last-batch lists (already enriched with
        # timestamps at 5b).
        nats_deployer_events: list[dict[str, Any]] = []
        if self.deployers_enabled:
            if evm_strategy and evm_strategy._last_pair_events:
                nats_deployer_events = list(evm_strategy._last_pair_events)
            elif solana_strategy and solana_strategy._last_deployer_events:
                nats_deployer_events = list(solana_strategy._last_deployer_events)
        nats_published = await self._publish_nats_events(
            self.chain,
            to_block,
            events,
            dex_events,
            nats_deployer_events,
            trace_events,
        )
        if nats_published:
            summary["nats_events_published"] = nats_published

        # 7d. Webhook delivery fan-out (best-effort, process-wide). The
        # delivery service subscribes to the SAME NATS subjects the
        # publisher writes to and POSTs matching events to registered
        # webhooks (HMAC-signed, bounded retries). It is started lazily
        # here — after the first NATS publish — and is idempotent, so in
        # daemon mode the first chain to publish starts the single shared
        # subscriber and later chains are no-ops.
        await self._maybe_start_webhook_delivery()

        # 8. Save state
        total_events = len(events) + len(dex_events) + deployer_written + len(trace_events)
        self._clear_reorg_recovery()
        self.state.save(to_block, total_events, enabled_events=self._enabled_event_types())
        # Monotonic high-water mark — only ever moves forward. Feeds the
        # backward-state guard on the next load.
        self._highwater.update(to_block)

        # 8b. Periodic data reconciliation: every N cycles compare the
        # ClickHouse row count for this range against the events fetched
        # from RPC; a >5% shortfall rewinds state so the next cycle
        # backfills the range. Best-effort — never blocks or aborts.
        self._maybe_reconcile_range(from_block, to_block, len(events), summary)

        # 8a. Vector ingestion (optional, LanceDB). Reads the freshly synced
        # block range from ClickHouse, generates text representations for
        # wallets/contracts, embeds them via Ollama (nomic-embed-text), and
        # stores them in LanceDB collections. Gated by
        # VECTOR_INGESTION_ENABLED (default off) and the block interval
        # VECTOR_INGESTION_INTERVAL_BLOCKS. Exception-guarded: a vector
        # ingestion failure logs a warning and never blocks or aborts the
        # sync cycle — the durable store is ClickHouse, not LanceDB.
        await self._run_vector_ingestion_guarded(from_block, to_block, summary)

        summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
        logger.info(
            "sync_complete",
            chain=self.chain,
            transfer_events=len(events),
            dex_events=len(dex_events),
            deployer_events=deployer_written,
            trace_events=len(trace_events),
            duration_s=summary["duration_seconds"],
        )

        return summary
