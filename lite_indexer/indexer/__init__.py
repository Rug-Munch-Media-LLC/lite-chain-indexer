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
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.chain_registry import get_chain_config, list_supported_chains, ChainConfig
from lite_indexer.chains.demand import (
    default_enabled_events,
    get_chain_demand,
    read_demand_files,
    record_scan_demand,
)
from lite_indexer.chunks import chunk_size_for_chain, sync_batch_for_chain
from lite_indexer.config import (
    DATA_DIR,
    DISK_CAP_BYTES,
    GC_EVERY_N_CYCLES,
    LAZY_INDEXING_ENABLED,
    NATS_ENABLED,
    NDJSON_ENABLED,
    STATE_BACKWARD_PROTECTION,
    STATE_REGRESSION_TOLERANCE_BLOCKS,
    TRACE_ENABLED,
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
from lite_indexer.indexer._sync_loop import SyncLoopMixin
from lite_indexer.logging import get_logger
from lite_indexer.plugin import BasePlugin, PluginContext, PluginLoader
from lite_indexer.rpc import RpcClient, RpcFetchFailedError
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
    SyncLoopMixin,
):
    """Multi-chain blockchain indexer using the strategy pattern.

    Chain-specific logic (RPC calls, event parsing) is delegated to a
    strategy object created based on the chain's type:
      - evm    → EVMChainStrategy (eth_getLogs, DEX swaps, pair creation)
      - solana → SolanaStrategy (getSignaturesForAddress, token balance changes)
      - tron   → TronStrategy (TronGrid event API, TRC20 transfers)

    Cross-cutting concerns (disk management, state, NDJSON, ClickHouse)
    are handled here in the indexer core.

    The sync loop body is split into mixins:
      - RpcFetchMixin      — block/timestamp/transfer fetching
      - SyncLoopMixin      — main sync cycle (range, persist, state save)
      - MaintenanceMixin   — disk cap, GC, reconciliation
      - PluginsMixin       — plugin dispatch
      - SidecarsMixin      — NATS, webhooks, vector ingestion
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
        # representations via Qwen3-Embedding-4B, and stores them in LanceDB collections.
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

        # Chain configuration from registry (optional enrichment)
        self._chain_config: ChainConfig | None = get_chain_config(chain)

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
