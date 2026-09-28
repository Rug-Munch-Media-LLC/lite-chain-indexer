"""LiteIndexer mixin — sync loop body.

Extracted from the former god-file (indexer/__init__.py). Holds the main
sync cycle: range determination, per-slice checkpointing, event fetching,
persistence, state save, and the error/reorg guards. Mixed into LiteIndexer.

REORG_SENTINEL lives in the parent module (indexer/__init__.py) and is
referenced here as a bare name so the mixin works standalone in tests.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from lite_indexer import metrics
from lite_indexer.chunks import sync_batch_for_chain
from lite_indexer.config import (
    ATTESTATION_ENABLED,
    BACKFILL_ENABLED,
    DEX_SKIP_TO_HEAD,
    DEX_SKIP_TO_HEAD_HOURS,
    HEAD_BUFFER,
    REORG_PROTECTION_ENABLED,  # noqa: F401 — patched by tests
    RPC_WS_HEAD_ENABLED,
    STATE_BACKWARD_PROTECTION,
    STATE_REGRESSION_TOLERANCE_BLOCKS,
)
from lite_indexer.indexer._base import IndexerMixinBase
from lite_indexer.indexer._checkpoint import (
    HARD_ABORT_STATUSES,
    SOFT_ABORT_STATUSES,
    iter_slices,
    record_slice_checkpoint,
    slice_size,
)
from lite_indexer.indexer._dex_phase import note_dex_timeout, resolve_dex_events
from lite_indexer.indexer._phase_guard import PhaseTimeoutError, guarded_phase
from lite_indexer.indexer._reorg_phase import ReorgPhaseMixin
from lite_indexer.indexer._writers import (
    flush_solana_launch_bundles,
    flush_tron_launch,
    write_deployer_events,
    write_ndjson,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcFetchFailedError
from lite_indexer.storage.ch_write_pool import run_ch_write
from lite_indexer.strategies import EVMChainStrategy, SolanaStrategy, TronStrategy

logger = get_logger(__name__)

class SyncLoopMixin(ReorgPhaseMixin, IndexerMixinBase):
    """Sync loop body — extracted from the former god-file.

    Holds the main sync cycle: block range determination, event fetching,
    persistence, state save, and all the error/reorg guards.
    """

    # ── Reorg sentinel ─────────────────────────────────────────────────
    # Defined at module level in indexer/__init__.py; re-declared here so
    # the mixin works standalone in tests.
    _REORG_SENTINEL = "__CHAIN_REORG__"

    def _zero_write_tracker(self) -> Any:
        """Lazily build the per-chain zero-write stall tracker (RC-3)."""
        from lite_indexer.indexer._zero_write import tracker_for

        return tracker_for(self)

    def _defer_stalled_range(
        self, block_range: tuple[int, int], summary: dict[str, Any], start_time: float, phase: str
    ) -> dict[str, Any]:
        """RC-3 stall breaker: enqueue for backfill + advance state."""
        from lite_indexer.indexer._phase_guard import defer_stalled_range

        return defer_stalled_range(self, block_range, summary, start_time, phase)

    def _check_reorg(self, events: list[Any]) -> bool:
        """True when the strategy returned the reorg sentinel."""
        return bool(events and len(events) == 1 and isinstance(events[0], str) and events[0] == self._REORG_SENTINEL)

    # EVM WebSocket head pool: push the tip so the daemon starts a sync pass
    # on a new head early; HTTP polling stays the fallback/heartbeat.

    def _head_ws_endpoints(self) -> list[str]:
        """Overlay websocket head endpoints for this chain ([] = disabled)."""
        from lite_indexer.chains.endpoints import WS_HEAD_ENDPOINTS

        return list(WS_HEAD_ENDPOINTS.get(self.chain, []))

    def _on_head(self, block_number: int, block_hash: str) -> None:
        """Record a pushed head and wake the daemon's poll wait early."""
        if self._head_wakeup is not None:
            self._head_wakeup.set()
        logger.info("ws_head_received", chain=self.chain, block_number=block_number, block_hash=block_hash[:12],)

    async def start_head_pool(self) -> None:
        """Start the EVM webSocket head subscriber (no-op when unconfigured).

        Resolves WS endpoints from the private overlay only. No endpoints, a
        non-EVM chain, a disabled ``RPC_WS_HEAD_ENABLED`` or an already
        running pool all make this a no-op — HTTP polling continues
        unaffected.
        """
        if self._head_pool_task is not None or not self._is_evm() or not RPC_WS_HEAD_ENABLED:
            return
        endpoints = self._head_ws_endpoints()
        if not endpoints:
            return
        from lite_indexer.rpc.head_pool import subscribe_heads

        self._head_wakeup = asyncio.Event()
        self._head_pool_task = asyncio.create_task(
            subscribe_heads(self.chain, endpoints, self._on_head),
            name=f"ws-head-{self.chain}",
        )

    async def stop_head_pool(self) -> None:
        """Cancel the WS head subscriber (idempotent)."""
        task, self._head_pool_task = self._head_pool_task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def wait_for_head(self, timeout: float) -> None:
        """Sleep up to ``timeout`` seconds, waking early on a pushed head.

        The HTTP poll interval stays the heartbeat: when no WS pool is
        running this is exactly ``asyncio.sleep(timeout)``. A pushed head
        returns immediately so the daemon runs a sync pass through the
        normal memory gate + concurrency cap.
        """
        wakeup = self._head_wakeup
        if wakeup is None or timeout <= 0:
            await asyncio.sleep(max(0.0, timeout))
            return
        try:
            await asyncio.wait_for(wakeup.wait(), timeout=timeout)
        except TimeoutError:
            return
        wakeup.clear()

    # ── State regression guard ─────────────────────────────────────────

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
                metrics.incr_labeled("lite_indexer_state_regression_detected", chain=self.chain)
                self._highwater.update(resume - 1)
                return resume
            return None

        # No high-water mark: only trust CH when it is actually AHEAD.
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
            metrics.incr_labeled("lite_indexer_state_regression_detected", chain=self.chain)
            self._highwater.update(resume - 1)
            return resume

        return None

    # ── Block range determination ──────────────────────────────────────

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

        # Backward-state guard: a state file regressed below the high-water
        # mark or CH max is corruption — resume from CH max, not genesis.
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
        # Stash the head for staleness heuristics (DEX_SKIP_TO_HEAD).
        self._last_tip = tip

        if from_block >= to_block:
            logger.info("caught_up", chain=self.chain, block=tip)
            summary["status"] = "caught_up"
            return None

        cycle_cap = sync_batch_for_chain(self.chain, self.batch_size)
        if to_block - from_block > cycle_cap:
            to_block = from_block + cycle_cap

        return from_block, to_block

    def _dex_window_is_stale(self, from_block: int) -> bool:
        """True when the sync window start is deeper than DEX_SKIP_TO_HEAD_HOURS
        behind the chain head (approximated with the chain's average block
        time). Used by the env-gated DEX_SKIP_TO_HEAD skip.
        """
        tip = self._last_tip
        if tip is None:
            return False
        from lite_indexer.config import CHAIN_BLOCK_TIMES, DEFAULT_BLOCK_TIME

        avg_block_time = CHAIN_BLOCK_TIMES.get(self.chain, DEFAULT_BLOCK_TIME)
        behind_seconds = (tip - from_block) * avg_block_time
        return behind_seconds > DEX_SKIP_TO_HEAD_HOURS * 3600

    # ── Sync loop body ─────────────────────────────────────────────────

    async def _sync_loop_body(
        self,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any]:
        """Process the cycle's block range as checkpointed sub-slices.

        Each sub-slice persists state + high-water on completion inside
        ``_sync_range_body``, so a later phase timeout keeps every earlier
        slice instead of discarding the whole cycle's fetch.
        """
        block_range = await self._determine_block_range(summary)
        if block_range is None:
            self._save_current_state()
            return summary
        from_block, to_block = block_range
        slices_done = 0
        last_from, last_to = from_block, from_block - 1
        for slice_from, slice_to in iter_slices(from_block, to_block, slice_size()):
            result = await self._sync_range_body(slice_from, slice_to, summary, start_time)
            status = result.get("status")
            if status in HARD_ABORT_STATUSES:
                return result
            if status in SOFT_ABORT_STATUSES:
                # First slice failed → no advance (fail-closed). A later one
                # failed → earlier checkpoints are durable; stop here.
                if slices_done == 0:
                    return result
                record_slice_checkpoint(self, last_from, last_to, slices_done)
                result["status"] = "slice_partial"
                return result
            slices_done += 1
            last_from, last_to = slice_from, slice_to
        record_slice_checkpoint(self, last_from, last_to, slices_done)
        return summary

    async def _sync_range_body(
        self,
        from_block: int,
        to_block: int,
        summary: dict[str, Any],
        start_time: float,
    ) -> dict[str, Any]:
        """Sync one sub-slice (restore handled in finally)."""
        _timing = bool(os.getenv("INDEXER_PHASE_TIMING")) or (os.getenv("SOLANA_TIMING") and self.chain == "solana")  # phase timers
        _t_win = time.perf_counter()
        _t_fetch = _t_ch = _t_drains = _t_nats = _t_state = _t_side = 0.0
        # 2a. Graceful ClickHouse pause: probe CH BEFORE fetching events
        # from RPC. Unreachable CH aborts here with status "ch_unavailable"
        # — no RPC budget burned, no state movement (retry next poll).
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
            # Solana block catch-up needs headroom (200 slots × getBlock ≈ 40-90s).
            _t0 = time.perf_counter()
            events = await guarded_phase(
                "transfers",
                self.fetch_transfers(from_block, to_block),
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
            )
            _t_fetch = time.perf_counter() - _t0
        except (RpcFetchFailedError, PhaseTimeoutError) as exc:
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
            metrics.incr_labeled("lite_indexer_rpc_failures", chain=self.chain)
            summary["duration_seconds"] = time.monotonic() - start_time
            self._save_current_state()
            return summary

        if events is None:  # RC-1: deferred/failed fetch — never treat as "no logs"
            logger.warning("rpc_logs_deferred", chain=self.chain, from_block=from_block, to_block=to_block)
            metrics.incr_labeled("lite_indexer_rpc_failures", chain=self.chain)
            summary["status"] = "fetch_failed"
            summary["errors"].append(f"transfer fetch deferred for range {from_block}-{to_block}")
            summary["duration_seconds"] = time.monotonic() - start_time
            self._save_current_state()
            return summary

        if events:
            summary["events_fetched"] = len(events)

        # 3a. Handle chain reorg
        if self._check_reorg(events):
            logger.warning("chain_reorg_during_sync", chain=self.chain)
            self.start_from_head = True
            self._reorg_recovery = True
            summary["status"] = "reorg_detected"
            summary["duration_seconds"] = time.monotonic() - start_time
            self._save_current_state()
            return summary

        # 3b. Lazy indexing: apply top-chain defaults + scanner demand
        # signals before DEX/deployer decisions; flags are sticky.
        self._refresh_demand_enabled()

        # 4. Fetch DEX swap events (EVM only, if enabled)
        dex_events: list[dict[str, Any]] = []
        if self.dex_enabled and self.strategy.supports_dex():
            # DEX_SKIP_TO_HEAD (env-gated): when the sync window is deeply
            # stale, skip the dex fetch — frees RPC budget for catch-up; old
            # dex blocks stay a hole until backfilled, dex resumes at head.
            if DEX_SKIP_TO_HEAD and self._dex_window_is_stale(from_block):
                logger.info(
                    "dex_skip_stale_window",
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                )
                summary["dex_events_skipped"] = 1
            else:
                try:
                    dex_events = await guarded_phase(
                        "dex_events",
                        self.fetch_swap_events(from_block, to_block),
                        chain=self.chain,
                        from_block=from_block,
                        to_block=to_block,
                    )
                except PhaseTimeoutError as exc:
                    # Non-fatal: the transfers leg checkpointed this slice.
                    note_dex_timeout(summary, chain=self.chain, exc=exc)
                dex_events = resolve_dex_events(
                    dex_events, summary, chain=self.chain, from_block=from_block, to_block=to_block
                )
                if self._check_reorg(dex_events):
                    logger.warning("chain_reorg_during_dex_sync", chain=self.chain)
                    self.start_from_head = True
                    self._reorg_recovery = True
                    summary["status"] = "reorg_detected"
                    summary["duration_seconds"] = time.monotonic() - start_time
                    self._save_current_state()
                    return summary
                summary["dex_events_fetched"] = len(dex_events)

        # 4a. Pair-created events (EVM). Independent of the DEX flag — do NOT
        # re-add `and not self.dex_enabled` (caused a 2.9-day prod gap).
        if self.deployers_enabled and self.strategy.supports_deployers():
            try:
                await guarded_phase(
                    "pair_created",
                    self._fetch_pair_created_events(from_block, to_block),
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                )
            except PhaseTimeoutError as exc:
                # Non-fatal: transfers already checkpointed this slice.
                logger.warning("sync_phase_failed", chain=self.chain, phase="pair_created", error=str(exc)[:120])
                summary.setdefault("phase_failures", []).append("pair_created")
            except Exception as exc:
                logger.warning("deployer_pair_fetch_failed", chain=self.chain, error=str(exc)[:100])

        # 4b. Internal-transfer traces (EVM, optional): best-effort, runs
        # BEFORE the "no events" short-circuit (blocks can hold trace-only).
        trace_events: list[dict[str, Any]] = []
        if self.traces_enabled:
            try:
                trace_events = await guarded_phase(
                    "internal_transfers",
                    self._collect_internal_transfers(from_block, to_block),
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                )
            except PhaseTimeoutError as exc:
                # Non-fatal: transfers already checkpointed this slice.
                logger.warning("sync_phase_failed", chain=self.chain, phase="internal_transfers", error=str(exc)[:120])
                summary.setdefault("phase_failures", []).append("internal_transfers")
            if trace_events:
                summary["trace_events_fetched"] = len(trace_events)

        # Check if we have any events or deployer tracking. Solana swap
        # rows are buffered on the strategy during fetch_transfers, so they
        # must be counted here too or a swap-only batch would short-circuit.
        solana_swap_pending = bool(getattr(self.strategy, "_last_swap_events", None))
        if (
            not events
            and not dex_events
            and not solana_swap_pending
            and not self.deployers_enabled
            and not trace_events
        ):
            self._clear_reorg_recovery()
            self.state.save(to_block, 0, enabled_events=self._enabled_event_types())
            self._highwater.update(to_block)
            summary["status"] = "no_events"
            summary["duration_seconds"] = time.monotonic() - start_time
            return summary

        # 5. Enrich with timestamps (skip reorg sentinel)
        if events and not self._check_reorg(events):
            events = await self._enrich_timestamps(events)
        if dex_events and not self._check_reorg(dex_events):
            dex_events = await self._enrich_timestamps(dex_events)

        # 5a. Plugin dispatch (EVM, best-effort) — after timestamp enrichment,
        # failures contained per plugin.
        plugin_stats = await self._dispatch_plugin_events(from_block, to_block)
        if plugin_stats:
            summary.update(plugin_stats)

        # 5b. Write deployer events: the EVM path resolves the deployer EOA
        # via eth_getTransactionByHash (hence async); Solana events are
        # captured structurally by the strategy (creation tx fee payer).
        deployer_written = 0
        deployer_candidates = 0
        evm_strategy = self.strategy if isinstance(self.strategy, EVMChainStrategy) else None
        solana_strategy = self.strategy if isinstance(self.strategy, SolanaStrategy) else None
        tron_strategy = self.strategy if isinstance(self.strategy, TronStrategy) else None
        if self.deployers_enabled:
            # D6 pending-row count: distinguishes "nothing to write" from
            # "write failed" so state never advances over unwritten rows.
            if evm_strategy and evm_strategy._last_pair_events:
                deployer_candidates = len(evm_strategy._last_pair_events)
            elif solana_strategy and solana_strategy._last_deployer_events:
                deployer_candidates = len(solana_strategy._last_deployer_events)
            elif tron_strategy and tron_strategy._last_launch_events:
                # Tron deployer rows mirror the drained launch buffer (batch-1).
                deployer_candidates = len(tron_strategy._last_launch_events)
            _t0 = time.perf_counter()
            deployer_written = await write_deployer_events(
                self.chain,
                evm_strategy,
                solana_strategy,
                self.ch_writer,
                self._enrich_timestamps,
                nats=self.nats_publisher,
            )
            _t_drains += time.perf_counter() - _t0
            if deployer_written:
                summary["deployer_events_written"] = deployer_written

        # 5c. Write launch + Jito bundle events (Solana + Tron): persist to
        # ClickHouse + NATS, best-effort; buffers drain below.
        launch_candidates = 0
        if solana_strategy is not None:
            launch_candidates += len(getattr(solana_strategy, "_last_launch_events", None) or [])
            launch_candidates += len(getattr(solana_strategy, "_last_bundle_events", None) or [])
        if tron_strategy is not None:
            launch_candidates += len(getattr(tron_strategy, "_last_launch_events", None) or [])
        launch_written = 0
        if solana_strategy is not None:
            _t0 = time.perf_counter()
            launch_written += await flush_solana_launch_bundles(solana_strategy, self.ch_writer, self.nats_publisher)
            _t_drains += time.perf_counter() - _t0
        if tron_strategy is not None:
            tron_launch_written, tron_deployer_written = await flush_tron_launch(
                tron_strategy, self.ch_writer, self.nats_publisher
            )
            launch_written += tron_launch_written
            # Fold tron deployer rows into D6 pending-row accounting.
            deployer_written += tron_deployer_written
        if launch_written:
            summary["launch_events_written"] = launch_written

        # 6. Write NDJSON (gated; redundant with ClickHouse — when disabled,
        # recovery is restart-from-state: at most one cycle lost on crash).
        if self.ndjson_enabled and self._ndjson_writer is not None:
            # NDJSON write runs check_write_ok → check_disk_cap → a blocking
            # `du` subprocess; move it off the event loop so one chain's disk
            # check never stalls the other chains' RPC/state work.
            await asyncio.to_thread(write_ndjson, self._ndjson_writer, self.chain, events, dex_events, summary)

        # 7. Write ClickHouse via strategy
        # ch_available: ClickHouse confirmed writable this cycle (transfers
        # inserted OK, or nothing to write — the trace write probes then).
        ch_available = not events
        if events:
            # Offload the blocking ClickHouse insert to the bounded write pool
            # so one chain's insert never stalls the others' event loop.
            _t0 = time.perf_counter()
            ch_inserted = await run_ch_write(self.strategy.write_clickhouse, events, self.ch_writer)
            _t_ch += time.perf_counter() - _t0
            summary["events_written_ch"] = ch_inserted
            ch_available = ch_inserted > 0

            # Data-loss guard: events fetched but NOT written to ClickHouse
            # → do NOT advance state (observed 08/03: bsc/tron ran ahead of
            # the ClickHouse max when state advanced unconditionally).
            if ch_inserted == 0:
                # RC-3: 3 consecutive zero-writes for the same range → defer to backfill + advance.
                if self._zero_write_tracker().record_zero_write(from_block, to_block):
                    return self._defer_stalled_range((from_block, to_block), summary, start_time, "transfers")
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
            self._zero_write_tracker().record_success(from_block)

            # 7a. Wallet-access optimizations (address-first projections,
            # token metadata, balance cache, holder-balances MV) — only run
            # after the transfer insert succeeded so caches never diverge.
            wallet_stats = await self.ch_writer.write_wallet_features(self.chain, events)
            if wallet_stats:
                summary.update(wallet_stats)

        # 7-sol. Solana DEX swaps: parsed from the same getTransaction calls
        # as SPL balance changes and buffered on the strategy. Drain here so
        # they ride the shared dex_events write path (chain='solana') and
        # show up in the sync_complete dex_events count.
        solana_swaps: list[dict[str, Any]] = []
        if solana_strategy is not None and solana_strategy._last_swap_events:
            solana_swaps = list(solana_strategy._last_swap_events)
            solana_strategy._last_swap_events = []
            dex_events.extend(solana_swaps)
            summary["dex_events_fetched"] = summary.get("dex_events_fetched", 0) + len(solana_swaps)

        if dex_events:
            # Blocking dex insert + whale/price row-building → bounded pool.
            _t0 = time.perf_counter()
            dex_ch_inserted = await run_ch_write(self.ch_writer.write_dex_events, self.chain, dex_events)
            _t_ch += time.perf_counter() - _t0
            summary["dex_events_written_ch"] = dex_ch_inserted

            # Data-loss guard (mirrors step 7): dex rows are the scanner's
            # primary feed; a 0-row insert for a non-empty batch means the
            # table write failed wholesale — do NOT advance state.
            if dex_ch_inserted == 0:
                # RC-3: same stall breaker as the transfers gate.
                if self._zero_write_tracker().record_zero_write(from_block, to_block):
                    return self._defer_stalled_range((from_block, to_block), summary, start_time, "dex_events")
                logger.error(
                    "sync_aborted_dex_write_failed",
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                    dex_events=len(dex_events),
                )
                summary["status"] = "dex_write_failed"
                summary["duration_seconds"] = time.monotonic() - start_time
                self._save_current_state()
                return summary
            self._zero_write_tracker().record_success(from_block)

            if solana_swaps:
                logger.info("solana_dex_clickhouse_inserted", rows=dex_ch_inserted)

        # 7a2. Internal-transfer traces (best-effort enrichment — a write
        # failure never aborts; only after the transfer insert succeeded).
        if trace_events and ch_available:
            trace_written = await self.ch_writer.write_internal_transfers(self.chain, trace_events)
            if trace_written:
                summary["traces_written_ch"] = trace_written
        else:
            trace_written = 0

        # 7b. Reorg staging (EVM + Solana) — extracted to _reorg_phase.py.
        _reorg = await self._run_reorg_stage(from_block, to_block, summary, start_time)
        if _reorg is not None:
            return _reorg

        # 7c. NATS publishing (best-effort; failures swallowed + circuit-broken).
        nats_deployer_events: list[dict[str, Any]] = []
        if self.deployers_enabled:
            if evm_strategy and evm_strategy._last_pair_events:
                nats_deployer_events = list(evm_strategy._last_pair_events)
            elif solana_strategy and solana_strategy._last_deployer_events:
                nats_deployer_events = list(solana_strategy._last_deployer_events)
        _t0 = time.perf_counter()
        nats_published = await self._publish_nats_events(
            self.chain,
            to_block,
            events,
            dex_events,
            nats_deployer_events,
            trace_events,
        )
        _t_nats = time.perf_counter() - _t0
        if nats_published:
            summary["nats_events_published"] = nats_published

        # 7d. Webhook fan-out (best-effort, process-wide, lazy, idempotent).
        await self._maybe_start_webhook_delivery()

        # 7e. Writer completeness gate (D6): an enabled best-effort writer
        # with pending rows and zero writes blocks the advance (retried).
        failed_writers: list[str] = []
        if self.deployers_enabled and deployer_candidates and deployer_written == 0:
            failed_writers.append("deployer")
        if launch_candidates and launch_written == 0:
            failed_writers.append("launch")
        if trace_events and ch_available and trace_written == 0:
            failed_writers.append("trace")
        if failed_writers:
            for _kind in failed_writers:
                metrics.incr_labeled("lite_indexer_writer_gap", chain=self.chain)
            # RC-3: same stall breaker for the best-effort writers gate.
            if self._zero_write_tracker().record_zero_write(from_block, to_block):
                return self._defer_stalled_range((from_block, to_block), summary, start_time, ",".join(failed_writers))
            logger.error(
                "sync_aborted_writer_incomplete",
                chain=self.chain,
                from_block=from_block,
                to_block=to_block,
                failed_writers=failed_writers,
            )
            summary["status"] = "writer_incomplete"
            summary["failed_writers"] = failed_writers
            summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
            self._save_current_state()
            return summary
        if deployer_written or launch_written or trace_written:
            self._zero_write_tracker().record_success(from_block)

        # 8. Save state
        total_events = len(events) + len(dex_events) + deployer_written + len(trace_events)
        self._clear_reorg_recovery()
        _t0 = time.perf_counter()
        self.state.save(to_block, total_events, enabled_events=self._enabled_event_types())
        _t_state = time.perf_counter() - _t0

        # 8-attest. Merkle-leaf attestation of the committed range (best-effort).
        if ATTESTATION_ENABLED and (events or dex_events):
            try:
                from lite_indexer.attestation import collect_block_hashes, record_range_attestation

                attest_rows = events + dex_events
                block_hashes = collect_block_hashes(events, dex_events)
                root = await asyncio.to_thread(
                    record_range_attestation,
                    self.chain,
                    from_block,
                    to_block,
                    attest_rows,
                    block_hashes,
                )
                if root:
                    summary["attestation_root"] = root
            except Exception as exc:  # noqa: BLE001 — attestation is best-effort
                logger.warning("attestation_failed", chain=self.chain, error=str(exc)[:120])

        # Monotonic high-water mark — only ever moves forward. Feeds the
        # backward-state guard on the next load.
        self._highwater.update(to_block)

        # 8b. Periodic reconciliation: a >5% CH/RPC row shortfall rewinds
        # state so the next cycle backfills. Best-effort, never aborts.
        self._maybe_reconcile_range(from_block, to_block, len(events), summary)

        # 8b2. Backfill sidecar: enqueue missed ranges, drain a bounded
        # number through keyed endpoints. Memory-gated; never blocks.
        if BACKFILL_ENABLED:
            self._enqueue_backfill_gaps(self._last_tip)
            _t0 = time.perf_counter()
            backfill_stats = await self._run_backfill_sidecar()
            _t_side += time.perf_counter() - _t0
            if backfill_stats and backfill_stats.get("dispatched"):
                summary["backfill"] = backfill_stats

        # 8a. Vector ingestion (optional LanceDB, gated + interval-limited;
        # exception-guarded — CH is the durable store, never blocked).
        _t0 = time.perf_counter()
        await self._run_vector_ingestion_guarded(from_block, to_block, summary)
        _t_side += time.perf_counter() - _t0

        summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
        if _timing:  # single per-window phase budget line
            logger.info(
                "solana_window_phases",
                from_block=from_block,
                to_block=to_block,
                total_s=round(time.perf_counter() - _t_win, 2),
                fetch_s=round(_t_fetch, 2),
                ch_write_s=round(_t_ch, 2),
                drains_s=round(_t_drains, 2),
                nats_s=round(_t_nats, 2),
                state_s=round(_t_state, 2),
                sidecars_s=round(_t_side, 2),
            )
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
