"""LiteIndexer mixin — sync loop body.

Extracted from the former god-file (indexer/__init__.py) to bring the core
class under 500 lines. Holds the main sync cycle: block range determination,
event fetching, persistence, state save, and all the error/reorg guards.
Mixed into LiteIndexer.

REORG_SENTINEL lives in the parent module (indexer/__init__.py) and is
referenced here as a bare name so the mixin works standalone in tests.
"""

from __future__ import annotations

import asyncio
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
    REORG_PROTECTION_ENABLED,
    RPC_WS_HEAD_ENABLED,
    STATE_BACKWARD_PROTECTION,
    STATE_REGRESSION_TOLERANCE_BLOCKS,
)
from lite_indexer.indexer._base import IndexerMixinBase
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
from lite_indexer.strategies.reorg_status import ReorgStatus

logger = get_logger(__name__)


class SyncLoopMixin(IndexerMixinBase):
    """Sync loop body — extracted from the former god-file.

    Holds the main sync cycle: block range determination, event fetching,
    persistence, state save, and all the error/reorg guards.
    """

    # ── Reorg sentinel ─────────────────────────────────────────────────
    # Defined at module level in indexer/__init__.py; re-declared here so
    # the mixin works standalone in tests.
    _REORG_SENTINEL = "__CHAIN_REORG__"

    def _check_reorg(self, events: list[Any]) -> bool:
        """Check if the events list contains a chain reorg sentinel.

        Strategies return ["__CHAIN_REORG__"] when from_block > to_block,
        indicating the chain experienced a reorganization.

        Args:
            events: Events list returned by a strategy method.

        Returns:
            True if a reorg was detected.
        """
        return bool(events and len(events) == 1 and isinstance(events[0], str) and events[0] == self._REORG_SENTINEL)

    # ── EVM WebSocket head pool ────────────────────────────────────────
    # Push the tip so the daemon can start a sync pass as soon as a new head
    # lands, instead of waiting out the whole HTTP poll interval. HTTP
    # polling remains the fallback/heartbeat: with no overlay WS endpoint
    # (or after the pool exhausts every endpoint) this is a plain sleep.
    # Every pass still goes through the daemon's memory gate + concurrency
    # cap and the normal RPC throttles — the pool only shortens the wait.

    def _head_ws_endpoints(self) -> list[str]:
        """Overlay websocket head endpoints for this chain ([] = disabled)."""
        from lite_indexer.chains.endpoints import WS_HEAD_ENDPOINTS

        return list(WS_HEAD_ENDPOINTS.get(self.chain, []))

    def _on_head(self, block_number: int, block_hash: str) -> None:
        """Record a pushed head and wake the daemon's poll wait early."""
        if self._head_wakeup is not None:
            self._head_wakeup.set()
        logger.info(
            "ws_head_received",
            chain=self.chain,
            block_number=block_number,
            block_hash=block_hash[:12],
        )

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
            # 2026-09-01: solana block-based catch-up needs more headroom (200 slots
            # x getBlock at concurrency 10 ≈ 40-90s). 300s covers it.
            events = await asyncio.wait_for(self.fetch_transfers(from_block, to_block), timeout=300.0)
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
            metrics.incr_labeled("lite_indexer_rpc_failures", chain=self.chain)
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
        # signals before deciding whether to fetch DEX/deployer events.
        # Runs after transfers so demand files touched this cycle (by the
        # rmi-backend scanner) are observed. Flags are sticky once enabled.
        self._refresh_demand_enabled()

        # 4. Fetch DEX swap events (EVM only, if enabled)
        dex_events: list[dict[str, Any]] = []
        if self.dex_enabled and self.strategy.supports_dex():
            # DEX_SKIP_TO_HEAD (env-gated, default off): when the sync
            # window is deeply stale (older than DEX_SKIP_TO_HEAD_HOURS
            # behind the chain head), skip the dex fetch for this cycle —
            # the dex getLogs calls only serve old blocks no scanner will
            # read, and dropping them frees RPC budget for the transfer
            # catch-up. Dex resumes automatically once transfers reach
            # fresh blocks. Old dex blocks stay a hole until backfilled.
            if DEX_SKIP_TO_HEAD and self._dex_window_is_stale(from_block):
                logger.info(
                    "dex_skip_stale_window",
                    chain=self.chain,
                    from_block=from_block,
                    to_block=to_block,
                )
                summary["dex_events_skipped"] = 1
            else:
                dex_events = await self.fetch_swap_events(from_block, to_block)
                if self._check_reorg(dex_events):
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
            # Offload the blocking ClickHouse insert + row serialization +
            # address-activity row-building to the bounded write pool: a
            # chain's insert must not stall the other chains' event loop
            # (2026-09-13 profile: ~53% of GIL samples here).
            ch_inserted = await run_ch_write(self.strategy.write_clickhouse, events, self.ch_writer)
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
            dex_ch_inserted = await run_ch_write(self.ch_writer.write_dex_events, self.chain, dex_events)
            summary["dex_events_written_ch"] = dex_ch_inserted

            # Data-loss guard (mirrors the transfer guard at step 7): dex
            # events were fetched but NONE landed in ClickHouse. Do NOT
            # advance state — abort the cycle so the range is re-fetched on
            # the next cycle instead of silently skipping blocks whose dex
            # rows never persisted. Dex rows are the scanner's primary
            # feed; a 0-row insert for a non-empty batch means the table
            # write failed wholesale.
            if dex_ch_inserted == 0:
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

            if solana_swaps:
                logger.info("solana_dex_clickhouse_inserted", rows=dex_ch_inserted)

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

        # 7b. Reorg staging (EVM + Solana, gated by REORG_PROTECTION_ENABLED):
        # checkpoint the batch tip block hash/slot blockhash, then verify
        # the block/slot at tip - depth against its checkpoint. On
        # mismatch the chain reorganized underneath us: roll back
        # evm_transfers/solana_spl_balance_changes + dex_events above the
        # fork point and re-fetch from the fork on the next cycle (state
        # is rewound to the fork).
        if REORG_PROTECTION_ENABLED and isinstance(self.strategy, (EVMChainStrategy, SolanaStrategy)):
            try:
                ch_client = self.ch_writer.client()
            except Exception as exc:  # noqa: BLE001 — staging is best-effort
                ch_client = None
                logger.debug("reorg_ch_client_failed", chain=self.chain, error=str(exc)[:80])

            if ch_client is not None:
                outcome = await self.strategy.reorg_stage(from_block, to_block, ch_client)
                if outcome.status is ReorgStatus.UNKNOWN:
                    # Fail CLOSED: we could not verify the range (checkpoint
                    # lookup / RPC / rollback failure). Do NOT advance state
                    # past it — the next cycle re-fetches the same range and
                    # re-checks instead of persisting orphaned-chain data as
                    # canonical.
                    logger.error(
                        "reorg_status_unknown",
                        chain=self.chain,
                        block=outcome.fork_block if outcome.fork_block is not None else to_block,
                        error=outcome.error,
                    )
                    metrics.incr_labeled("lite_indexer_reorg_unknown", chain=self.chain)
                    summary["status"] = "reorg_status_unknown"
                    summary["errors"].append(f"reorg status unknown for range {from_block}-{to_block}: {outcome.error}")
                    summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
                    self._save_current_state()
                    return summary
                if outcome.status is ReorgStatus.REORG and outcome.fork_block is not None:
                    logger.warning(
                        "reorg_rollback_scheduled",
                        chain=self.chain,
                        fork_block=outcome.fork_block,
                    )
                    summary["status"] = "reorg_rollback"
                    summary["reorg_fork_block"] = outcome.fork_block
                    # Re-fetch from fork: next cycle starts at fork_block.
                    self.state.save(outcome.fork_block - 1, 0, enabled_events=self._enabled_event_types())
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

        # 8-attest. On-chain-verifiable attestation of this committed range:
        # append a Merkle leaf over the range's block hashes + normalized row
        # digest to the persisted accumulator. Best-effort and off-loop
        # (hashing a batch is CPU work); a failure logs and continues — the
        # write path is never blocked or aborted by attestation.
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

        # 8b. Periodic data reconciliation: every N cycles compare the
        # ClickHouse row count for this range against the events fetched
        # from RPC; a >5% shortfall rewinds state so the next cycle
        # backfills the range. Best-effort — never blocks or aborts.
        self._maybe_reconcile_range(from_block, to_block, len(events), summary)

        # 8b2. Freemium backfill sidecar: enqueue ranges we did not index
        # ourselves (stalled-chain backlog, reconciliation rewinds), then
        # drain a bounded number of chunks through keyed/freemium
        # endpoints only. Memory-gated; never blocks the sync cycle.
        if BACKFILL_ENABLED:
            self._enqueue_backfill_gaps(self._last_tip)
            backfill_stats = await self._run_backfill_sidecar()
            if backfill_stats and backfill_stats.get("dispatched"):
                summary["backfill"] = backfill_stats

        # 8a. Vector ingestion (optional, LanceDB). Reads the freshly synced
        # block range from ClickHouse, generates text representations for
        # wallets/contracts, embeds them via Qwen3-Embedding-4B, and
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
