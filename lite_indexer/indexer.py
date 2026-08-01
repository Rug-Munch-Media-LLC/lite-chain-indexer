"""
LiteIndexer — Core indexing engine.

Orchestrates the complete index cycle using the strategy pattern:
  1. Determine block range (from state or override)
  2. Fetch Transfer events via chain-specific strategy
  3. Fetch DEX Swap events (EVM only, if enabled)
  4. Fetch PairCreated/PoolCreated events (EVM only, if enabled)
  5. Enrich with block timestamps
  6. Write NDJSON + ClickHouse (if configured)
  7. Save state

Each chain gets its own LiteIndexer instance with independent state tracking.
Non-EVM chains (Solana, Tron) use their own strategy with chain-specific RPC.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lite_indexer.config import DATA_DIR, DISK_CAP_BYTES, HEAD_BUFFER
from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcClient
from lite_indexer.rpc.verify import (
    EndpointVerifyResult,
    apply_verified_endpoints,
    restore_original_endpoints,
    verify_chain_endpoints,
)
from lite_indexer.state import StateManager
from lite_indexer.storage import (
    ClickHouseWriter,
    NDJSONWriter,
    check_disk_cap,
    check_disk_space,
)
from lite_indexer.strategies import (
    SUPPORTED_CHAINS,
    EVMChainStrategy,
    create_strategy,
)
from lite_indexer.strategies.base import AbstractChainStrategy

logger = get_logger(__name__)

# Sentinel value returned by strategies when a chain reorg is detected
_REORG_SENTINEL = "__CHAIN_REORG__"


def _check_reorg(events: list) -> bool:
    """Check if the events list contains a chain reorg sentinel.

    Strategies return ["__CHAIN_REORG__"] when from_block > to_block,
    indicating the chain experienced a reorganization.

    Args:
        events: Events list returned by a strategy method.

    Returns:
        True if a reorg was detected.
    """
    return bool(events and len(events) == 1 and isinstance(events[0], str) and events[0] == _REORG_SENTINEL)


class LiteIndexer:
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
    ) -> None:
        self.chain = chain
        self.batch_size = batch_size
        self.disk_cap = disk_cap
        self.dex_enabled = False
        self.deployers_enabled = False
        self.start_from_head = start_from_head
        self.data_dir = data_dir or DATA_DIR

        # Chain type detection
        chain_info = SUPPORTED_CHAINS.get(chain, {"type": "evm"})
        self._chain_type: str = chain_info.get("type", "evm")

        # State manager (generic — works for all chain types)
        self.state = StateManager(chain)

        # Optional override blocks (for testing / backfill)
        self._override_from = from_block
        self._override_to = to_block

        # RPC client (used by EVM strategy, others create their own)
        self.rpc = RpcClient()

        # Latest endpoint verification result (EVM only) — restored after sync
        self._verify_result: EndpointVerifyResult | None = None

        # Storage writers
        self._ndjson_writer = NDJSONWriter(data_dir=self.data_dir)
        self._ch_writer: ClickHouseWriter | None = None

        # Strategy — chain-specific logic
        self._strategy: AbstractChainStrategy | None = None

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
            )
        return self._strategy

    # ── Helpers ──────────────────────────────────────────────────────

    def _is_evm(self) -> bool:
        """Check if this chain uses EVM strategy."""
        return self._chain_type == "evm"

    @property
    def ch_writer(self) -> ClickHouseWriter:
        """Lazy-initialized ClickHouse writer."""
        if self._ch_writer is None:
            self._ch_writer = ClickHouseWriter()
        return self._ch_writer

    # ── RPC: Latest Block ───────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get the latest block number / slot from the chain strategy."""
        return await self.strategy.fetch_block_number()

    # ── RPC: Block Timestamps ───────────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch timestamps for a set of block numbers.

        Delegates to the chain strategy's timestamp fetcher.
        Each strategy handles this differently:
          - EVM: eth_getBlockByNumber
          - Solana: getBlock with blockTime
          - Tron: block timestamps from block_header
        """
        return await self.strategy.fetch_block_timestamps(block_numbers)

    # ── RPC: Transfer Events ────────────────────────────────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch transfer events via the chain-specific strategy.

        Each chain type fetches events differently:
          - EVM: eth_getLogs with Transfer event topic
          - Solana: getSignaturesForAddress + getTransaction (token balances)
          - Tron: TronGrid /v1/contracts/{address}/events (TRC20)
        """
        return await self.strategy.fetch_transfers(from_block, to_block)

    # ── RPC: Swap Events (EVM only) ─────────────────────────────────

    async def fetch_swap_events(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch DEX Swap events (V2 + V3) via the EVM strategy.

        Only works for EVM chains. Delegates to EVMChainStrategy which
        fetches PairCreated events first, then V2/V3 swap topics.
        Returns empty list for non-EVM chains.
        """
        if not self._is_evm():
            return []
        if not isinstance(self.strategy, EVMChainStrategy):
            return []
        return await self.strategy.fetch_swap_events(from_block, to_block)

    # ── PairCreated Fetch (EVM only, internal) ──────────────────────

    async def _fetch_pair_created_events(
        self,
        from_block: int,
        to_block: int,
    ) -> None:
        """Fetch PairCreated/PoolCreated events via the EVM strategy.

        Only works for EVM chains. Builds pair→(token0, token1) mapping.
        """
        if not self._is_evm():
            return
        if not isinstance(self.strategy, EVMChainStrategy):
            return
        await self.strategy._fetch_pair_created_events(from_block, to_block)

    # ── Timestamp Enrichment ────────────────────────────────────────

    async def _enrich_timestamps(
        self,
        events: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Fetch block timestamps and attach to events.

        For Solana events (spl_balance_change), timestamps come from
        the transaction blockTime field which is already populated.
        For EVM/Tron, timestamps are fetched via RPC.

        Skips reorg sentinel values (string markers).
        """
        if not events:
            return events

        # Skip reorg sentinel or non-dict events
        events = [e for e in events if isinstance(e, dict)]

        if not events:
            return events

        # Solana events already have blockTimestamp from tx blockTime
        if self._chain_type == "solana":
            return events

        block_numbers = {e.get("blockNumber") for e in events if e.get("blockNumber") is not None}
        if not block_numbers:
            return events

        timestamps = await self.fetch_block_timestamps(block_numbers)

        for event in events:
            bn = event.get("blockNumber")
            if bn and bn in timestamps:
                event["blockTimestamp"] = timestamps[bn]
            elif not event.get("blockTimestamp"):
                event["blockTimestamp"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        return events

    # ── Block Range Determination ──────────────────────────────────

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

        if to_block - from_block > self.batch_size:
            to_block = from_block + self.batch_size

        return from_block, to_block

    # ── Main Sync Loop ─────────────────────────────────────────────

    async def sync_loop(self) -> dict[str, Any]:
        """Main sync loop: load state → fetch → persist → save.

        Returns summary dict with stats.
        """
        start_time = time.monotonic()
        summary: dict[str, Any] = {
            "chain": self.chain,
            "status": "ok",
            "events_fetched": 0,
            "events_written_ndjson": 0,
            "events_written_ch": 0,
            "bytes_written": 0,
            "duration_seconds": 0,
            "errors": [],
        }

        # 1. Disk cap + disk space checks (generic — all chains)
        if not check_disk_cap(self.data_dir, self.disk_cap):
            summary["status"] = "disk_cap_exceeded"
            summary["errors"].append("disk cap exceeded")
            return summary

        if not check_disk_space():
            summary["status"] = "disk_full"
            summary["errors"].append("disk space critically low")
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
            return summary

        summary["endpoints_total"] = result.total
        summary["endpoints_verified"] = result.verified

        # Temporarily use verified endpoints for this sync cycle
        self._verify_result = result
        apply_verified_endpoints(result)
        return None  # Proceed to sync

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
        """
        # Restore EVM endpoints after verification (if applicable)
        try:
            if self._is_evm() and self._verify_result is not None:
                restore_original_endpoints(self._verify_result)
        except Exception:
            pass

        # 2. Determine block range
        block_range = await self._determine_block_range(summary)
        if block_range is None:
            return summary
        from_block, to_block = block_range

        # 3. Fetch transfer events via strategy
        logger.info("sync_start", chain=self.chain, from_block=from_block, to_block=to_block)

        events = await self.fetch_transfers(from_block, to_block)
        if events:
            summary["events_fetched"] = len(events)

        # 3a. Handle chain reorg
        if _check_reorg(events):
            logger.warning("chain_reorg_during_sync", chain=self.chain)
            self.start_from_head = True
            summary["status"] = "reorg_detected"
            summary["duration_seconds"] = time.monotonic() - start_time
            return summary

        # 4. Fetch DEX swap events (EVM only, if enabled)
        dex_events: list[dict[str, Any]] = []
        if self.dex_enabled and self.strategy.supports_dex():
            dex_events = await self.fetch_swap_events(from_block, to_block)
            if _check_reorg(dex_events):
                logger.warning("chain_reorg_during_dex_sync", chain=self.chain)
                self.start_from_head = True
                summary["status"] = "reorg_detected"
                summary["duration_seconds"] = time.monotonic() - start_time
                return summary
            summary["dex_events_fetched"] = len(dex_events)

        # 4a. Fetch pair created events for deployer tracking (EVM only).
        # Runs even when DEX is enabled — dex_handler fetches pair-created
        # events to build its token cache, but deployer events are only
        # written from the strategy's _last_pair_events, which is populated
        # here.
        if self.deployers_enabled and self.strategy.supports_deployers():
            try:
                await self._fetch_pair_created_events(from_block, to_block)
            except Exception as exc:
                logger.warning("deployer_pair_fetch_failed", chain=self.chain, error=str(exc)[:100])

        # Check if we have any events or deployer tracking
        if not events and not dex_events and not self.deployers_enabled:
            self.state.save(to_block, 0)
            summary["status"] = "no_events"
            summary["duration_seconds"] = time.monotonic() - start_time
            return summary

        # 5. Enrich with timestamps (skip reorg sentinel)
        if events and not _check_reorg(events):
            events = await self._enrich_timestamps(events)
        if dex_events and not _check_reorg(dex_events):
            dex_events = await self._enrich_timestamps(dex_events)

        # 5b. Write deployer events (EVM only)
        deployer_written = 0
        evm_strategy = self.strategy if isinstance(self.strategy, EVMChainStrategy) else None
        if self.deployers_enabled and evm_strategy and evm_strategy._last_pair_events:
            evm_strategy._last_pair_events = await self._enrich_timestamps(evm_strategy._last_pair_events)
            deployer_written = self.ch_writer.write_deployer_events(self.chain, evm_strategy._last_pair_events)
            if deployer_written:
                summary["deployer_events_written"] = deployer_written

        # 6. Write NDJSON
        bytes_written = 0
        if events:
            bytes_written = self._ndjson_writer.write_events(self.chain, events)
            if bytes_written:
                summary["events_written_ndjson"] = len(events)
                summary["bytes_written"] = bytes_written

        if dex_events:
            dex_bytes = self._ndjson_writer.write_dex_events(self.chain, dex_events)
            if dex_bytes:
                summary["dex_events_written_ndjson"] = len(dex_events)
                summary["dex_bytes_written"] = dex_bytes

        # 7. Write ClickHouse via strategy
        if events:
            ch_inserted = self.strategy.write_clickhouse(events, self.ch_writer)
            summary["events_written_ch"] = ch_inserted

        if dex_events:
            dex_ch_inserted = self.ch_writer.write_dex_events(self.chain, dex_events)
            summary["dex_events_written_ch"] = dex_ch_inserted

        # 8. Save state
        total_events = len(events) + len(dex_events) + deployer_written
        self.state.save(to_block, total_events)

        # A reorg-triggered head start (or --start-from-head) is one-shot:
        # clear the flag after a successful sync so subsequent cycles
        # resume from saved state instead of re-indexing from the tip.
        self.start_from_head = False

        summary["duration_seconds"] = round(time.monotonic() - start_time, 2)
        logger.info(
            "sync_complete",
            chain=self.chain,
            transfer_events=len(events),
            dex_events=len(dex_events),
            deployer_events=deployer_written,
            duration_s=summary["duration_seconds"],
        )

        return summary
