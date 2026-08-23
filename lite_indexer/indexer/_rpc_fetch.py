"""LiteIndexer mixins — extracted from the former god-file (indexer/__init__.py).

Degod split 2026-08-23: each mixin holds one cohesive responsibility and is
mixed into LiteIndexer. Method names/behavior unchanged — tests validate.
"""

from __future__ import annotations

import gc  # noqa: F401 — used by maintenance mixin body
from typing import Any  # noqa: F401

from lite_indexer.strategies.evm.strategy import EVMChainStrategy
from lite_indexer.config import TRACE_MAX_BLOCKS_PER_CYCLE
from lite_indexer.logging import get_logger

log = get_logger(__name__)
logger = log  # god-file bodies reference `logger`; keep both names

class RpcFetchMixin:
    'Chain RPC fetching: blocks, transfers, swaps, timestamp enrichment.'


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

    # ── Internal-Transfer Traces (EVM only, optional) ───────────────

    async def _collect_internal_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Collect internal transfers for the sync range via free trace RPCs.

        The #1 analytical blind spot: WETH unwraps, bridge deposits, fee
        flows and SELFDESTRUCT payouts never emit ERC-20 Transfer logs.
        When ``traces_enabled`` and the chain has a verified trace
        endpoint, this sweeps the batch range with a bounded concurrency
        and returns standardized internal-transfer rows.

        Best-effort by design: trace RPCs are rate-limited on free tiers,
        so a total trace failure degrades to an empty list + a warning and
        never blocks the main transfer sync (the trace table is
        enrichment, not the durable transfer record).

        Args:
            from_block: First block of the sync range.
            to_block: Last block of the sync range.

        Returns:
            Standardized internal-transfer rows (possibly empty).
        """
        from lite_indexer.traces import has_trace_endpoints
        from lite_indexer.traces.trace_collector import TraceCollector

        if not self._is_evm() or not has_trace_endpoints(self.chain):
            return []

        max_blocks = min(TRACE_MAX_BLOCKS_PER_CYCLE, to_block - from_block + 1)
        if max_blocks <= 0:
            return []
        # Sweep from the range head (most recent) — oldest blocks get
        # covered on later cycles when the range advances.
        start = to_block - max_blocks + 1
        collector = TraceCollector(self.chain)
        try:
            return await collector.collect_range(
                self.chain,
                start,
                to_block,
                concurrency=TRACE_CONCURRENCY,
                max_blocks=max_blocks,
            )
        except Exception as exc:  # noqa: BLE001 — trace collection is best-effort
            logger.warning(
                "trace_collection_failed",
                chain=self.chain,
                from_block=start,
                to_block=to_block,
                error=str(exc)[:120],
            )
            return []

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

        block_numbers = {int(bn) for e in events if (bn := e.get("blockNumber")) is not None}
        if not block_numbers:
            return events

        timestamps = await self.fetch_block_timestamps(block_numbers)

        for event in events:
            bn = event.get("blockNumber")
            if bn and bn in timestamps:
                event["blockTimestamp"] = timestamps[bn]
            elif not event.get("blockTimestamp"):
                event["blockTimestamp"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
