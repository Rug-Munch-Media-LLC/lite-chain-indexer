"""
EVMChainStrategy — orchestrator for EVM chain indexing.

Wires together the transfer, DEX, and deployer handler modules into
a unified strategy class that conforms to AbstractChainStrategy.

Manages:
- HTTP client lifecycle
- RPC endpoint verification
- Block number and timestamp fetching
- Delegation to handler modules for events
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import httpx

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    BATCH_SIZE,
    BLOOM_ENABLED,
    CHUNK_SIZE_EVM,
    REORG_PROTECTION_ENABLED,
    RPC_TIMEOUT,
)
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    get_best_endpoint,
    record_endpoint_failure,
    verify_endpoints_for_chain,
)
from lite_indexer.rpc import (
    fetch_block_timestamps as rpc_fetch_block_timestamps,
)
from lite_indexer.state import PairCache
from lite_indexer.storage.address_activity import parse_evm_address_activity
from lite_indexer.strategies.address_activity import CrossChainAddressActivityMixin
from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.bloom import BloomCache
from lite_indexer.strategies.evm.deployer_handler import fetch_pair_created as _fetch_pair_created
from lite_indexer.strategies.evm.dex_handler import fetch_swap_events as _fetch_swap_events
from lite_indexer.strategies.evm.reorg import (
    check_reorg,
    checkpoint_block,
    ensure_checkpoint_table,
    reorg_depth,
    rollback_to_block,
)
from lite_indexer.strategies.evm.transfer_handler import fetch_transfers as _fetch_transfers
from lite_indexer.strategies.reorg_status import ReorgCheck, ReorgStatus
from lite_indexer.util.http import http_limits

logger = get_logger(__name__)


class EVMChainStrategy(CrossChainAddressActivityMixin, AbstractChainStrategy):
    """Strategy for EVM-compatible chains using eth_getLogs.

    Indexes ERC-20 Transfer events, DEX Swap events (V2+V3), and
    PairCreated/PoolCreated events. This is the original LiteIndexer
    logic refactored into the strategy + handler pattern.
    """

    def __init__(
        self,
        chain: str,
        rpc: RpcClient | None = None,
        batch_size: int = BATCH_SIZE,
        pair_cache: PairCache | None = None,
        chunk_size: int | None = None,
        bloom_cache: BloomCache | None = None,
    ) -> None:
        super().__init__(chain)
        self.rpc = rpc or RpcClient()
        self.batch_size = batch_size
        # Chunk-based indexing: split ranges into chunk_size-wide chunks
        # fetched in parallel. Defaults to the chain-agnostic EVM chunk.
        self.chunk_size = chunk_size if chunk_size and chunk_size > 0 else CHUNK_SIZE_EVM
        self._pair_cache = pair_cache or PairCache(chain)

        # Bloom pre-filter cache (TrueBlocks approach) for address-indexed
        # queries. Created lazily when bloom filtering is enabled.
        self._bloom_cache = bloom_cache if bloom_cache is not None else (BloomCache() if BLOOM_ENABLED else None)

        # Latest batch of pair creation events for deployer tracking
        self._last_pair_events: list[dict[str, Any]] = []

        # HTTP client for eth_getLogs batch calls
        self._client: httpx.AsyncClient | None = None

        # Address-activity keystone rows (wallet_memory.address_activity):
        # one row per address touched by every EVM event this strategy
        # fetches (zero extra RPC). Buffered here and drained in
        # write_clickhouse (see CrossChainAddressActivityMixin).
        self._init_address_activity()

    # ── Address-Activity Keystone (EVM) ──────────────────────────────

    def _parse_address_activity_event(self, event: dict[str, Any]) -> list[dict[str, Any]]:
        """Expand one parsed EVM event into address-activity rows."""
        return parse_evm_address_activity(event)

    def write_clickhouse(self, events: list[dict[str, Any]], ch_writer: Any) -> int:
        """Write EVM transfers, then buffer + drain address-activity rows.

        Mirrors the Solana write path: the address rows are derived from
        the same events already fetched (no extra RPC) and drained in a
        best-effort step that never affects the transfer write result.
        """
        written = super().write_clickhouse(events, ch_writer)
        self._ingest_address_activity_events(events)
        client = ch_writer._connect()
        if client is not None:
            self._drain_address_activity_rows(client)
        return written

    @property
    def bloom_cache(self) -> BloomCache | None:
        """The bloom cache used for address pre-filtering (may be None)."""
        return self._bloom_cache

    @property
    def pair_cache(self) -> PairCache:
        """The persistent pair→(token0, token1) cache for this chain."""
        return self._pair_cache

    # ── Block Range Enforcement ──────────────────────────────────────

    def enforce_block_range(self, from_block: int, to_block: int) -> tuple[int, int]:
        """Apply chain-specific block range limits + batch size cap.

        Delegates to the module-level enforce_block_range helper.

        Args:
            from_block: Requested start block.
            to_block: Requested end block.

        Returns:
            (from_block, capped_to_block) tuple.
        """
        return enforce_block_range(self.chain, from_block, to_block, self.batch_size)

    def detect_reorg(self, from_block: int, to_block: int) -> bool:
        """Detect chain reorgs where block number goes backwards.

        Args:
            from_block: Previously indexed block.
            to_block: Current chain tip.

        Returns:
            True if a reorg was detected.
        """
        return detect_reorg(from_block, to_block)

    # ── Feature Flags ────────────────────────────────────────────────

    def supports_dex(self) -> bool:
        """Whether this chain indexes DEX swap events (always True for EVM)."""
        return True

    def supports_deployers(self) -> bool:
        """Whether this chain tracks factory deployers (always True for EVM)."""
        return True

    # ── HTTP Client ──────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client for eth_getLogs calls."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=http_limits(),
            )
        return self._client

    async def aclose(self) -> None:
        """Close the shared HTTP client (idempotent; called on shutdown)."""
        client, self._client = self._client, None
        if client is not None and not client.is_closed:
            with contextlib.suppress(Exception):
                await client.aclose()

    # ── Endpoint Verification ────────────────────────────────────────

    async def verify_endpoints(self) -> list[str]:
        """[DEPRECATED] Verify eth_getLogs endpoints are healthy for this chain.

        Deprecated: the indexer no longer calls this method — it verifies
        endpoints directly via :func:`lite_indexer.rpc.verify_endpoints_for_chain`
        during ``LiteIndexer.sync_loop``. Kept only for the public strategy
        API and tests.
        """
        import warnings

        warnings.warn(
            "EVMChainStrategy.verify_endpoints() is deprecated; "
            "use lite_indexer.rpc.verify_endpoints_for_chain instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return []

        tip = await self.rpc.eth_block_number(get_best_endpoint(endpoints))
        return await verify_endpoints_for_chain(
            endpoints,
            self.chain,
            latest_block=tip,
        )

    # ── Block Number ─────────────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get latest block number via eth_blockNumber.

        Returns None on all errors (timeout, connection, empty response,
        malformed JSON). The caller handles None by treating it as
        "no RPC available" and aborting the sync cycle.
        """
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.error("no_endpoints_for_chain", chain=self.chain)
            return None

        # Try multiple endpoints for redundancy
        for _ in range(3):
            endpoint = get_best_endpoint(endpoints)
            try:
                result = await self.rpc.eth_block_number(endpoint)
                if result is not None:
                    return result
            except (httpx.HTTPError, ConnectionError, OSError, TimeoutError, ValueError) as exc:
                logger.warning(
                    "fetch_block_number_failed",
                    chain=self.chain,
                    endpoint=endpoint[:50],
                    error=str(exc)[:60],
                )
                record_endpoint_failure(endpoint)
                await asyncio.sleep(1)
                continue

        logger.error("fetch_block_number_all_failed", chain=self.chain)
        return None

    # ── Block Timestamps ─────────────────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch ISO-8601 timestamps for a set of EVM block numbers.

        Also warms the bloom cache from the fetched block headers so
        later address-indexed queries can pre-filter without extra RPC
        calls.
        """
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return {}

        return await rpc_fetch_block_timestamps(
            self.rpc,
            endpoints,
            block_numbers,
            chain=self.chain,
            bloom_cache=self._bloom_cache,
        )

    # ── Transfer Events (delegates to transfer_handler) ──────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch ERC-20 Transfer events via eth_getLogs.

        Delegates to :func:`transfer_handler.fetch_transfers` with the
        strategy's chunk size and bloom cache.

        Args:
            from_block: Start block (inclusive).
            to_block: End block (inclusive).

        Returns:
            List of parsed transfer event dicts.
        """
        client = await self._get_client()
        return await _fetch_transfers(
            self.chain,
            from_block,
            to_block,
            client=client,
            rpc=self.rpc,
            batch_size=self.batch_size,
            chunk_size=self.chunk_size,
            bloom_cache=self._bloom_cache,
        )

    async def fetch_transfers_for_address(
        self,
        address: str,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch ERC-20 Transfer events for a specific token address.

        Uses TrueBlocks-style bloom pre-filtering when a bloom cache is
        available: block headers are checked for the address first and
        blocks that cannot contain it are skipped before any
        ``eth_getLogs`` call. Rarely-traded tokens over a cold range
        short-circuit with zero log queries.

        Args:
            address: 0x-prefixed token contract address.
            from_block: Start block (inclusive).
            to_block: End block (inclusive).

        Returns:
            List of parsed transfer event dicts for the address.
        """
        client = await self._get_client()
        return await _fetch_transfers(
            self.chain,
            from_block,
            to_block,
            client=client,
            rpc=self.rpc,
            batch_size=self.batch_size,
            chunk_size=self.chunk_size,
            address=address,
            bloom_cache=self._bloom_cache,
        )

    # ── DEX Swap Events (delegates to dex_handler) ───────────────────

    async def fetch_swap_events(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]] | None:
        """Fetch DEX Swap events (V2 + V3); None = total dex-leg failure."""
        client = await self._get_client()
        events = await _fetch_swap_events(
            self.chain,
            from_block,
            to_block,
            client=client,
            rpc=self.rpc,
            pair_cache=self._pair_cache,
            batch_size=self.batch_size,
            chunk_size=self.chunk_size,
        )
        # Buffer address rows from the swap payloads too (sender/recipient/
        # pair/tokens) — drained in write_clickhouse. If a cycle yields swaps
        # but no transfers, write_clickhouse is not called and these rows
        # drain on the next transfer cycle (bounded by the row cap). The
        # parser also accepts ``involvedAccounts``/``addresses`` so the
        # internal-transfer trace path can feed log/internal-call addresses
        # in a follow-up. Best-effort.
        if events is not None:
            self._ingest_address_activity_events(events)
        return events

    # ── PairCreated Fetch (delegates to deployer_handler) ────────────

    async def _fetch_pair_created_events(
        self,
        from_block: int,
        to_block: int,
    ) -> None:
        """Fetch PairCreated (V2) + PoolCreated (V3) events from known DEX factories.

        Delegates to :func:`deployer_handler.fetch_pair_created` and
        stores results in self._last_pair_events.
        """
        client = await self._get_client()
        self._last_pair_events = await _fetch_pair_created(
            self.chain,
            from_block,
            to_block,
            client=client,
            pair_cache=self._pair_cache,
        )

    # ── Reorg Staging (Envio/TrueBlocks) ─────────────────────────────

    async def reorg_stage(
        self,
        from_block: int,
        to_block: int,
        ch_client: Any | None = None,
    ) -> ReorgCheck:
        """Checkpoint block hashes and roll back deep reorgs for a batch.

        Envio/TrueBlocks-style reorg-safe staging, gated by
        ``REORG_PROTECTION_ENABLED`` (default on for EVM). Called after a
        block batch is processed:

        1. Save the batch tip block hash to the checkpoint table (the
           last ``MAX_REORG_DEPTH`` blocks are "unripe").
        2. For ``to_block - MAX_REORG_DEPTH[chain]``, verify the current
           hash matches the checkpointed hash.
        3. On mismatch: log a warning, roll back ``evm_transfers`` +
           ``dex_events`` above the fork point, and return the fork block
           so the caller can re-fetch from it.

        Fail-CLOSED: when a checkpoint lookup fails, a required RPC hash
        is missing, or the rollback fails, the result is
        :attr:`ReorgStatus.UNKNOWN` (with ``error`` set) so the caller
        does NOT advance state past a range it could not verify. The
        benign "no checkpoint yet" case is normalised to NO_REORG so
        catch-up does not stall (the block predates our checkpoint
        coverage).

        Args:
            from_block: Start block of the processed batch.
            to_block: Tip block of the processed batch (checkpointed).
            ch_client: ClickHouse client (raw, from ClickHouseWriter).

        Returns:
            :class:`ReorgCheck` — REORG with ``fork_block`` when a reorg
            was detected and rolled back; UNKNOWN (``error`` set) when
            verification/rollback could not be performed; NO_REORG
            otherwise.
        """
        if not REORG_PROTECTION_ENABLED or ch_client is None:
            return ReorgCheck(ReorgStatus.NO_REORG)

        if not await ensure_checkpoint_table(ch_client):
            return ReorgCheck(ReorgStatus.UNKNOWN, error="checkpoint table unavailable")

        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            logger.debug("reorg_no_endpoints", chain=self.chain)
            return ReorgCheck(ReorgStatus.UNKNOWN, error="no endpoints")

        endpoint = get_best_endpoint(endpoints)

        # 1. Checkpoint the batch tip hash.
        tip_block = await self.rpc.eth_get_block_by_number(endpoint, to_block)
        if not tip_block or not isinstance(tip_block, dict) or not tip_block.get("hash"):
            logger.debug("reorg_tip_hash_missing", chain=self.chain, block=to_block)
            return ReorgCheck(ReorgStatus.UNKNOWN, error="tip block hash unavailable")
        tip_hash = str(tip_block["hash"])
        await checkpoint_block(self.chain, to_block, tip_hash, ch_client)

        # 2. Verify the block that should now be deep-confirmed.
        depth = reorg_depth(self.chain)
        verify_block = to_block - depth
        if verify_block < 0:
            return ReorgCheck(ReorgStatus.NO_REORG)

        current = await self.rpc.eth_get_block_by_number(endpoint, verify_block)
        if not current or not isinstance(current, dict) or not current.get("hash"):
            logger.debug("reorg_verify_hash_missing", chain=self.chain, block=verify_block)
            return ReorgCheck(ReorgStatus.UNKNOWN, error="verify block hash unavailable")
        current_hash = str(current["hash"])

        check = await check_reorg(self.chain, verify_block, current_hash, ch_client)
        if check.status is ReorgStatus.UNKNOWN:
            if check.error is None:
                # Benign: no checkpoint for this block yet — nothing to
                # compare, safe to continue (do NOT stall catch-up).
                return ReorgCheck(ReorgStatus.NO_REORG)
            logger.error(
                "reorg_status_unknown",
                chain=self.chain,
                block=verify_block,
                error=check.error,
            )
            return check
        if check.status is ReorgStatus.NO_REORG:
            return check

        # 3. Reorg detected — log, roll back to the fork point, re-fetch.
        logger.warning(
            "chain_reorg_deep",
            chain=self.chain,
            fork_block=verify_block,
            depth=depth,
        )
        deleted = await rollback_to_block(self.chain, verify_block, ch_client)
        if deleted is None:
            logger.error("reorg_rollback_failed", chain=self.chain, fork_block=verify_block)
            return ReorgCheck(ReorgStatus.UNKNOWN, fork_block=verify_block, error="rollback failed")
        return check


__all__ = [
    "EVMChainStrategy",
]
