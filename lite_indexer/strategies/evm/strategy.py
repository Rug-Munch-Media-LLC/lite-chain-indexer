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
from typing import Any

import httpx

from lite_indexer.chains import CHAIN_BLOCK_RANGE_LIMITS, GET_LOGS_ENDPOINTS
from lite_indexer.config import BATCH_SIZE, RPC_TIMEOUT
from lite_indexer.logging import get_logger
from lite_indexer.rpc import (
    RpcClient,
    get_best_endpoint,
    record_endpoint_failure,
    verify_endpoints_for_chain,
    fetch_block_timestamps as rpc_fetch_block_timestamps,
)
from lite_indexer.state import PairCache
from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.strategies.evm._helpers import detect_reorg, enforce_block_range
from lite_indexer.strategies.evm.transfer_handler import fetch_transfers as _fetch_transfers
from lite_indexer.strategies.evm.dex_handler import fetch_swap_events as _fetch_swap_events
from lite_indexer.strategies.evm.deployer_handler import fetch_pair_created as _fetch_pair_created

logger = get_logger(__name__)


class EVMChainStrategy(AbstractChainStrategy):
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
    ) -> None:
        super().__init__(chain)
        self.rpc = rpc or RpcClient()
        self.batch_size = batch_size
        self._pair_cache = pair_cache or PairCache(chain)

        # Latest batch of pair creation events for deployer tracking
        self._last_pair_events: list[dict[str, Any]] = []

        # HTTP client for eth_getLogs batch calls
        self._client: httpx.AsyncClient | None = None

    @property
    def pair_cache(self) -> PairCache:
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
        return True

    def supports_deployers(self) -> bool:
        return True

    # ── HTTP Client ──────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client for eth_getLogs calls."""
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(RPC_TIMEOUT),
                limits=limits,
            )
        return self._client

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
        for attempt in range(3):
            endpoint = get_best_endpoint(endpoints)
            try:
                result = await self.rpc.eth_block_number(endpoint)
                if result is not None:
                    return result
            except Exception as exc:
                logger.debug(
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
        """Fetch ISO-8601 timestamps for a set of EVM block numbers."""
        endpoints = GET_LOGS_ENDPOINTS.get(self.chain, [])
        if not endpoints:
            return {}

        return await rpc_fetch_block_timestamps(self.rpc, endpoints, block_numbers)

    # ── Transfer Events (delegates to transfer_handler) ──────────────

    async def fetch_transfers(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch ERC-20 Transfer events via eth_getLogs.

        Delegates to :func:`transfer_handler.fetch_transfers`.
        """
        client = await self._get_client()
        return await _fetch_transfers(
            self.chain,
            from_block,
            to_block,
            client=client,
            rpc=self.rpc,
            batch_size=self.batch_size,
        )

    # ── DEX Swap Events (delegates to dex_handler) ───────────────────

    async def fetch_swap_events(
        self,
        from_block: int,
        to_block: int,
    ) -> list[dict[str, Any]]:
        """Fetch DEX Swap events (V2 + V3) via eth_getLogs.

        Delegates to :func:`dex_handler.fetch_swap_events`.
        """
        client = await self._get_client()
        return await _fetch_swap_events(
            self.chain,
            from_block,
            to_block,
            client=client,
            rpc=self.rpc,
            pair_cache=self._pair_cache,
            batch_size=self.batch_size,
        )

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


__all__ = [
    "EVMChainStrategy",
]
