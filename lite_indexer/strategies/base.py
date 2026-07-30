"""
Abstract base class for chain-specific indexing strategies.

All chain strategies must implement:
  - fetch_block_number()
  - fetch_transfers()

Optionally override:
  - fetch_block_timestamps() — for chains with block-level timestamps
  - write_clickhouse() — for custom ClickHouse table schemas
  - supports_dex() / supports_deployers() — for EVM-only features
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class AbstractChainStrategy(ABC):
    """Base class for all chain indexing strategies.

    Each strategy encapsulates the RPC logic for fetching block numbers
    and transfer events from a specific blockchain type.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain

    # ── Required ─────────────────────────────────────────────────────

    @abstractmethod
    async def fetch_block_number(self) -> int | None:
        """Get the latest block number (or slot for Solana).

        Returns:
            Latest block number/slot, or None on failure.
        """
        ...

    @abstractmethod
    async def fetch_transfers(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        """Fetch transfer events in the given block range.

        Args:
            from_block: Start block number (inclusive).
            to_block: End block number (inclusive).

        Returns:
            List of parsed transfer event dicts with schema-compatible fields.
        """
        ...

    # ── Optional: Block Timestamps ───────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch ISO-8601 timestamps for a set of block numbers.

        Default implementation returns empty dict (no timestamps).
        Override for chains that support block-level timestamp queries.

        Args:
            block_numbers: Set of block numbers to resolve.

        Returns:
            Dict of {block_number: ISO-8601 timestamp string}.
        """
        return {}

    # ── Optional: Feature Flags ──────────────────────────────────────

    def supports_dex(self) -> bool:
        """Whether this chain supports DEX swap event indexing.

        Only EVM-compatible chains with Uniswap-style DEXes return True.
        """
        return False

    def supports_deployers(self) -> bool:
        """Whether this chain supports deployer tracking via factory events.

        Only EVM-compatible chains with PairCreated/PoolCreated events return True.
        """
        return False

    # ── Optional: Endpoint Verification ──────────────────────────────

    async def verify_endpoints(self) -> list[str]:
        """Verify chain-specific RPC endpoints are healthy.

        Default returns empty list (no verification). Override for chains
        with custom RPC verification logic.

        Returns:
            List of verified healthy endpoint URLs.
        """
        return []

    # ── Optional: ClickHouse Write ───────────────────────────────────

    def write_clickhouse(
        self,
        events: list[dict[str, Any]],
        ch_writer: Any,  # ClickHouseWriter from storage
    ) -> int:
        """Write events to ClickHouse.

        Default delegates to ClickHouseWriter.write_transfers().
        Override for chains with custom table schemas (e.g., Solana).

        Args:
            events: List of parsed event dicts.
            ch_writer: ClickHouseWriter instance.

        Returns:
            Number of rows inserted.
        """
        return ch_writer.write_transfers(self.chain, events)
