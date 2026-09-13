"""Shared mixin contract for :class:`SolanaStrategy`.

The Solana strategy was split from a single monolithic class into mixins that
freely reference attributes and methods owned by sibling mixins or by the
concrete :class:`~lite_indexer.strategies.solana.strategy.SolanaStrategy`.
mypy strict checks each mixin in isolation, so it cannot resolve those
cross-mixin references — producing ``attr-defined``, ``has-type`` and
``assignment`` errors.

This module declares the shared surface once, mirroring the pattern used by
:class:`lite_indexer.indexer._base.IndexerMixinBase`. Every data attribute is
typed here and every method each mixin relies on gets a signature; each mixin
inherits this contract so mypy sees a complete surface in isolation.

All members are declarations only. The real implementations live in one of the
mixins or in the concrete class, all of which are earlier in the MRO. Method
bodies raise so an accidental dispatch to an undeclared implementation fails
loudly instead of silently returning ``None``.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

import httpx

from lite_indexer.strategies.reorg_status import ReorgCheck
from lite_indexer.util.bounded import BoundedSet

if TYPE_CHECKING:
    from lite_indexer.traces.solana import SolanaTraceCollector


class SolanaMixinBase:
    """Declared shared surface consumed by every ``SolanaStrategy`` mixin."""

    # ── State set by ``SolanaStrategy.__init__`` ──────────────────────────
    chain: str
    tracked_tokens: list[dict[str, Any]]
    _mint_set: set[str]
    _client: httpx.AsyncClient | None
    _last_deployer_events: list[dict[str, Any]]
    _deployer_captured_mints: set[str]
    _deployer_attempted_mints: set[str]
    _trace_collector: SolanaTraceCollector
    _trace_rows: list[dict[str, Any]]
    _address_activity_rows: list[dict[str, Any]]
    _last_launch_events: list[Any]
    _last_bundle_events: list[Any]
    _last_swap_events: list[dict[str, Any]]
    _ch_client: Any
    _base_mints: frozenset[str]
    _launchpad_last_slot: int
    _launchpad_seen_mints: BoundedSet[str]

    # ── Methods owned by the concrete class / sibling mixins ──────────────
    async def fetch_block_number(self) -> int | None:
        raise NotImplementedError

    async def fetch_transfers(self, from_slot: int, to_slot: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        raise NotImplementedError

    def supports_deployers(self) -> bool:
        raise NotImplementedError

    def write_clickhouse(self, events: list[dict[str, Any]], ch_writer: Any) -> int:
        raise NotImplementedError

    async def reorg_stage(
        self,
        from_slot: int,
        to_slot: int,
        ch_client: Any | None = None,
    ) -> ReorgCheck:
        raise NotImplementedError

    # ── Methods owned by ``SyncMixin`` ────────────────────────────────────
    def _discover_new_tokens(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def poll_launchpad_blocks(self) -> int:
        raise NotImplementedError

    async def capture_deployers_for_new_tokens(self, new_tokens: list[dict[str, Any]]) -> int:
        raise NotImplementedError

    async def _rpc_call(
        self,
        method: str,
        params: list[Any] | None = None,
        endpoint: str | None = None,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    async def _capture_deployer_event(
        self,
        mint: str,
        creation_sig: dict[str, Any],
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    # ── Methods owned by the buffering mixins ─────────────────────────────
    def _buffer_swap_rows(self, swap_rows: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def _buffer_trace_rows(self, trace_rows: list[dict[str, Any]]) -> None:
        raise NotImplementedError

    def _drain_trace_rows(self, client: Any) -> None:
        raise NotImplementedError

    def _ingest_address_activity(self, tx_data: dict[str, Any], slot: int | None = None) -> int:
        raise NotImplementedError

    def _drain_address_activity_rows(self, client: Any) -> None:
        raise NotImplementedError
