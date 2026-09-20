"""Shared mixin contract for :class:`LiteIndexer`.

The de-god refactor split ``LiteIndexer`` into mixins that freely reference
attributes and methods owned by sibling mixins or the concrete class. mypy
strict checks each mixin in isolation and cannot resolve those cross-mixin
references, which produced ``attr-defined`` / ``has-type`` / ``assignment``
errors. This module declares the shared surface once — every data attribute
with its type plus the method/property signatures each mixin relies on — so
each mixin can inherit the contract.

All members here are declarations only. The real implementations live in the
concrete ``LiteIndexer`` or one of the mixins, all of which are more derived
in the MRO than this base. The bodies raise so that an accidental dispatch to
an undeclared implementation fails loudly instead of silently returning None.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio
    from pathlib import Path

    from lite_indexer.chain_registry import ChainConfig
    from lite_indexer.indexer._resource_guards import ClickHouseHealth, MemoryGate
    from lite_indexer.indexer.backfill import BackfillExecutor, BackfillQueue
    from lite_indexer.plugin import BasePlugin
    from lite_indexer.rpc import RpcClient
    from lite_indexer.rpc.verify import EndpointVerifyResult
    from lite_indexer.state import StateHighWater, StateManager
    from lite_indexer.storage import ClickHouseWriter, NDJSONWriter
    from lite_indexer.strategies.base import AbstractChainStrategy
    from lite_indexer.streaming.nats_publisher import NATSPublisher
    from lite_indexer.webhooks.delivery import WebhookDelivery


class IndexerMixinBase:
    """Declared shared surface consumed by every ``LiteIndexer`` mixin."""

    # ── State set by ``LiteIndexer.__init__`` ─────────────────────────────
    chain: str
    batch_size: int
    disk_cap: int
    dex_enabled: bool
    deployers_enabled: bool
    traces_enabled: bool
    lazy_indexing_enabled: bool
    vector_ingestion_enabled: bool
    ndjson_enabled: bool
    nats_enabled: bool
    webhook_enabled: bool
    start_from_head: bool
    start_from_head_user: bool
    _reorg_recovery: bool
    data_dir: Path
    chunk_size: int
    rpc: RpcClient
    state: StateManager
    _highwater: StateHighWater
    _cycles_since_gc: int
    _cycles_since_reconcile: int
    _memory_gate: MemoryGate
    _ch_health: ClickHouseHealth
    _chain_type: str
    _chain_config: ChainConfig | None
    _persisted_enabled_events: set[str]
    _override_from: int | None
    _override_to: int | None
    _verify_result: EndpointVerifyResult | None
    _ndjson_writer: NDJSONWriter | None
    _ch_writer: ClickHouseWriter | None
    _strategy: AbstractChainStrategy | None
    _plugins: list[BasePlugin] | None
    _nats_publisher: NATSPublisher | None
    _webhook_delivery: WebhookDelivery | None
    _vector_store: Any
    _last_vector_ingestion_block: int | None
    # Freemium backfill queue (lazy) + injected executor + gap signals.
    _backfill_queue: BackfillQueue | None
    _backfill_executor: BackfillExecutor | None
    _backfill_seen_block: int | None
    _reconcile_rewinds: list[tuple[int, int]]
    # Set by ``_determine_block_range``; default keeps reads safe.
    _last_tip: int | None = None
    # EVM WebSocket head pool (rpc/head_pool.py) — none = not started/disabled.
    _head_pool_task: asyncio.Task[None] | None
    _head_wakeup: asyncio.Event | None

    # ── Properties owned by the concrete class / a mixin ──────────────────
    @property
    def strategy(self) -> AbstractChainStrategy:
        raise NotImplementedError

    @property
    def ch_writer(self) -> ClickHouseWriter:
        raise NotImplementedError

    @property
    def nats_publisher(self) -> NATSPublisher | None:
        raise NotImplementedError

    # ── Methods owned by the concrete class ───────────────────────────────
    def _is_evm(self) -> bool:
        raise NotImplementedError

    def _fetch_ch_max_block(self) -> int | None:
        raise NotImplementedError

    def _save_current_state(self) -> None:
        raise NotImplementedError

    def _enabled_event_types(self) -> list[str]:
        raise NotImplementedError

    def _clear_reorg_recovery(self) -> None:
        raise NotImplementedError

    def _refresh_demand_enabled(self) -> None:
        raise NotImplementedError

    # ── Methods owned by sibling mixins ───────────────────────────────────
    async def fetch_block_number(self) -> int | None:
        raise NotImplementedError

    async def fetch_transfers(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_swap_events(self, from_block: int, to_block: int) -> list[dict[str, Any]] | None:
        raise NotImplementedError

    async def _fetch_pair_created_events(self, from_block: int, to_block: int) -> None:
        raise NotImplementedError

    async def _enrich_timestamps(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def _collect_internal_transfers(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def _dispatch_plugin_events(self, from_block: int, to_block: int) -> dict[str, Any]:
        raise NotImplementedError

    async def _publish_nats_events(
        self,
        chain: str,
        block: int,
        transfers: list[dict[str, Any]],
        dex_events: list[dict[str, Any]],
        deployer_events: list[dict[str, Any]],
        trace_events: list[dict[str, Any]],
    ) -> int:
        raise NotImplementedError

    async def _maybe_start_webhook_delivery(self) -> None:
        raise NotImplementedError

    async def _run_vector_ingestion_guarded(self, from_block: int, to_block: int, summary: dict[str, Any]) -> None:
        raise NotImplementedError

    def memory_gate_ok(self) -> bool:
        raise NotImplementedError

    def guarded_memory_mb(self) -> int:
        raise NotImplementedError

    def _maybe_reconcile_range(self, from_block: int, to_block: int, expected: int, summary: dict[str, Any]) -> None:
        raise NotImplementedError

    # ── EVM WebSocket head pool (SyncLoopMixin) ───────────────────────────
    async def stop_head_pool(self) -> None:
        raise NotImplementedError

    # ── Freemium backfill queue (BackfillMixin) ───────────────────────────
    def _get_backfill_queue(self) -> BackfillQueue:
        raise NotImplementedError

    def _record_reconcile_rewind(self, from_block: int, to_block: int) -> None:
        raise NotImplementedError

    def _enqueue_backfill_gaps(self, tip: int | None) -> int:
        raise NotImplementedError

    async def _run_backfill_sidecar(self) -> dict[str, int]:
        raise NotImplementedError
