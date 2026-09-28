"""Concrete EVM executor for the freemium backfill queue.

Split out of :mod:`lite_indexer.indexer.backfill` to keep that module focused
on the queue itself. The executor is deliberately narrow: it fetches transfer
logs for one chunk through the freemium endpoint the worker selected (the
worker already wraps the call in the provider-account concurrency slot), parses
them with the standard transfer parser, and writes them through the existing
ClickHouse path. It returns True only when the chunk was fully handled.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from lite_indexer.config import TRANSFER_EVENT_TOPIC
from lite_indexer.indexer.backfill import BackfillChunk, BackfillExecutor
from lite_indexer.indexer.guarded_write import write_with_guard
from lite_indexer.logging import get_logger

if TYPE_CHECKING:
    from lite_indexer.rpc import RpcClient
    from lite_indexer.storage import ClickHouseWriter
    from lite_indexer.strategies.base import AbstractChainStrategy

log = get_logger(__name__)
logger = log


class EvmBackfillHost(Protocol):
    """Surface the EVM backfill executor needs from a LiteIndexer.

    ``strategy`` is declared read-only (property) so a ``LiteIndexer`` whose
    ``strategy`` is exposed as a property structurally satisfies it.
    """

    chain: str
    rpc: RpcClient

    @property
    def strategy(self) -> AbstractChainStrategy: ...

    @property
    def ch_writer(self) -> ClickHouseWriter: ...

    def _is_evm(self) -> bool: ...


def build_evm_backfill_executor(host: EvmBackfillHost) -> BackfillExecutor:
    """Build the concrete EVM backfill executor for ``host``.

    The executor fetches transfer logs for the chunk through a freemium
    endpoint only (the worker wraps it in the provider-account concurrency
    slot), parses them with the standard transfer parser, and writes them
    through the existing ClickHouse path.

    Returns an executor returning True only when the chunk was fully handled
    (written, or confirmed empty); any failure returns False so the chunk is
    retried rather than advanced past.
    """

    async def _execute(chunk: BackfillChunk, endpoint: str) -> bool:
        from lite_indexer.dex import parse_transfer_logs

        if not host._is_evm():
            return False
        logs = await host.rpc.get_logs(
            endpoint,
            chunk.from_block,
            chunk.to_block,
            TRANSFER_EVENT_TOPIC,
            chain=chunk.chain,
        )
        if logs is None:
            return False
        events = parse_transfer_logs(logs, chunk.chain, chunk.from_block, chunk.to_block)
        if not events:
            return True
        return bool(
            await write_with_guard(
                host.strategy,
                host.ch_writer,
                events,
                chunk.from_block,
                chunk.to_block,
            )
        )

    return _execute


# ── Non-EVM (TRON) executor ─────────────────────────────────────────────


class NonEvmBackfillHost(Protocol):
    """Surface the non-EVM backfill executor needs from a LiteIndexer.

    TRON has no ``eth_getLogs``: its events are fetched through the chain
    strategy (TronGrid ``/v1/contracts/{address}/events``), so the executor
    delegates to ``strategy.fetch_transfers`` and ignores the freemium
    endpoint the queue resolved for it.
    """

    chain: str

    @property
    def strategy(self) -> AbstractChainStrategy: ...

    @property
    def ch_writer(self) -> ClickHouseWriter: ...


def build_tron_backfill_executor(host: NonEvmBackfillHost) -> BackfillExecutor:
    """Build the concrete TRON backfill executor for ``host``.

    Fetches the chunk's TRC20 transfers through the strategy (which owns the
    TronGrid REST transport and its own endpoint failover) and writes them
    through the existing ClickHouse path. Returns True only when the chunk
    was fully handled (written, or confirmed empty); an RPC failure or an
    exception returns False so the chunk is retried rather than advanced
    past.

    This is what lets ``backfill_queue_tron.json`` drain: the EVM executor
    hard-fails on ``_is_evm()`` and TRON has no freemium ``get_logs``
    endpoint, so without this the queue sat at ``attempts: 0`` forever.
    """

    async def _execute(chunk: BackfillChunk, endpoint: str) -> bool:
        from lite_indexer.rpc import RpcFetchFailedError

        try:
            events = await host.strategy.fetch_transfers(chunk.from_block, chunk.to_block)
        except RpcFetchFailedError:
            return False
        if not events:
            return True
        return bool(
            await write_with_guard(
                host.strategy,
                host.ch_writer,
                events,
                chunk.from_block,
                chunk.to_block,
            )
        )

    return _execute


__all__ = [
    "EvmBackfillHost",
    "NonEvmBackfillHost",
    "build_evm_backfill_executor",
    "build_tron_backfill_executor",
]
