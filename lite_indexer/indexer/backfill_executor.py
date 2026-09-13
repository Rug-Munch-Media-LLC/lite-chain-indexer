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
        return bool(host.strategy.write_clickhouse(events, host.ch_writer))

    return _execute


__all__ = ["EvmBackfillHost", "build_evm_backfill_executor"]
