"""Trace-level extraction mixin — SOL transfers, CPIs, account state changes.

Split from the former monolithic strategy.py. Rows are buffered on the
strategy instance (``_trace_rows``) and drained in ``write_clickhouse``.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.traces.solana import (
    ensure_solana_transfers_table,
    write_solana_transfers,
)

logger = get_logger(__name__)

# Upper bound on buffered trace rows awaiting a ClickHouse write. Trace
# rows are collected from the same getTransaction calls as SPL events
# (zero extra RPC cost) and drained in write_clickhouse; the cap bounds
# memory when CH is down between cycles (oldest rows are dropped first).
_SOLANA_TRACE_ROW_CAP = 10_000


class TracesMixin:
    """Buffering and draining of Solana trace rows (best-effort)."""

    def _buffer_trace_rows(self, trace_rows: list[dict[str, Any]]) -> None:
        """Buffer trace rows, dropping the oldest when over the cap."""
        self._trace_rows.extend(trace_rows)
        if len(self._trace_rows) > _SOLANA_TRACE_ROW_CAP:
            self._trace_rows = self._trace_rows[-_SOLANA_TRACE_ROW_CAP:]

    def _drain_trace_rows(self, client: Any) -> None:
        """Write buffered trace rows to ClickHouse (best-effort).

        On failure the rows are kept (bounded by _SOLANA_TRACE_ROW_CAP)
        and retried on the next write cycle.
        """
        trace_rows = self._trace_rows
        self._trace_rows = []
        if trace_rows:
            try:
                ensure_solana_transfers_table(client)
                written = write_solana_transfers(client, trace_rows)
                logger.info("solana_trace_clickhouse_inserted", rows=written)
            except Exception as trace_exc:  # noqa: BLE001 — best-effort traces
                logger.error(
                    "solana_trace_clickhouse_insert_failed",
                    rows=len(trace_rows),
                    error=str(trace_exc)[:150],
                )
                self._trace_rows = trace_rows
