"""
Internal-transfer trace collection (the #1 analytical blind spot).

The main indexer path only sees ERC-20 ``Transfer`` logs. ETH that moves
*inside* contracts — wrapped ETH unwraps, bridge deposits, fee flows,
``SELFDESTRUCT`` payouts — is invisible to ``eth_getLogs``. This package
closes that gap using the free ``trace_*`` / ``debug_traceBlockByNumber``
RPCs that public archive-capable endpoints expose:

  - ``trace_block``            — parity/erigon format (flat trace list)
  - ``debug_traceBlockByNumber`` with the ``callTracer`` — geth format
    (nested call tree)

Both formats are parsed into a standardized internal-transfer row and
written to ``wallet_memory.internal_transfers``.

Usage::

    collector = TraceCollector(chain="ethereum")
    raw = await collector.collect_block_traces("ethereum", 19_700_000)
    transfers = collector.extract_internal_transfers(raw)
    writer.write_internal_transfers("ethereum", transfers)

The collector is gated by ``TRACE_ENABLED`` (default off) because free
tier trace endpoints are heavily rate-limited; enable explicitly with
``TRACE_ENABLED=true`` or the ``--traces`` CLI flag.
"""

from __future__ import annotations

from lite_indexer.traces.endpoints import TRACE_ENDPOINTS, has_trace_endpoints
from lite_indexer.traces.parsers import (
    extract_internal_transfers,
    input_method_id,
    parse_call_tracer,
    parse_parity_trace_block,
)
from lite_indexer.traces.trace_collector import TraceCollector

__all__ = [
    "TRACE_ENDPOINTS",
    "has_trace_endpoints",
    "TraceCollector",
    "extract_internal_transfers",
    "input_method_id",
    "parse_call_tracer",
    "parse_parity_trace_block",
]
