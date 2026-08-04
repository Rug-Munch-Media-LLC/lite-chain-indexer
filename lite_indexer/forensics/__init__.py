"""
Cross-chain fund forensics — follow the money across chains, bridges,
DEXes and CEX deposits.

This package turns the per-chain transfer data the indexer already
collects (``evm_transfers``, ``internal_transfers``,
``solana_spl_balance_changes``, ``dex_events``) into Arkham-style
cross-chain traces — without any external intelligence API:

* :mod:`address_registry` — curated bridge + CEX hot-wallet registry
  (``{chain: {address: {name, type, confidence}}}``).
* :mod:`fund_tracer` — :class:`FundTracer`: forward/backward traces,
  cross-chain path finding, bridge + CEX deposit detection. Entity
  tagging and risk scoring layer on ``wallet_labels`` / ``scam_addresses``.
* :mod:`bridge_store` — persists confirmed bridge matches to
  ``wallet_memory.bridge_mappings`` for reuse by later traces.

Usage::

    from lite_indexer.forensics import FundTracer

    tracer = FundTracer()
    graph = await tracer.trace_forward(tx_hash, chain="ethereum")
    print(graph.to_dict())
"""

from __future__ import annotations

from lite_indexer.forensics.address_registry import DEFAULT_REGISTRY, AddressRegistry
from lite_indexer.forensics.fund_tracer import (
    BridgeInteraction,
    CexDeposit,
    FundTracer,
    TraceEdge,
    TraceGraph,
    TraceNode,
)

__all__ = [
    "AddressRegistry",
    "BridgeInteraction",
    "CexDeposit",
    "DEFAULT_REGISTRY",
    "FundTracer",
    "TraceEdge",
    "TraceGraph",
    "TraceNode",
]
