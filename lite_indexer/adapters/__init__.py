"""
Chain adapters — focused, single-provider data accessors.

Adapters are thin, self-contained clients for a specific RPC provider or
data source. Unlike strategies (which own the full index/sync lifecycle),
an adapter answers one question well: "give me the transfers for this
chain from this provider."

Currently ships a single adapter:

  - ``SolanaHeliusAdapter`` — SPL token transfers via the Helius RPC
    (``getSignaturesForAddress`` + ``getTransaction``), written to the
    ``wallet_memory.solana_transfers`` ClickHouse table.
"""

from __future__ import annotations

from lite_indexer.adapters.solana import SolanaHeliusAdapter

__all__ = ["SolanaHeliusAdapter"]
