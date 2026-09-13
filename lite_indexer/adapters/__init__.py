"""
Chain adapters — focused, single-purpose data accessors.

Adapters are thin, self-contained clients for a specific RPC/data source.
Unlike strategies (which own the full index/sync lifecycle), an adapter
answers one question well: "give me the transfers for this chain from this
provider."

Currently ships a single adapter:

  - ``SolanaTransfersAdapter`` — SPL token transfers via the shared Solana
    RPC pool (``getSignaturesForAddress`` + ``getTransaction``), written to
    the ``wallet_memory.solana_transfers`` ClickHouse table.
"""

from __future__ import annotations

from lite_indexer.adapters.solana import SolanaTransfersAdapter

__all__ = ["SolanaTransfersAdapter"]
