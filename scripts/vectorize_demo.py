#!/usr/bin/env python3
"""Demo: wallet similarity search with LanceDB.

Takes a wallet address, builds a transaction-pattern description, embeds
it, and returns the top-10 most similar wallets from the LanceDB
``wallet_embeddings`` collection.

The transaction pattern is built two ways:

1. When ``--clickhouse`` is set, the pattern is summarized from the
   ClickHouse warehouse (chain, transfer counts, unique tokens) via a
   lightweight query against ``wallet_memory.evm_transfers``.
2. Otherwise a generic pattern description is constructed from the
   address so the demo runs with zero external dependencies.

Usage:
    python scripts/vectorize_demo.py 0x1234... --clickhouse
    python scripts/vectorize_demo.py 0x1234... --k 20 --lance-dir /var/data/lance
"""

from __future__ import annotations

import argparse
import asyncio
import sys

# Allow running from the repo root without installation.
sys.path.insert(0, ".")

from lite_indexer.vectors import LanceVectorStore  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("address", help="Wallet address to find similar wallets for")
    parser.add_argument("--k", type=int, default=10, help="Number of similar wallets to return")
    parser.add_argument("--lance-dir", default="/var/data/lance", help="LanceDB database directory")
    parser.add_argument("--clickhouse", action="store_true", help="Summarize pattern from ClickHouse")
    parser.add_argument("--ch-host", default="127.0.0.1", help="ClickHouse host (with --clickhouse)")
    parser.add_argument("--ch-port", type=int, default=9000, help="ClickHouse port (with --clickhouse)")
    parser.add_argument("--ch-db", default="wallet_memory", help="ClickHouse database (with --clickhouse)")
    return parser.parse_args()


def _wallet_pattern_from_clickhouse(address: str, args: argparse.Namespace) -> str:
    """Summarize a wallet's transaction pattern from the warehouse."""
    try:
        from clickhouse_driver import Client

        client = Client(host=args.ch_host, port=args.ch_port, database=args.ch_db, connect_timeout=5)
        rows = client.execute(
            """
            SELECT chain,
                   count() AS tx_count,
                   uniqExact(token_address) AS tokens
            FROM evm_transfers
            WHERE from_address = %(addr)s OR to_address = %(addr)s
            GROUP BY chain
            ORDER BY tx_count DESC
            LIMIT 5
            """,
            {"addr": address},
        )
    except Exception as exc:  # noqa: BLE001 — demo must degrade gracefully
        return f"wallet {address} with unknown transaction pattern (clickhouse unavailable: {exc})"

    if not rows:
        return f"wallet {address} with no observed transfer activity"

    chains = ", ".join(f"{chain} ({tx} tx / {tokens} tokens)" for chain, tx, tokens in rows)
    return f"wallet {address} with active transfers on {chains}"


def _wallet_pattern(address: str, args: argparse.Namespace) -> str:
    """Build a transaction-pattern description for embedding."""
    if args.clickhouse:
        return _wallet_pattern_from_clickhouse(address, args)
    return f"wallet {address} with high-volume token transfer activity across multiple chains"


async def main() -> int:
    args = parse_args()
    address = args.address.lower()
    pattern = _wallet_pattern(address, args)

    store = LanceVectorStore(path=args.lance_dir)
    try:
        vector = (await store.embedder.embed([pattern]))[0]
        results = await store.search("wallet_embeddings", vector, k=args.k)
    finally:
        await store.aclose()

    print(f"Query wallet : {address}")
    print(f"Pattern      : {pattern[:120]}…" if len(pattern) > 120 else f"Pattern      : {pattern}")
    print(f"Top {len(results)} similar wallets (LanceDB {args.lance_dir}):\n")

    if not results:
        print("No similar wallets found. Index the wallet_embeddings collection first:")
        print(
            '  python -c "import asyncio; from lite_indexer.vectors import LanceVectorStore;'
            ' asyncio.run(LanceVectorStore().ensure_default_collections())"'
        )
        return 1

    for i, row in enumerate(results, start=1):
        distance = row.get("_distance")
        print(f"{i:2d}. {row.get('id')}  (distance={distance:.4f})")
        labels = row.get("labels")
        if labels:
            print(f"    labels: {labels}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
