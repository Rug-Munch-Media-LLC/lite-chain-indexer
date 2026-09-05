"""DEX program-name resolution and transfer-graph aggregation.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

from typing import Any

from .helpers import _to_int, _to_timestamp

# Short display names for well-known programs; unknown programs keep their
# raw pubkey in the ``program_id`` column.
_PROGRAM_NAMES: dict[str, str] = {
    "11111111111111111111111111111111": "systemProgram",
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA": "tokenProgram",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb": "token2022Program",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL": "associatedToken",
    "Stake11111111111111111111111111111111111111": "stakeProgram",
    "ComputeBudget111111111111111111111111111111": "computeBudget",
    "Vote111111111111111111111111111111111111111": "voteProgram",
    # Popular DEX/router programs, resolved for readability.
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "jupiter",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "pumpfunAmm",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wF2t2j8": "raydium",
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pumpfun",
}


def _program_name(program_id: str) -> str:
    """Resolve a program pubkey to a friendly display name.

    Args:
        program_id: Program pubkey string.

    Returns:
        The friendly name, or the raw pubkey for unknown programs.
    """
    return _PROGRAM_NAMES.get(program_id, program_id)


def build_transfer_graph_from_rows(
    signature: str,
    tx_data: dict[str, Any],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build a directed graph of all value movement in a transaction.

    Aggregates transfer rows by (from, to, mint, transfer_type), summing
    amounts so parallel legs of a swap collapse into one edge.

    Args:
        signature: Transaction signature (base58).
        tx_data: Parsed ``getTransaction`` result (jsonParsed).
        rows: Transfer rows from :func:`extract_solana_transfers`.

    Returns:
        Graph dict: ``{signature, chain, slot, block_timestamp,
        transfers, nodes, edges}``.
    """
    edges: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["from_address"], row["to_address"], row["mint"], row["transfer_type"])
        edge = edges.setdefault(
            key,
            {
                "from": row["from_address"],
                "to": row["to_address"],
                "token": row["mint"],
                "transfer_type": row["transfer_type"],
                "amount": 0,
                "decimals": row["decimals"],
                "instruction_paths": [],
            },
        )
        edge["amount"] += int(row["amount"])
        edge["instruction_paths"].append([int(i) for i in row["instruction_path"]])

    nodes: list[str] = []
    seen: set[str] = set()
    for edge in edges.values():
        for addr in (edge["from"], edge["to"]):
            if addr and addr not in seen:
                seen.add(addr)
                nodes.append(addr)

    return {
        "signature": signature,
        "chain": "solana",
        "slot": _to_int(tx_data.get("slot"), 0),
        "block_timestamp": _to_timestamp(tx_data.get("blockTime")),
        "transfers": len(rows),
        "nodes": sorted(nodes),
        "edges": list(edges.values()),
    }


__all__ = ["_program_name", "build_transfer_graph_from_rows"]
