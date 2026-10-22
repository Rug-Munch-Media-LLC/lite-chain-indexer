"""Shortest paths from the traced wallet to cash-out / exit nodes.

Custody attribution is a path problem, not a volume problem: a wallet is
"one hop from Binance" only if an edge actually reaches a Binance address.
This module walks the assembled graph breadth-first and returns the shortest
path to every exchange / bridge / mixer node, with the transaction hashes
that carry each hop, so the caller can show evidence rather than a score.
"""

from __future__ import annotations

from collections import deque
from typing import Any

#: Node kinds that end a path: money leaves the traced cluster here.
EXIT_KINDS = ("exchange", "bridge", "mixer")


def _adjacency(graph: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    adjacency: dict[str, list[dict[str, Any]]] = {}
    for edge in graph.get("edges") or []:
        adjacency.setdefault(edge["from"], []).append(edge)
    return adjacency


def paths_to_cex(graph: dict[str, Any], root: str, *, max_hops: int = 3) -> list[dict[str, Any]]:
    """Return one shortest exit path per exchange/bridge/mixer node reached.

    Args:
        graph: Output of :func:`lite_indexer.query.trace_model.build_graph`.
        root: The traced address (path origin).
        max_hops: Maximum number of edges to follow (depth guard).

    Returns:
        One dict per exit node, sorted by hop count then inbound USD:
        ``{address, kind, entity, label, hops, path, tx_hashes}``. A path is
        the address chain from ``root`` to the exit node; ``tx_hashes`` is
        the per-hop evidence, in the same order.
    """
    adjacency = _adjacency(graph)
    kinds = {node["address"]: node["kind"] for node in graph.get("nodes") or []}
    inbound = {node["address"]: node.get("in_usd") or 0.0 for node in graph.get("nodes") or []}

    queue: deque[tuple[str, list[str], list[str]]] = deque([(root, [root], [])])
    best: dict[str, tuple[list[str], list[str]]] = {}
    while queue:
        address, path, hashes = queue.popleft()
        if len(path) - 1 >= max_hops:
            continue
        for edge in adjacency.get(address, []):
            nxt = edge["to"]
            if nxt in path:
                continue
            next_path = [*path, nxt]
            next_hashes = [*hashes, edge.get("tx_hash") or ""]
            exit_kind = kinds.get(nxt)
            if exit_kind in EXIT_KINDS:
                if nxt not in best or len(next_path) < len(best[nxt][0]):
                    best[nxt] = (next_path, next_hashes)
                continue
            queue.append((nxt, next_path, next_hashes))

    labels = {node["address"]: node for node in graph.get("nodes") or []}
    paths = [
        {
            "address": endpoint,
            "kind": kinds.get(endpoint, ""),
            "entity": labels.get(endpoint, {}).get("entity") or "",
            "label": labels.get(endpoint, {}).get("label") or "",
            "hops": len(path) - 1,
            "path": path,
            "tx_hashes": hashes,
            "in_usd": inbound.get(endpoint, 0.0),
        }
        for endpoint, (path, hashes) in best.items()
    ]
    return sorted(paths, key=lambda item: (item["hops"], -item["in_usd"]))
