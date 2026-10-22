"""Graph assembly for the wallet trace — rows in, nodes/edges out.

Pure functions only: no DB, no HTTP. The orchestrator hands over normalized
edge rows (one per ``(from, to, asset)`` per tier) plus the resolved entity
labels, and this module produces the JSON contract the API/CLI returns.

Deduplication is by ``(tx_hash, from, to, asset)``: the same movement seen by
ClickHouse *and* BigLake (or by Helius and a local table) is one edge, and
the tier list records every source that saw it. Amounts are never summed
across tiers — the larger value wins, because duplicate ingestion always
duplicates downward (a lake row is never *more* precise than a local one).
"""

from __future__ import annotations

from typing import Any

#: Label category -> node kind. ``wallet_labels`` carries the curated
#: exchange/mixer/bridge taxonomy; anything unlisted stays a plain wallet.
_KIND_BY_CATEGORY: dict[str, str] = {
    "exchange": "exchange",
    "cex_hot_wallet": "exchange",
    "p2p_service": "exchange",
    "bridge": "bridge",
    "mixer": "mixer",
    "defi": "protocol",
    "lending": "protocol",
    "stablecoin": "token",
    "mining_pool": "mining_pool",
}

#: Categories that raise a risk flag regardless of node kind.
_RISK_CATEGORIES = frozenset(
    {"sanctioned", "darknet_market", "ransomware", "money_laundering", "scam", "malicious"}
)

_STOP_KINDS = ("exchange", "bridge", "mixer")


def _decimals(asset: str, chain: str, meta: dict[str, Any]) -> int:
    if asset == "native":
        return 9 if chain == "solana" else 18
    row = meta.get(asset) or {}
    value = row.get("decimals")
    if value is None:
        return 9 if chain == "solana" else 18
    return int(value)


def _amount(row: dict[str, Any], chain: str, meta: dict[str, Any]) -> float:
    """Human amount for an edge row (pre-scaled sources keep their value)."""
    if row.get("amount") is not None:
        return float(row["amount"])
    raw = row.get("raw") or 0
    return float(raw) / float(10 ** _decimals(str(row.get("asset") or "native"), chain, meta))


def _usd(asset: str, amount: float, prices: dict[str, Any]) -> float | None:
    price = (prices.get(asset) or {}).get("price_usd")
    if price is None:
        return None
    return round(float(amount) * float(price), 2)


def _kind(address: str, labels: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    label = labels.get(address) or {}
    kind = label.get("kind") or "wallet"
    return kind, label


def _node(address: str, chain: str, labels: dict[str, Any], hops: dict[str, int]) -> dict[str, Any]:
    kind, label = _kind(address, labels)
    return {
        "address": address,
        "chain": chain,
        "kind": kind,
        "label": label.get("label") or "",
        "entity": label.get("entity") or "",
        "entity_type": label.get("entity_type") or "",
        "sanctioned": bool(label.get("sanctioned")),
        "risk": sorted(label.get("risk") or []),
        "hops": hops.get(address),
        "in_usd": 0.0,
        "out_usd": 0.0,
        "tx_count": 0,
    }


def _merge_edge(
    edges: dict[tuple[str, str, str, str], dict[str, Any]], edge: dict[str, Any]
) -> None:
    key = (edge["tx_hash"], edge["from"], edge["to"], edge["asset"])
    seen = edges.get(key)
    if seen is None:
        edges[key] = edge
        return
    if (edge.get("usd") or 0) > (seen.get("usd") or 0):
        seen["amount"] = max(float(seen["amount"]), float(edge["amount"]))
        seen["usd"] = edge.get("usd") if edge.get("usd") is not None else seen.get("usd")
    for tier in edge.get("tiers") or []:
        if tier not in seen["tiers"]:
            seen["tiers"].append(tier)


def build_graph(
    root: str,
    chain: str,
    rows: list[dict[str, Any]],
    labels: dict[str, Any],
    *,
    prices: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
    hops: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Assemble the trace graph from normalized edge rows.

    Args:
        root: The traced address (always present in ``nodes``).
        chain: Chain the rows came from (one chain per trace call).
        rows: Edge rows — ``from``, ``to``, ``asset``, ``raw``/``amount``,
            ``tx_hash``, ``ts``, ``kind``, ``tier``, ``tx_count``.
        labels: ``{address: {kind, label, entity, entity_type, ...}}``.
        prices: ``{asset: {price_usd}}`` for USD conversion (optional).
        meta: ``{asset: {decimals}}`` for raw-unit scaling (optional).
        hops: ``{address: hop}`` — hop at which the node was reached.

    Returns:
        ``{"nodes": [...], "edges": [...], "stop_addresses": [...]}``.
    """
    prices = prices or {}
    meta = meta or {}
    hops = hops or {root: 0}
    merged: dict[tuple[str, str, str, str], dict[str, Any]] = {}

    for row in rows:
        src, dst = str(row.get("from") or ""), str(row.get("to") or "")
        if not src or not dst or src == dst:
            continue
        asset = str(row.get("asset") or "native")
        amount = _amount(row, chain, meta)
        edge = {
            "from": src,
            "to": dst,
            "asset": asset,
            "symbol": str(
                row.get("symbol") or ("SOL" if asset == "native" and chain == "solana" else "")
            ),
            "amount": round(amount, 8),
            "usd": _usd(asset, amount, prices),
            "tx_hash": str(row.get("tx_hash") or ""),
            "ts": str(row.get("ts") or ""),
            "kind": str(row.get("kind") or ""),
            "tx_count": int(row.get("tx_count") or 1),
            "hop": hops.get(dst if src == root else src),
            "tiers": [str(row.get("tier") or "")],
        }
        _merge_edge(merged, edge)

    edges = list(merged.values())
    addresses = {root}
    for edge in edges:
        addresses.add(edge["from"])
        addresses.add(edge["to"])
    nodes = {addr: _node(addr, chain, labels, hops) for addr in addresses}
    nodes[root]["is_root"] = True

    for edge in edges:
        value = edge.get("usd")
        nodes[edge["from"]]["out_usd"] += value or 0.0
        nodes[edge["to"]]["in_usd"] += value or 0.0
        nodes[edge["from"]]["tx_count"] += edge["tx_count"]
        nodes[edge["to"]]["tx_count"] += edge["tx_count"]

    for node in nodes.values():
        node["in_usd"] = round(node["in_usd"], 2)
        node["out_usd"] = round(node["out_usd"], 2)

    return {
        "nodes": sorted(
            nodes.values(), key=lambda n: (n["hops"] if n["hops"] is not None else 99, n["address"])
        ),
        "edges": edges,
        "stop_addresses": sorted(a for a, n in nodes.items() if n["kind"] in _STOP_KINDS),
    }
