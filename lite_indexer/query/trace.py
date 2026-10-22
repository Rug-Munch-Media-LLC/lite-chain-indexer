"""trace() — one wallet, depth-bounded graph, every tier we actually own.

Contract (stable; the CLI and the API both call this):

    trace(address, chain, depth=3, direction="both") -> {
        nodes: [{address, chain, kind, label, entity, entity_type,
                 sanctioned, hops, in_usd, out_usd, tx_count}],
        edges: [{from, to, asset, symbol, amount, usd, tx_hash, ts, kind,
                 hop, tx_count, tiers}],
        paths_to_cex: [{address, kind, entity, hops, path, tx_hashes}],
        provenance: {tiers, rows, edges, depth, direction, latency_ms, ...},
    }

Source policy per chain (no tier is invented, absence is reported):

* EVM — ClickHouse ``evm_transfers`` + ``internal_transfers`` for the recent
  window; optionally BigLake (ethereum only) for history older than it.
* Solana — Helius enhanced transactions (native SOL + SPL). Our tables store
  SPL balance deltas without a counterparty and no native SOL at all.

Expansion is level-synchronous: one query per hop over the whole frontier,
not one per address, so latency stays flat as the graph widens.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from lite_indexer.logging import get_logger
from lite_indexer.query.trace_evm import fetch_evm, fetch_lake
from lite_indexer.query.trace_labels import resolve_entities
from lite_indexer.query.trace_model import build_graph
from lite_indexer.query.trace_paths import paths_to_cex
from lite_indexer.query.trace_solana import fetch_solana
from lite_indexer.query.trace_sql import sql_for

logger = get_logger(__name__)

Run = Callable[[str, dict[str, Any]], list[dict[str, Any]]]
DIRECTIONS = ("both", "in", "out")

__all__ = ["trace", "default_run", "DIRECTIONS"]


def default_run() -> Run:
    """Build a ClickHouse reader from the indexer env (lazy import)."""
    import os

    from clickhouse_driver import Client

    client = Client(
        host=os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"),
        user=os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or "",
        database=os.getenv("CH_DATABASE", "wallet_memory"),
        connect_timeout=5,
        send_receive_timeout=60,
        settings={"max_execution_time": 60},
    )

    def run(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        rows, columns = client.execute(sql, params, with_column_types=True)
        names = [name for name, _ in columns]
        return [dict(zip(names, row, strict=False)) for row in rows]

    return run


def _norm(chain: str, address: str) -> str:
    return address.strip() if chain == "solana" else address.strip().lower()


def _touches(edge: dict[str, Any], frontier: set[str], direction: str) -> bool:
    if direction == "out":
        return edge["from"] in frontier
    if direction == "in":
        return edge["to"] in frontier
    return edge["from"] in frontier or edge["to"] in frontier


def _candidates(edge: dict[str, Any], frontier: set[str], seen: set[str]) -> list[str]:
    out = []
    for addr in (edge["from"], edge["to"]):
        if addr in frontier or addr in seen:
            continue
        out.append(addr)
    return out


def _rank(edges: list[dict[str, Any]], fanout: int) -> list[str]:
    weights: dict[str, float] = {}
    for edge in edges:
        for addr in (edge["from"], edge["to"]):
            weights[addr] = (
                weights.get(addr, 0.0) + float(edge.get("usd") or 0.0) + float(edge["amount"])
            )
    return [
        addr for addr, _ in sorted(weights.items(), key=lambda kv: kv[1], reverse=True)[:fanout]
    ]


def _prices(run: Run, chain: str, assets: list[str]) -> tuple[dict[str, Any], dict[str, Any]]:
    tokens = sorted({a for a in assets if a != "native"})
    if not tokens:
        return {}, {}
    prices: dict[str, Any] = {}
    meta: dict[str, Any] = {}
    for kind, target in (("prices", prices), ("meta", meta)):
        try:
            sql, params = sql_for(kind, chain=chain, tokens=tokens)
            for row in run(sql, params):
                target[str(row["token_address"]).lower()] = row
        except Exception as exc:  # noqa: BLE001 — USD is best-effort; raw amounts remain
            logger.debug("trace_enrich_failed", kind=kind, error=str(exc)[:90])
    return prices, meta


def trace(
    address: str,
    chain: str,
    depth: int = 3,
    direction: str = "both",
    *,
    since_days: int = 7,
    max_nodes: int = 40,
    fanout: int = 6,
    limit: int = 400,
    lake_dates: list[str] | None = None,
    run: Run | None = None,
) -> dict[str, Any]:
    """Trace a wallet out to ``depth`` hops and return the graph + exits.

    Args:
        address: Wallet address (EVM hex or Solana base58).
        chain: Chain name as used by the indexer (``ethereum``, ``base``,
            ``solana``, ...).
        depth: Maximum hops from the root (1-4).
        direction: ``both`` (default), ``in`` (funding) or ``out`` (spending).
        since_days: ClickHouse lookback for the recent window.
        max_nodes: Hard cap on addresses in the graph.
        fanout: Max new addresses expanded per hop (ranked by value).
        limit: Per-query aggregated row ceiling.
        lake_dates: Explicit ``YYYY-MM-DD`` BigLake partitions to merge for
            ``ethereum`` history older than the ClickHouse window.
        run: Executor override (tests); defaults to a ClickHouse client.

    Returns:
        The graph contract described in the module docstring.
    """
    started = time.perf_counter()
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be one of {DIRECTIONS}")
    depth = max(1, min(int(depth), 4))
    root = _norm(chain, address)
    run = run or default_run()
    since = datetime.now(UTC) - timedelta(days=since_days)

    rows: list[dict[str, Any]] = []
    notes: list[str] = []
    hops: dict[str, int] = {root: 0}
    frontier = [root]
    seen = {root}

    for hop in range(1, depth + 1):
        if not frontier or len(seen) >= max_nodes:
            break
        level_rows: list[dict[str, Any]] = []
        if chain == "solana":
            for addr in frontier[:fanout]:
                found, level_notes = fetch_solana(addr, limit=limit)
                level_rows.extend(found)
                notes.extend(level_notes)
        else:
            found, level_notes = fetch_evm(run, chain, frontier, since, limit=limit)
            level_rows.extend(found)
            notes.extend(level_notes)

        frontier_set = set(frontier)
        touching = [row for row in level_rows if _touches(row, frontier_set, direction)]
        # One hop can return thousands of token movements; keep the value-
        # bearing ones so the graph stays readable and the JSON bounded.
        touching.sort(key=lambda r: (float(r.get("amount") or 0), str(r.get("ts") or "")), reverse=True)
        touching = touching[:limit]
        rows.extend(touching)
        for row in touching:
            for addr in (row["from"], row["to"]):
                hops.setdefault(addr, hop)
        candidates: list[str] = []
        for row in touching:
            candidates.extend(_candidates(row, frontier_set, seen))
        ranked = _rank([r for r in touching if r["from"] in seen or r["to"] in seen], fanout)
        new_nodes = [addr for addr in ranked if addr in set(candidates)][
            : max(0, max_nodes - len(seen))
        ]
        for addr in new_nodes:
            seen.add(addr)
            hops[addr] = hop
        frontier = new_nodes

    if lake_dates:
        lake_rows, lake_notes = fetch_lake(chain, root, lake_dates, limit=limit)
        rows.extend(lake_rows)
        notes.extend(lake_notes)

    assets = [row["asset"] for row in rows]
    prices, meta = _prices(run, chain, assets)
    labels = resolve_entities(run, sorted(seen))
    graph = build_graph(root, chain, rows, labels, prices=prices, meta=meta, hops=hops)
    exits = paths_to_cex(graph, root, max_hops=depth)

    tiers: dict[str, int] = {}
    for row in rows:
        tiers[row["tier"]] = tiers.get(row["tier"], 0) + 1
    latency_ms = round((time.perf_counter() - started) * 1000, 1)
    logger.info(
        "trace_done",
        chain=chain,
        nodes=len(graph["nodes"]),
        edges=len(graph["edges"]),
        latency_ms=latency_ms,
    )

    return {
        "address": root,
        "chain": chain,
        "depth": depth,
        "direction": direction,
        "since_days": since_days,
        "nodes": graph["nodes"],
        "edges": graph["edges"],
        "paths_to_cex": exits,
        "provenance": {
            "tiers": tiers,
            "row_count": len(rows),
            "edge_count": len(graph["edges"]),
            "node_count": len(graph["nodes"]),
            "labels_resolved": len(labels),
            "usd_coverage": sum(1 for row in graph["edges"] if row["usd"] is not None),
            "latency_ms": latency_ms,
            "notes": sorted(set(notes))[:12],
        },
    }
