"""Deployer lineage: token -> deployer -> funder -> CEX/bridge.

The question a launch investigation actually asks is not "what did the
deployer trade" but "who paid the gas, and does that payer run other
launches". This module answers it in one call:

1. Resolve the deployer (input may be a token address or the deployer EOA)
   from ``deployer_events``, with the deployment count and tx evidence.
2. Trace the deployer to ``depth`` hops (both directions) with
   :func:`lite_indexer.query.trace.trace` — that supplies the funder, the
   funder's own counterparties, and the exit paths to a CEX/bridge.
3. Flag a *shared* funder: the same payer funding several deployers is the
   strongest cheap signal of one operator behind many launches.

Solana note: shared-funder detection needs a full transfer graph per
candidate wallet. The archive tier answers "who funded this wallet", not
"who else did
this payer fund", so for Solana the flag is computed only from our own
tables and is reported as unavailable when they cannot supply it.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from lite_indexer.query.trace import default_run, trace
from lite_indexer.query.trace_sql import sql_for

Run = Callable[[str, dict[str, Any]], list[dict[str, Any]]]

#: A deployer above this many distinct tokens is a factory/launcher, not a
#: one-off team wallet.
SERIAL_DEPLOYER_TOKENS = 50


def _funder(edges: list[dict[str, Any]], deployer: str) -> dict[str, Any] | None:
    """Earliest inbound movement that paid the deployer's way.

    Native value (SOL/ETH/traces) is preferred over token transfers: gas and
    rent come from native, so a 0.17 USDC transfer is a payment, not funding.
    This is a heuristic over the fetched window — it says "earliest inbound
    we hold", never "the wallet's original funder".
    """
    inbound = [e for e in edges if e["to"] == deployer and e["from"] != deployer]
    if not inbound:
        return None
    native = [e for e in inbound if e["asset"] == "native"]
    pool = native or inbound
    pool.sort(key=lambda e: (e.get("ts") or "", e["tx_hash"]))
    first = pool[0]
    return {
        "address": first["from"],
        "tx_hash": first["tx_hash"],
        "ts": first["ts"],
        "asset": first["asset"],
        "symbol": first["symbol"],
        "amount": first["amount"],
        "usd": first["usd"],
        "kind": first["kind"],
        "heuristic": "earliest_native_inbound" if native else "earliest_inbound_in_window",
    }


def _shared(run: Run, chain: str, funder: str, deployer: str) -> dict[str, Any]:
    if chain == "solana":
        return {
            "funder": funder,
            "available": False,
            "reason": "solana shared-funder needs a full payer graph (tier is per-wallet)",
        }
    try:
        sql, params = sql_for("shared_funder", chain=chain, funder=funder, exclude=deployer, limit=50)
        others = [str(row["deployer"]) for row in run(sql, params) if row.get("deployer")]
    except Exception as exc:  # noqa: BLE001 — a missing flag must not fail the lineage
        return {"funder": funder, "available": False, "reason": str(exc)[:90]}
    return {
        "funder": funder,
        "available": True,
        "count": len(others),
        "also_funded_deployers": others,
    }


def deployer_path(
    target: str,
    chain: str,
    *,
    depth: int = 2,
    since_days: int = 30,
    run: Run | None = None,
) -> dict[str, Any]:
    """Trace deployer lineage for a token or a deployer address.

    Args:
        target: Token address or deployer EOA.
        chain: Chain name (``ethereum``, ``base``, ``bsc``, ``solana``, ...).
        depth: Hops traced from the deployer (2 = deployer -> funder -> exit).
        since_days: Lookback for the transfer window.
        run: Executor override (tests).

    Returns:
        ``{target, chain, deployer, deployments, sample_deployments, funder,
        shared_funder, flags, graph, latency_ms}``. ``deployer`` is empty
        and ``found`` false when the address never appears in
        ``deployer_events``.
    """
    started = time.perf_counter()
    run = run or default_run()
    addr = target.strip() if chain == "solana" else target.strip().lower()
    notes: list[str] = []

    sql, params = sql_for("deployer_of_token", chain=chain, target=addr, limit=5)
    token_rows = run(sql, params)
    deployer = str(token_rows[0]["deployer"]) if token_rows else addr
    if token_rows:
        notes.append("resolved deployer from token lookup")
    else:
        notes.append("no token row; treating input as the deployer address")

    sql, params = sql_for("deployments", chain=chain, address=deployer)
    summary = (run(sql, params) or [{}])[0]
    tokens = int(summary.get("tokens") or 0)

    sql, params = sql_for("deployment_sample", chain=chain, address=deployer, limit=5)
    samples = [
        {
            "token_address": str(row.get("token_address") or ""),
            "tx_hash": str(row.get("tx_hash") or ""),
            "ts": str(row.get("block_timestamp") or ""),
            "type": str(row.get("type") or ""),
        }
        for row in run(sql, params)
    ]

    if not tokens:
        return {
            "target": addr,
            "chain": chain,
            "found": False,
            "deployer": deployer,
            "deployments": {"tokens": 0},
            "notes": notes + ["deployer_events has no row for this address"],
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }

    graph = trace(deployer, chain, depth=depth, direction="both", since_days=since_days, run=run)
    funder = _funder(graph["edges"], deployer)
    shared = _shared(run, chain, funder["address"], deployer) if funder else None

    labels = {node["address"]: node for node in graph["nodes"]}
    if funder:
        node = labels.get(funder["address"]) or {}
        funder["label"] = node.get("label") or ""
        funder["entity"] = node.get("entity") or ""
        funder["node_kind"] = node.get("kind") or "wallet"

    return {
        "target": addr,
        "chain": chain,
        "found": True,
        "deployer": deployer,
        "deployments": {
            "tokens": tokens,
            "rows": int(summary.get("rows") or 0),
            "first_ts": str(summary.get("first_ts") or ""),
            "last_ts": str(summary.get("last_ts") or ""),
            "factory": summary.get("factory"),
        },
        "sample_deployments": samples,
        "funder": funder,
        "shared_funder": shared,
        "flags": {
            "serial_deployer": tokens >= SERIAL_DEPLOYER_TOKENS,
            "shared_funder": bool(shared and shared.get("count")),
            "funder_is_exchange": bool(funder and funder.get("node_kind") == "exchange"),
        },
        "paths_to_cex": graph["paths_to_cex"],
        "graph": {"nodes": graph["nodes"], "edges": graph["edges"]},
        "provenance": graph["provenance"],
        "notes": notes + graph["provenance"].get("notes", []),
        "latency_ms": round((time.perf_counter() - started) * 1000, 1),
    }
