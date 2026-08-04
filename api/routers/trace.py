"""Cross-chain fund tracing endpoints.

Wraps :class:`lite_indexer.forensics.FundTracer` over the read-only
ClickHouse connection used by the rest of the API. Every endpoint returns
Arkham-style graph / interaction data derived entirely from the warehouse
(``evm_transfers``, ``internal_transfers``, ``solana_spl_balance_changes``,
``dex_events``, ``wallet_labels``, ``scam_addresses``) — no external
intelligence APIs.

Endpoints:

* ``GET /api/v1/trace/{tx_hash}`` — forward (default) or backward trace
  from a transaction hash. Forward returns the downstream movement graph;
  the same path also reports the bridge/CEX classifications.
* ``GET /api/v1/trace/address/{address}/backward`` — upstream funding tree.
* ``GET /api/v1/trace/address/{address}/bridges`` — bridge interactions.
* ``GET /api/v1/trace/address/{address}/cex`` — CEX deposit detection.
* ``GET /api/v1/trace/path`` — BFS cross-chain path between two wallets.

Tracing is expensive (multi-query fan-out), so the endpoints are rate
limited more strictly than the default 100/min.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from api.db import query
from api.params import normalize_date
from api.schemas import (
    BridgeInteractionOut,
    CexDepositOut,
    TraceEdgeOut,
    TraceGraphOut,
    TraceNodeOut,
)
from lite_indexer.forensics import FundTracer

router = APIRouter(prefix="/api/v1/trace", tags=["trace"])

_tracer: FundTracer | None = None
_tracer_error: str | None = None


def get_tracer() -> FundTracer:
    """Return the shared tracer (lazy singleton, wired to api.db.query)."""
    global _tracer, _tracer_error
    if _tracer is None and _tracer_error is None:
        try:
            _tracer = FundTracer(query=query)
        except Exception as exc:  # noqa: BLE001 — surface init failures
            _tracer_error = str(exc)
            raise HTTPException(status_code=503, detail=f"fund tracer unavailable: {exc}") from exc
    if _tracer is None:
        raise HTTPException(status_code=503, detail=f"fund tracer unavailable: {_tracer_error}")
    return _tracer


def _graph_out(graph: Any) -> TraceGraphOut:
    """Convert a tracer TraceGraph into the response model."""
    data = graph.to_dict()
    return TraceGraphOut(
        nodes=[TraceNodeOut(**n) for n in data["nodes"]],
        edges=[TraceEdgeOut(**e) for e in data["edges"]],
        stats=data["stats"],
    )


@router.get("/{tx_hash}", response_model=TraceGraphOut, summary="Trace funds from a transaction")
async def trace_tx(
    request: Request,
    tx_hash: str,
    chain: str = Query(default="ethereum", description="Chain the transaction is on"),
    direction: str = Query(default="forward", pattern="^(forward|backward)$"),
    depth: int = Query(default=5, ge=1, le=10, description="Max expansion depth"),
    max_hops: int = Query(default=100, ge=1, le=500, description="Max graph edges"),
) -> TraceGraphOut:
    """Trace funds forward (downstream) or backward from a transaction hash."""
    tracer = get_tracer()
    if direction == "forward":
        graph = await tracer.trace_forward(tx_hash, chain, max_depth=depth, max_hops=max_hops)
    else:
        # Backward from a tx: root is the tx sender, windowed around it.
        window = tracer.tx_window(tx_hash, chain)
        if window is None:
            raise HTTPException(status_code=404, detail="no transfer events found for tx")
        root, start, end = window
        graph = await tracer.trace_backward(root, start, end, max_depth=depth)
    return _graph_out(graph)


@router.get("/address/{address}/backward", response_model=TraceGraphOut, summary="Upstream funding tree")
async def trace_backward_address(
    request: Request,
    address: str,
    start_time: str = Query(default=None, description="Window start (YYYY-MM-DD or ISO)"),
    end_time: str = Query(default=None, description="Window end (YYYY-MM-DD or ISO)"),
    depth: int = Query(default=5, ge=1, le=10),
) -> TraceGraphOut:
    """Trace funds backward: where did this address get its money?"""
    end = normalize_date(end_time) or datetime.now(UTC)
    start = normalize_date(start_time) or (end.replace(year=end.year - 1))
    graph = await get_tracer().trace_backward(address, start, end, max_depth=depth)
    return _graph_out(graph)


@router.get(
    "/address/{address}/bridges",
    response_model=list[BridgeInteractionOut],
    summary="Bridge usage for a wallet",
)
async def bridge_usage(request: Request, address: str) -> list[BridgeInteractionOut]:
    """Identify all bridge interactions for a wallet (deposits + withdrawals)."""
    interactions = await get_tracer().detect_bridge_usage(address)
    return [BridgeInteractionOut(**i.to_dict()) for i in interactions]


@router.get(
    "/address/{address}/cex",
    response_model=list[CexDepositOut],
    summary="CEX deposits from a wallet",
)
async def cex_deposits(request: Request, address: str) -> list[CexDepositOut]:
    """Detect deposits from a wallet to known CEX hot wallets."""
    deposits = await get_tracer().detect_cex_deposits(address)
    return [CexDepositOut(**d.to_dict()) for d in deposits]


@router.get("/path", response_model=TraceGraphOut, summary="Cross-chain path between wallets")
async def cross_chain_path(
    request: Request,
    from_address: str = Query(..., description="Starting wallet"),
    to_address: str = Query(..., description="Target wallet (any chain)"),
    max_depth: int = Query(default=5, ge=1, le=8),
) -> TraceGraphOut:
    """BFS path between two wallets across chains (via bridges where needed)."""
    graph = await get_tracer().find_cross_chain_path(from_address, to_address, max_depth=max_depth)
    if graph is None:
        raise HTTPException(status_code=404, detail="no path found between addresses")
    return _graph_out(graph)
