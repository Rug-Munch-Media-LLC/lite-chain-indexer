"""Vector search endpoints over the LanceDB store.

These endpoints make the vectorized chain data queryable over HTTP:

* ``GET /api/v1/search/wallets?q=...`` — semantic wallet search
* ``GET /api/v1/search/contracts?q=...`` — find similar contracts
* ``GET /api/v1/similar/wallets/{address}`` — wallets with similar behavior
* ``GET /api/v1/similar/contracts/{contract_address}`` — contract clones

The store is a lazy singleton (created on first request). When LanceDB is
unavailable (not installed, or the directory is unreachable) the endpoints
return 503 with a clear message — the rest of the API keeps working.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from api.config import (
    LANCE_DIR,
    VECTOR_SEARCH_ENABLED,
)
from lite_indexer.vectors.embedder import EmbeddingError, create_embedder
from lite_indexer.vectors.lance_store import CollectionNotFoundError, LanceVectorStore

router = APIRouter(prefix="/api/v1", tags=["search"])

_store: LanceVectorStore | None = None
_store_error: str | None = None


def get_store() -> LanceVectorStore:
    """Return the lazy singleton vector store, or raise HTTP 503."""
    global _store, _store_error
    if not VECTOR_SEARCH_ENABLED:
        raise HTTPException(status_code=503, detail="vector search disabled (VECTOR_SEARCH_ENABLED=0)")
    if _store is None and _store_error is None:
        try:
            _store = LanceVectorStore(path=LANCE_DIR, embedder=create_embedder())
        except Exception as exc:  # noqa: BLE001 — surface any LanceDB init failure
            _store_error = str(exc)
            raise HTTPException(status_code=503, detail=f"vector store unavailable: {exc}") from exc
    if _store is None:
        raise HTTPException(status_code=503, detail=f"vector store unavailable: {_store_error}")
    return _store


async def _embed_query(query: str) -> list[float]:
    """Embed the user's text query for ANN search."""
    store = get_store()
    try:
        (vector,) = await store.embedder.embed([query])
        return vector
    except EmbeddingError as exc:
        raise HTTPException(status_code=503, detail=f"embedding failed: {exc}") from exc


def _hits(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize LanceDB rows into JSON-safe hit objects."""
    hits: list[dict[str, Any]] = []
    for row in rows:
        hits.append(
            {
                "id": row.get("id"),
                "text": row.get("text"),
                "score": row.get("_distance") or row.get("_rrf_score") or row.get("_score"),
                "metadata": {
                    k: v for k, v in row.items() if not k.startswith("_") and k not in ("id", "text", "vector")
                },
            }
        )
    return hits


@router.get("/search/wallets", summary="Semantic wallet search")
async def search_wallets(
    q: str = Query(..., min_length=3, description="Natural-language query (e.g. 'rug pull pattern')"),
    k: int = Query(default=10, ge=1, le=100, description="Number of results"),
) -> dict[str, Any]:
    """Search wallet behavior embeddings by semantic query."""
    vector = await _embed_query(q)
    store = get_store()
    try:
        rows = await store.search("wallet_embeddings", vector, k=k)
    except CollectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"query": q, "count": len(rows), "hits": _hits(rows)}


@router.get("/search/contracts", summary="Find similar contracts")
async def search_contracts(
    q: str = Query(..., min_length=3, description="Natural-language query (e.g. 'uniswap v3 clone')"),
    k: int = Query(default=10, ge=1, le=100, description="Number of results"),
) -> dict[str, Any]:
    """Search contract embeddings by semantic query."""
    vector = await _embed_query(q)
    store = get_store()
    try:
        rows = await store.search("contract_embeddings", vector, k=k)
    except CollectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"query": q, "count": len(rows), "hits": _hits(rows)}


@router.get("/similar/wallets/{address}", summary="Wallets with similar behavior")
async def similar_wallets(
    address: str,
    k: int = Query(default=10, ge=1, le=100, description="Number of results"),
) -> dict[str, Any]:
    """Find wallets whose transaction behavior resembles ``address``."""
    store = get_store()
    try:
        rows = await store.similar_by_id("wallet_embeddings", address.lower(), k=k)
    except CollectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"address": address, "count": len(rows), "hits": _hits(rows)}


@router.get("/similar/contracts/{contract_address}", summary="Find contract clones")
async def similar_contracts(
    contract_address: str,
    k: int = Query(default=10, ge=1, le=100, description="Number of results"),
) -> dict[str, Any]:
    """Find contracts whose code/behavior resembles ``contract_address``."""
    store = get_store()
    try:
        rows = await store.similar_by_id("contract_embeddings", contract_address.lower(), k=k)
    except CollectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"address": contract_address, "count": len(rows), "hits": _hits(rows)}
