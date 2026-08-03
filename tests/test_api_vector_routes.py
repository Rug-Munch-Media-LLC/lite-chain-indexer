"""Hermetic route tests for the vector search endpoints.

The LanceDB store is replaced with a fake so no vector database or
embedding service is needed. Tests cover the four endpoints, the 404 path
(missing collection), the 503 path (store unavailable), and rate-limit
exemptions for health/metrics.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app


class FakeStore:
    """Minimal stand-in for LanceVectorStore with a fake embedder."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = rows or [
            {"id": "0xaaa", "text": "rug pull honeypot", "chain": "ethereum", "_distance": 0.1},
            {"id": "0xddd", "text": "rug pull honeypot", "chain": "bsc", "_distance": 0.2},
        ]

    class Embedder:
        model = "fake"

        async def embed(self, texts: list[str], on_progress: Any = None) -> list[list[float]]:
            return [[0.1, 0.2, 0.3, 0.4] for _ in texts]

    embedder = Embedder()

    async def search(
        self, collection: str, query_vec: list[float], k: int = 10, filter: str | None = None
    ) -> list[dict[str, Any]]:
        return [dict(r) for r in self.rows[:k]]

    async def similar_by_id(
        self, collection: str, row_id: str, k: int = 10, filter: str | None = None
    ) -> list[dict[str, Any]]:
        return [dict(r) for r in self.rows[:k]]


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with a fake vector store installed."""
    app = create_app()
    monkeypatch.setattr("api.routers.search._store", FakeStore())
    monkeypatch.setattr("api.routers.search._store_error", None)
    monkeypatch.setattr("api.routers.search.VECTOR_SEARCH_ENABLED", True)
    with TestClient(app) as c:
        yield c


def test_openapi_lists_search_endpoints(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/v1/search/wallets" in paths
    assert "/api/v1/search/contracts" in paths
    assert "/api/v1/similar/wallets/{address}" in paths
    assert "/api/v1/similar/contracts/{contract_address}" in paths


def test_search_wallets(client: TestClient) -> None:
    resp = client.get("/api/v1/search/wallets?q=rug+pull+pattern&k=5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["query"] == "rug pull pattern"
    assert body["count"] == 2
    assert body["hits"][0]["id"] == "0xaaa"
    assert body["hits"][0]["metadata"]["chain"] == "ethereum"
    assert body["hits"][0]["score"] == 0.1


def test_search_wallets_requires_query(client: TestClient) -> None:
    resp = client.get("/api/v1/search/wallets")
    assert resp.status_code == 422


def test_search_wallets_short_query(client: TestClient) -> None:
    resp = client.get("/api/v1/search/wallets?q=ab")
    assert resp.status_code == 422


def test_search_contracts(client: TestClient) -> None:
    resp = client.get("/api/v1/search/contracts?q=uniswap+v3+clone")
    assert resp.status_code == 200
    assert resp.json()["count"] == 2


def test_similar_wallets(client: TestClient) -> None:
    resp = client.get("/api/v1/similar/wallets/0xaaa")
    assert resp.status_code == 200
    body = resp.json()
    assert body["address"] == "0xaaa"
    assert body["hits"][0]["id"] == "0xaaa"


def test_similar_contracts(client: TestClient) -> None:
    resp = client.get("/api/v1/similar/contracts/0xfactory")
    assert resp.status_code == 200
    assert resp.json()["address"] == "0xfactory"


def test_search_collection_missing(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    class EmptyStore(FakeStore):
        async def search(
            self, collection: str, query_vec: list[float], k: int = 10, filter: str | None = None
        ) -> list[dict[str, Any]]:
            from lite_indexer.vectors.lance_store import CollectionNotFoundError

            raise CollectionNotFoundError(f"collection '{collection}' does not exist")

    monkeypatch.setattr("api.routers.search._store", EmptyStore())
    resp = client.get("/api/v1/search/wallets?q=rug+pull")
    assert resp.status_code == 404


def test_search_store_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app()
    monkeypatch.setattr("api.routers.search._store", None)
    monkeypatch.setattr("api.routers.search._store_error", "lancedb not installed")
    monkeypatch.setattr("api.routers.search.VECTOR_SEARCH_ENABLED", True)
    with TestClient(app) as c:
        resp = c.get("/api/v1/search/wallets?q=rug+pull")
    assert resp.status_code == 503
    assert "unavailable" in resp.json()["detail"]


def test_search_disabled_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app()
    monkeypatch.setattr("api.routers.search.VECTOR_SEARCH_ENABLED", False)
    with TestClient(app) as c:
        resp = c.get("/api/v1/search/wallets?q=rug+pull")
    assert resp.status_code == 503


def test_health_still_works_with_search_down(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app()
    monkeypatch.setattr("api.routers.search._store", None)
    monkeypatch.setattr("api.routers.search._store_error", "boom")
    with TestClient(app) as c:
        assert c.get("/health").status_code == 200
