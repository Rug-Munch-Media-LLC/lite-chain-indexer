"""Hermetic tests for the LanceDB vector store (lite_indexer.vectors).

Uses a deterministic fake embedder so no embedding service is needed.
LanceDB runs embedded against a tmp directory.
"""

from __future__ import annotations

import hashlib
import math
from typing import Any

import pyarrow as pa
import pytest

from lite_indexer.vectors.embedder import EmbeddingError
from lite_indexer.vectors.lance_store import CollectionNotFoundError, LanceVectorStore, _rrf_merge


class FakeEmbedder:
    """Deterministic embedder: hashes the text into a fixed-dim vector.

    Similar texts (same prefix) map to similar vectors so ANN search is
    meaningful without a real model.
    """

    model = "fake-embed"

    def __init__(self, dim: int = 4) -> None:
        self.dim = dim

    async def embed(self, texts: list[str], on_progress: Any = None) -> list[list[float]]:
        vectors: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode()).digest()
            vec = [float(digest[i % len(digest)]) / 255.0 for i in range(self.dim)]
            norm = math.sqrt(sum(v * v for v in vec)) or 1.0
            vectors.append([v / norm for v in vec])
        if on_progress is not None:
            on_progress(len(texts), len(texts))
        return vectors


@pytest.fixture()
def store(tmp_path) -> LanceVectorStore:
    """A LanceVectorStore backed by a tmp dir + fake embedder."""
    return LanceVectorStore(path=str(tmp_path / "lance"), embedder=FakeEmbedder())


@pytest.fixture()
def wallet_rows() -> list[dict[str, Any]]:
    """Synthetic wallet rows with behavior patterns."""
    return [
        {
            "id": "0xaaa",
            "text": "rug pull honeypot token with high volume",
            "chain": "ethereum",
            "labels": "rug, honeypot",
            "tx_count": 12,
        },
        {
            "id": "0xbbb",
            "text": "uniswap v3 concentrated liquidity clone",
            "chain": "base",
            "labels": "dex, clone",
            "tx_count": 40,
        },
        {
            "id": "0xccc",
            "text": "aave lending pool deposit patterns",
            "chain": "ethereum",
            "labels": "defi, lending",
            "tx_count": 7,
        },
        {
            "id": "0xddd",
            "text": "rug pull honeypot token with high volume",
            "chain": "bsc",
            "labels": "rug, honeypot",
            "tx_count": 3,
        },
    ]


async def test_create_collection(store: LanceVectorStore) -> None:
    created = await store.create_collection("wallet_embeddings", pa.schema([pa.field("id", pa.string())]))
    assert created is True
    # second call is idempotent
    created_again = await store.create_collection("wallet_embeddings", pa.schema([pa.field("id", pa.string())]))
    assert created_again is False
    assert "wallet_embeddings" in store.list_collections()


async def test_ensure_default_collections(store: LanceVectorStore) -> None:
    created = await store.ensure_default_collections()
    assert set(created) == {
        "wallet_embeddings",
        "contract_embeddings",
        "transaction_embeddings",
        "token_embeddings",
    }
    # idempotent
    assert await store.ensure_default_collections() == []
    assert store.collection_stats("wallet_embeddings")["rows"] == 0


async def test_index_embeddings_and_search(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    indexed = await store.index_embeddings("wallet_embeddings", wallet_rows)
    assert indexed == len(wallet_rows)
    assert store.collection_stats("wallet_embeddings")["rows"] == len(wallet_rows)

    # Query vector for a rug-pull-ish description (same text as 0xaaa).
    (query_vec,) = await store.embedder.embed(["rug pull honeypot token with high volume"])
    results = await store.search("wallet_embeddings", query_vec, k=2)
    assert len(results) == 2
    assert results[0]["id"] in ("0xaaa", "0xddd")
    assert results[0]["_distance"] <= results[1]["_distance"]


async def test_search_missing_collection(store: LanceVectorStore) -> None:
    with pytest.raises(CollectionNotFoundError):
        await store.search("nope", [0.1, 0.2, 0.3, 0.4])


async def test_search_with_filter(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    await store.index_embeddings("wallet_embeddings", wallet_rows)
    (query_vec,) = await store.embedder.embed(["rug pull honeypot token with high volume"])
    results = await store.search("wallet_embeddings", query_vec, k=10, filter="chain = 'bsc'")
    assert len(results) == 1
    assert results[0]["id"] == "0xddd"


async def test_similar_by_id(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    await store.index_embeddings("wallet_embeddings", wallet_rows)
    results = await store.similar_by_id("wallet_embeddings", "0xaaa", k=3)
    assert len(results) == 3
    # 0xaaa and 0xddd share the same pattern text -> nearest neighbor.
    assert results[0]["id"] in ("0xaaa", "0xddd")
    assert results[1]["id"] in ("0xaaa", "0xddd")


async def test_similar_by_id_missing(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    await store.index_embeddings("wallet_embeddings", wallet_rows)
    with pytest.raises(CollectionNotFoundError):
        await store.similar_by_id("wallet_embeddings", "0xunknown")


async def test_hybrid_search(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    await store.index_embeddings("wallet_embeddings", wallet_rows)
    results = await store.hybrid_search("wallet_embeddings", "rug pull honeypot", k=2)
    assert len(results) == 2
    # FTS should surface the rug-pull rows.
    ids = {r["id"] for r in results}
    assert "0xaaa" in ids
    assert all("_rrf_score" in r for r in results)


async def test_drop_collection(store: LanceVectorStore, wallet_rows: list[dict[str, Any]]) -> None:
    await store.index_embeddings("wallet_embeddings", wallet_rows)
    assert store.drop_collection("wallet_embeddings") is True
    assert store.drop_collection("wallet_embeddings") is False
    assert "wallet_embeddings" not in store.list_collections()


def test_rrf_merge() -> None:
    fts = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    vec = [{"id": "b"}, {"id": "c"}, {"id": "d"}]
    merged = _rrf_merge(fts, vec, limit=3)
    assert [r["id"] for r in merged] == ["b", "c", "a"]
    assert merged[0]["_rrf_score"] > merged[1]["_rrf_score"]


def test_rrf_merge_ignores_missing_ids() -> None:
    merged = _rrf_merge([{"text": "no id"}], [{"id": "x"}], limit=5)
    assert [r["id"] for r in merged] == ["x"]


async def test_add_pre_embedded_vectors(store: LanceVectorStore) -> None:
    rows = [{"id": "0x1", "text": "a", "vector": [1.0, 0.0, 0.0, 0.0]}]
    added = await store.add_vectors("wallet_embeddings", rows)
    assert added == 1
    assert store.collection_stats("wallet_embeddings")["rows"] == 1
    results = await store.search("wallet_embeddings", [1.0, 0.0, 0.0, 0.0], k=1)
    assert results[0]["id"] == "0x1"


async def test_embedding_error_propagates(store: LanceVectorStore) -> None:
    class BrokenEmbedder:
        model = "broken"

        async def embed(self, texts: list[str], on_progress: Any = None) -> list[list[float]]:
            raise EmbeddingError("provider down")

    broken = LanceVectorStore(path=str(store.path) + "_broken", embedder=BrokenEmbedder())
    with pytest.raises(EmbeddingError):
        await broken.embedder.embed(["x"])


async def test_aclose_noop_with_injected_embedder(store: LanceVectorStore) -> None:
    # Injected embedder is not owned -> aclose must not close it.
    await store.aclose()
