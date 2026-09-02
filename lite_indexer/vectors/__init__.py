"""
Vector store adapter — LanceDB for AI-vector-ready chain indexing.

LanceDB is an embedded, zero-cost, disk-based vector database with native
columnar storage and full SQL + ANN search. It complements ClickHouse:
ClickHouse answers analytical SQL over the event warehouse, LanceDB answers
semantic / ANN queries over vectorized chain data.

Key components:

* :class:`LanceVectorStore` — embedded vector store (create, index, ANN
  search, BM25+vector hybrid search, similarity by id).
* :class:`DualLaneEmbedder` — dual-lane embedding provider: Qwen3-Embedding-4B
  (2560-dim) via the DeepInfra API (primary, fast) with a local llama.cpp
  backup lane on port 8004; returns ``None`` when both lanes fail so
  ingestion skips gracefully.
* :mod:`lite_indexer.vectors.collections` — pre-built schemas for wallet,
  contract, transaction, and token embeddings.
"""

from lite_indexer.vectors.config import (
    COLLECTION_CONTRACTS,
    COLLECTION_TOKENS,
    COLLECTION_TRANSACTIONS,
    COLLECTION_WALLETS,
    DEEPINFRA_EMBED_MODEL,
    EMBEDDING_DIM,
    EMBEDDING_MODE,
    LANCE_DIR,
    LOCAL_EMBED_MODEL,
)
from lite_indexer.vectors.embedder import (
    DualLaneEmbedder,
    EmbeddingError,
    create_embedder,
)
from lite_indexer.vectors.lance_store import CollectionNotFoundError, LanceVectorStore

__all__ = [
    "LanceVectorStore",
    "CollectionNotFoundError",
    "DualLaneEmbedder",
    "EmbeddingError",
    "create_embedder",
    "LANCE_DIR",
    "EMBEDDING_DIM",
    "EMBEDDING_MODE",
    "DEEPINFRA_EMBED_MODEL",
    "LOCAL_EMBED_MODEL",
    "COLLECTION_WALLETS",
    "COLLECTION_CONTRACTS",
    "COLLECTION_TRANSACTIONS",
    "COLLECTION_TOKENS",
]
