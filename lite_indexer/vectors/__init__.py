"""
Vector store adapter — LanceDB for AI-vector-ready chain indexing.

LanceDB is an embedded, zero-cost, disk-based vector database with native
columnar storage and full SQL + ANN search. It complements ClickHouse:
ClickHouse answers analytical SQL over the event warehouse, LanceDB answers
semantic / ANN queries over vectorized chain data.

Key components:

* :class:`LanceVectorStore` — embedded vector store (create, index, ANN
  search, BM25+vector hybrid search, similarity by id).
* :class:`OllamaEmbedder` / :class:`OpenAICompatEmbedder` — embedding
  providers (local Ollama ``nomic-embed-text`` by default, any
  OpenAI-compatible API as an alternative).
* :mod:`lite_indexer.vectors.collections` — pre-built schemas for wallet,
  contract, transaction, and token embeddings.
"""

from lite_indexer.vectors.config import (
    COLLECTION_CONTRACTS,
    COLLECTION_TOKENS,
    COLLECTION_TRANSACTIONS,
    COLLECTION_WALLETS,
    EMBEDDING_DIM,
    EMBEDDING_MODEL,
    EMBEDDING_PROVIDER,
    LANCE_DIR,
)
from lite_indexer.vectors.embedder import (
    EmbeddingError,
    OllamaEmbedder,
    OpenAICompatEmbedder,
    create_embedder,
)
from lite_indexer.vectors.lance_store import CollectionNotFoundError, LanceVectorStore

__all__ = [
    "LanceVectorStore",
    "CollectionNotFoundError",
    "EmbeddingError",
    "OllamaEmbedder",
    "OpenAICompatEmbedder",
    "create_embedder",
    "LANCE_DIR",
    "EMBEDDING_DIM",
    "EMBEDDING_MODEL",
    "EMBEDDING_PROVIDER",
    "COLLECTION_WALLETS",
    "COLLECTION_CONTRACTS",
    "COLLECTION_TRANSACTIONS",
    "COLLECTION_TOKENS",
]
