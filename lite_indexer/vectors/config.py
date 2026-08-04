"""
Vector store configuration.

All tunable parameters for the LanceDB adapter live here — the database
path, embedding provider settings (Infinity local default, OpenAI-compatible
alternative), batching, and pre-built collection names.

Environment variables:
  LANCE_DIR               — LanceDB database directory (default /var/data/lance)
  EMBEDDING_PROVIDER      — "infinity" (default) or "openai"; "ollama" kept
                            only as a legacy alias for the OpenAI-compatible path
  EMBEDDING_MODEL         — embedding model name (default BAAI/bge-m3)
  EMBEDDING_BASE_URL      — Infinity base URL (default http://localhost:8001)
                            or OpenAI-compatible base URL when provider=openai
  EMBEDDING_API_KEY       — API key for OpenAI-compatible providers (default "")
  EMBEDDING_BATCH_SIZE    — texts per embedding request (default 32)
  EMBEDDING_DIM           — embedding vector dimension (default 1024, BAAI/bge-m3)
"""

from __future__ import annotations

import os
from pathlib import Path

# ── Database ────────────────────────────────────────────────────────────
LANCE_DIR: Path = Path(os.environ.get("LANCE_DIR", "/var/data/lance"))
LANCE_DIR.mkdir(parents=True, exist_ok=True)

# ── Embedding provider ─────────────────────────────────────────────────
EMBEDDING_PROVIDER: str = os.environ.get("EMBEDDING_PROVIDER", "infinity").lower()
EMBEDDING_MODEL: str = os.environ.get("EMBEDDING_MODEL", "BAAI/bge-m3")
EMBEDDING_BASE_URL: str = os.environ.get(
    "EMBEDDING_BASE_URL",
    "http://localhost:8001" if EMBEDDING_PROVIDER in ("infinity", "ollama") else "https://api.openai.com/v1",
)
EMBEDDING_API_KEY: str = os.environ.get("EMBEDDING_API_KEY", "")

# BAAI/bge-m3 produces 1024-dim vectors; other models differ. The
# dimension must match the vector column declared in each collection schema.
EMBEDDING_DIM: int = int(os.environ.get("EMBEDDING_DIM", "1024"))

# Embedding requests are batched to keep memory bounded and respect the
# provider's request size limits.
EMBEDDING_BATCH_SIZE: int = int(os.environ.get("EMBEDDING_BATCH_SIZE", "32"))

# HTTP timeout for a single embedding request (seconds).
EMBEDDING_TIMEOUT: float = float(os.environ.get("EMBEDDING_TIMEOUT", "60"))

# ── Pre-built collection names ──────────────────────────────────────────
# These are the canonical vector collections created by
# ``LanceVectorStore.ensure_default_collections()``.
COLLECTION_WALLETS: str = "wallet_embeddings"
COLLECTION_CONTRACTS: str = "contract_embeddings"
COLLECTION_TRANSACTIONS: str = "transaction_embeddings"
COLLECTION_TOKENS: str = "token_embeddings"

ALL_COLLECTIONS: tuple[str, ...] = (
    COLLECTION_WALLETS,
    COLLECTION_CONTRACTS,
    COLLECTION_TRANSACTIONS,
    COLLECTION_TOKENS,
)
