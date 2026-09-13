"""
Vector store configuration.

All tunable parameters for the LanceDB adapter live here — the database
path, embedding settings for the dual-lane Qwen3-Embedding-4B setup,
batching, and pre-built collection names.

Embedding architecture (Qwen3-Embedding-4B, 2560-dim):

* **DeepInfra API** (primary) — fast hosted inference for quick tasks.
  Authenticated with ``DEEPINFRA_API_KEY``.
* **Local llama.cpp** (backup) — ``http://127.0.0.1:8004/v1/embeddings``,
  no API key, for light tasks and when DeepInfra is unavailable.

``EMBEDDING_MODE`` selects the lane strategy:

* ``"local"`` (default) — local llama.cpp only (zero-cost). Set "auto" or
  "deepinfra" to use the paid DeepInfra lane (opt-in).
* ``"deepinfra"`` — DeepInfra only.
* ``"local"`` — local llama.cpp only.

Environment variables:
  LANCE_DIR               — LanceDB database directory (default /var/data/lance)
  EMBEDDING_MODE          — "local" (default), "auto", or "deepinfra"
  DEEPINFRA_EMBED_URL     — DeepInfra OpenAI-compatible base URL
                            (default https://api.deepinfra.com/v1/openai)
  LOCAL_EMBED_URL         — local llama.cpp base URL (default http://127.0.0.1:8004/v1)
  DEEPINFRA_EMBED_MODEL   — DeepInfra model (default Qwen/Qwen3-Embedding-4B)
  LOCAL_EMBED_MODEL       — local model (default qwen3-embedding-4b)
  DEEPINFRA_API_KEY       — API key for DeepInfra (default "")
  EMBEDDING_BATCH_SIZE    — texts per embedding request (default 32)
  EMBEDDING_DIM           — embedding vector dimension (default 2560, Qwen3-Embedding-4B)
"""

from __future__ import annotations

import os
from pathlib import Path

# ── Database ────────────────────────────────────────────────────────────
LANCE_DIR: Path = Path(os.environ.get("LANCE_DIR", "/var/data/lance"))
LANCE_DIR.mkdir(parents=True, exist_ok=True)

# ── Embedding: dual-lane Qwen3-Embedding-4B ─────────────────────────────
EMBEDDING_MODE: str = os.environ.get("EMBEDDING_MODE", "local").lower()

DEEPINFRA_EMBED_URL: str = os.environ.get("DEEPINFRA_EMBED_URL", "https://api.deepinfra.com/v1/openai")
LOCAL_EMBED_URL: str = os.environ.get("LOCAL_EMBED_URL", "http://127.0.0.1:8004/v1")

DEEPINFRA_EMBED_MODEL: str = os.environ.get("DEEPINFRA_EMBED_MODEL", "Qwen/Qwen3-Embedding-4B")
LOCAL_EMBED_MODEL: str = os.environ.get("LOCAL_EMBED_MODEL", "qwen3-embedding-4b")

DEEPINFRA_API_KEY: str = os.environ.get("DEEPINFRA_API_KEY", "")

# Qwen3-Embedding-4B produces 2560-dim vectors. The dimension must match
# the vector column declared in each collection schema.
EMBEDDING_DIM: int = int(os.environ.get("EMBEDDING_DIM", "2560"))

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
