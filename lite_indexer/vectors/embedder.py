"""
Embedding generation for the vector store.

Supports two providers:

* **Ollama** (default) — ``nomic-embed-text`` served locally, zero cost,
  no API key. ``POST /api/embed`` with a batch of texts returns a list of
  embeddings.
* **OpenAI-compatible** — any provider implementing the ``/embeddings``
  contract (OpenAI, Together, local vLLM, …). Authenticated with a Bearer
  token via ``EMBEDDING_API_KEY``.

Both providers are called over HTTP with :mod:`httpx`. Requests are
batched (``EMBEDDING_BATCH_SIZE``) and each batch can be observed via the
``on_progress`` callback so long indexing runs show progress.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import httpx

from lite_indexer.vectors.config import (
    EMBEDDING_API_KEY,
    EMBEDDING_BASE_URL,
    EMBEDDING_BATCH_SIZE,
    EMBEDDING_MODEL,
    EMBEDDING_PROVIDER,
    EMBEDDING_TIMEOUT,
)

# Progress callback: (completed, total) — called after each completed batch.
ProgressCallback = Callable[[int, int], None]


class EmbeddingError(RuntimeError):
    """Raised when the embedding provider cannot be reached or returns an error."""


class Embedder(Protocol):
    """Protocol for embedding providers."""

    @property
    def model(self) -> str:
        """The model name used for embedding."""
        ...

    async def embed(self, texts: list[str], on_progress: ProgressCallback | None = None) -> list[list[float]]:
        """Embed a list of texts, returning one vector per text (same order)."""
        ...


class OllamaEmbedder:
    """Embed via a local Ollama server (default: nomic-embed-text)."""

    def __init__(
        self,
        base_url: str = EMBEDDING_BASE_URL,
        model: str = EMBEDDING_MODEL,
        batch_size: int = EMBEDDING_BATCH_SIZE,
        timeout: float = EMBEDDING_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._model = model
        self._batch_size = batch_size
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=10.0))

    @property
    def model(self) -> str:
        return self._model

    async def embed(self, texts: list[str], on_progress: ProgressCallback | None = None) -> list[list[float]]:
        """Embed texts via ``POST /api/embed`` in batches."""
        vectors: list[list[float]] = []
        total = len(texts)
        for start in range(0, total, self._batch_size):
            batch = texts[start : start + self._batch_size]
            try:
                resp = await self._client.post(
                    f"{self.base_url}/api/embed",
                    json={"model": self._model, "input": batch},
                )
                resp.raise_for_status()
                payload: dict[str, Any] = resp.json()
            except httpx.HTTPError as exc:
                raise EmbeddingError(f"ollama embed request failed: {exc}") from exc

            embeddings = payload.get("embeddings")
            if not embeddings or not isinstance(embeddings, list):
                raise EmbeddingError(f"ollama returned no embeddings: {payload}")
            for emb in embeddings:
                if not isinstance(emb, list):
                    raise EmbeddingError("ollama embedding entry is not a list of floats")
                vectors.append([float(v) for v in emb])

            done = min(start + self._batch_size, total)
            if on_progress is not None:
                on_progress(done, total)
        return vectors

    async def aclose(self) -> None:
        """Close the HTTP client if we created it."""
        if self._owns_client:
            await self._client.aclose()


class OpenAICompatEmbedder:
    """Embed via any OpenAI-compatible ``/embeddings`` endpoint.

    Works with OpenAI, Together AI, local vLLM/llama.cpp servers, and any
    other provider implementing the same request/response shape.
    """

    def __init__(
        self,
        base_url: str = EMBEDDING_BASE_URL,
        model: str = EMBEDDING_MODEL,
        api_key: str = EMBEDDING_API_KEY,
        batch_size: int = EMBEDDING_BATCH_SIZE,
        timeout: float = EMBEDDING_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._model = model
        self._api_key = api_key
        self._batch_size = batch_size
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=10.0))

    @property
    def model(self) -> str:
        return self._model

    async def embed(self, texts: list[str], on_progress: ProgressCallback | None = None) -> list[list[float]]:
        """Embed texts via ``POST {base}/embeddings`` in batches."""
        vectors: list[list[float]] = []
        total = len(texts)
        headers = {"Authorization": f"Bearer {self._api_key}"} if self._api_key else {}
        for start in range(0, total, self._batch_size):
            batch = texts[start : start + self._batch_size]
            try:
                resp = await self._client.post(
                    f"{self.base_url}/embeddings",
                    headers=headers,
                    json={"model": self._model, "input": batch},
                )
                resp.raise_for_status()
                payload: dict[str, Any] = resp.json()
            except httpx.HTTPError as exc:
                raise EmbeddingError(f"embedding request failed: {exc}") from exc

            data = payload.get("data")
            if not data or not isinstance(data, list):
                raise EmbeddingError(f"embedding endpoint returned no data: {payload}")

            # OpenAI returns results in input order but be defensive: sort by index.
            ordered = sorted((item for item in data if isinstance(item, dict)), key=lambda d: d.get("index", 0))
            vectors.extend(item["embedding"] for item in ordered if "embedding" in item)

            done = min(start + self._batch_size, total)
            if on_progress is not None:
                on_progress(done, total)
        return vectors

    async def aclose(self) -> None:
        """Close the HTTP client if we created it."""
        if self._owns_client:
            await self._client.aclose()


def create_embedder(
    provider: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
) -> Embedder:
    """Factory: build the embedder configured by env (or explicit args).

    ``provider`` is one of ``"ollama"`` (default) or ``"openai"``. Explicit
    arguments override environment values, which lets callers like the API
    layer point at a different model without touching global state.
    """
    selected = (provider or EMBEDDING_PROVIDER).lower()
    embed_model = model or EMBEDDING_MODEL
    url = base_url or EMBEDDING_BASE_URL
    key = api_key if api_key is not None else EMBEDDING_API_KEY

    if selected == "openai":
        return OpenAICompatEmbedder(base_url=url, model=embed_model, api_key=key)
    return OllamaEmbedder(base_url=url, model=embed_model)
