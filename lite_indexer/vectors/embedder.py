"""
Embedding generation for the vector store.

Dual-lane Qwen3-Embedding-4B (2560-dim) architecture:

* **DeepInfra API** (primary lane) — ``Qwen/Qwen3-Embedding-4B`` via the
  OpenAI-compatible ``/embeddings`` endpoint at
  ``https://api.deepinfra.com/v1/openai``. Fast hosted inference for quick
  tasks, authenticated with ``DEEPINFRA_API_KEY``.
* **Local llama.cpp** (backup lane) — ``qwen3-embedding-4b`` served at
  ``http://127.0.0.1:8004/v1/embeddings``. No API key, zero cost. Used for
  light tasks and as failover when DeepInfra is rate-limited, out of
  credits, or unreachable.

``DualLaneEmbedder`` tries the lanes in order (DeepInfra → local) and
returns ``None`` when both fail, so vector ingestion can skip gracefully
instead of crashing the indexer.

Requests are batched (``EMBEDDING_BATCH_SIZE``) and each batch can be
observed via the ``on_progress`` callback so long indexing runs show
progress.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import httpx

from lite_indexer.vectors.config import (
    DEEPINFRA_API_KEY,
    DEEPINFRA_EMBED_MODEL,
    DEEPINFRA_EMBED_URL,
    EMBEDDING_BATCH_SIZE,
    EMBEDDING_MODE,
    EMBEDDING_TIMEOUT,
    LOCAL_EMBED_MODEL,
    LOCAL_EMBED_URL,
)

# Progress callback: (completed, total) — called after each completed batch.
ProgressCallback = Callable[[int, int], None]


class EmbeddingError(RuntimeError):
    """Raised when a single embedding lane cannot be reached or errors."""


class Embedder(Protocol):
    """Protocol for embedding providers."""

    @property
    def model(self) -> str:
        """The model name used for embedding."""
        ...

    async def embed(self, texts: list[str], on_progress: ProgressCallback | None = None) -> list[list[float]]:
        """Embed a list of texts, returning one vector per text (same order)."""
        ...


class _Lane:
    """One OpenAI-compatible embedding lane (DeepInfra or local llama.cpp)."""

    def __init__(
        self,
        name: str,
        base_url: str,
        model: str,
        api_key: str = "",
        batch_size: int = EMBEDDING_BATCH_SIZE,
        timeout: float = EMBEDDING_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.name = name
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
                raise EmbeddingError(f"{self.name} embed request failed: {exc}") from exc

            data = payload.get("data")
            if not data or not isinstance(data, list):
                raise EmbeddingError(f"{self.name} embedding endpoint returned no data: {payload}")

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


class DualLaneEmbedder:
    """Embed via DeepInfra (primary) with local llama.cpp failover.

    Lane order is controlled by ``mode``:

    * ``"auto"`` — DeepInfra first, local second.
    * ``"deepinfra"`` — DeepInfra only.
    * ``"local"`` — local llama.cpp only.

    ``embed()`` returns ``None`` when every configured lane fails so
    callers can skip vector ingestion gracefully.
    """

    def __init__(
        self,
        mode: str = "auto",
        deepinfra_url: str = DEEPINFRA_EMBED_URL,
        deepinfra_model: str = DEEPINFRA_EMBED_MODEL,
        deepinfra_api_key: str = DEEPINFRA_API_KEY,
        local_url: str = LOCAL_EMBED_URL,
        local_model: str = LOCAL_EMBED_MODEL,
        batch_size: int = EMBEDDING_BATCH_SIZE,
        timeout: float = EMBEDDING_TIMEOUT,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.mode = mode.lower()
        self._lanes: list[_Lane] = []
        if self.mode in ("auto", "deepinfra"):
            self._lanes.append(
                _Lane(
                    "deepinfra",
                    base_url=deepinfra_url,
                    model=deepinfra_model,
                    api_key=deepinfra_api_key,
                    batch_size=batch_size,
                    timeout=timeout,
                    client=client,
                )
            )
        if self.mode in ("auto", "local"):
            self._lanes.append(
                _Lane(
                    "local",
                    base_url=local_url,
                    model=local_model,
                    api_key="",
                    batch_size=batch_size,
                    timeout=timeout,
                    client=client,
                )
            )
        if not self._lanes:
            raise ValueError(f"unknown EMBEDDING_MODE: {mode!r} (use auto, deepinfra, or local)")

    @property
    def model(self) -> str:
        """The primary lane's model name."""
        return self._lanes[0].model

    async def embed(self, texts: list[str], on_progress: ProgressCallback | None = None) -> list[list[float]] | None:
        """Embed texts, trying lanes in order; ``None`` when all lanes fail."""
        _last_error: Exception | None = None
        for lane in self._lanes:
            try:
                return await lane.embed(texts, on_progress=on_progress)
            except EmbeddingError as exc:
                _last_error = exc
                continue
        return None

    async def aclose(self) -> None:
        """Close the HTTP clients of all lanes we created."""
        for lane in self._lanes:
            await lane.aclose()


def create_embedder(
    mode: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_key: str | None = None,
) -> DualLaneEmbedder:
    """Factory: build the dual-lane embedder configured by env (or args).

    ``mode`` is one of ``"auto"`` (default), ``"deepinfra"``, or ``"local"``.
    Explicit ``model``/``base_url``/``api_key`` override the DeepInfra
    primary lane, which lets callers like the API layer point at a
    different model without touching global state.
    """
    selected = (mode or EMBEDDING_MODE).lower()
    kwargs: dict[str, Any] = {"mode": selected}
    if model is not None:
        kwargs["deepinfra_model"] = model
    if base_url is not None:
        kwargs["deepinfra_url"] = base_url
    if api_key is not None:
        kwargs["deepinfra_api_key"] = api_key
    return DualLaneEmbedder(**kwargs)
