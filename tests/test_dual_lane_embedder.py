"""Tests for the dual-lane embedder failover (DeepInfra → local llama.cpp).

Uses a mocked httpx.AsyncClient so no network access is needed.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from lite_indexer.vectors.embedder import DualLaneEmbedder, EmbeddingError


def _response(status: int, payload: dict[str, Any] | None = None) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    if status >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(f"HTTP {status}", request=MagicMock(), response=resp)
    else:
        resp.raise_for_status.return_value = None
        resp.json.return_value = payload
    return resp


def _ok_payload(texts: list[str]) -> dict[str, Any]:
    return {"data": [{"index": i, "embedding": [float(i)] * 4} for i in range(len(texts))]}


def _client_with_responses(responses: list[MagicMock]) -> MagicMock:
    client = MagicMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(side_effect=responses)
    return client


@pytest.mark.asyncio
async def test_deepinfra_429_falls_back_to_local() -> None:
    """DeepInfra 429 → local lane is used and returns vectors."""
    client = _client_with_responses([_response(429), _response(200, _ok_payload(["hello"]))])
    embedder = DualLaneEmbedder(mode="auto", deepinfra_api_key="test-key", client=client)
    vectors = await embedder.embed(["hello"])
    assert vectors is not None
    assert vectors == [[0.0, 0.0, 0.0, 0.0]]
    assert client.post.call_count == 2
    # First call hits DeepInfra with Bearer auth; second hits local without.
    first, second = client.post.call_args_list
    assert "api.deepinfra.com" in first.args[0]
    assert first.kwargs["headers"]["Authorization"].startswith("Bearer ")
    assert "127.0.0.1:8004" in second.args[0]
    assert "Authorization" not in second.kwargs["headers"]


@pytest.mark.asyncio
async def test_both_lanes_fail_returns_none() -> None:
    """DeepInfra 402 + local connection error → None (graceful skip)."""
    client = _client_with_responses([_response(402), _response(500)])
    embedder = DualLaneEmbedder(mode="auto", deepinfra_api_key="test-key", client=client)
    vectors = await embedder.embed(["hello"])
    assert vectors is None
    assert client.post.call_count == 2


@pytest.mark.asyncio
async def test_deepinfra_success_skips_local() -> None:
    """DeepInfra success → local lane never called."""
    client = _client_with_responses([_response(200, _ok_payload(["a", "b"]))])
    embedder = DualLaneEmbedder(mode="auto", client=client)
    vectors = await embedder.embed(["a", "b"])
    assert vectors is not None and len(vectors) == 2
    assert client.post.call_count == 1


@pytest.mark.asyncio
async def test_local_only_mode() -> None:
    """mode='local' never touches DeepInfra."""
    client = _client_with_responses([_response(200, _ok_payload(["x"]))])
    embedder = DualLaneEmbedder(mode="local", client=client)
    vectors = await embedder.embed(["x"])
    assert vectors is not None
    (call,) = client.post.call_args_list
    assert "127.0.0.1:8004" in call.args[0]
