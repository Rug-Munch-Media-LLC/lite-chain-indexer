"""HTTP client lifecycle tests (2026-09-20 daemon-hang incident).

Covers the self-heal path added to ``rpc/client.py``: a pooled
``httpx.AsyncClient`` closed under an in-flight request (LRU eviction,
shutdown race, proxy TLS teardown) must be dropped and recreated instead
of surfacing ``RuntimeError: client has been closed`` /
``anyio.ClosedResourceError`` to the caller — and instead of hanging a
chain task silently.
"""

from __future__ import annotations

import httpx
import pytest

from lite_indexer.rpc import client as client_mod


class _FakeClosedResourceError(Exception):
    """Mimics anyio.ClosedResourceError by class NAME (no anyio import needed)."""


_FakeClosedResourceError.__name__ = "ClosedResourceError"


def test_is_closed_client_error_detects_closed_client() -> None:
    assert client_mod._is_closed_client_error(RuntimeError("Cannot send a request, as the client has been closed."))
    assert client_mod._is_closed_client_error(_FakeClosedResourceError())
    assert client_mod._is_closed_client_error(httpx.ConnectError("boom"))


def test_is_closed_client_error_ignores_unrelated_errors() -> None:
    assert not client_mod._is_closed_client_error(ValueError("nope"))
    assert not client_mod._is_closed_client_error(RuntimeError("some other runtime error"))


def test_drop_call_client_evicts_cached_entry() -> None:
    key = (12.5, None)
    client_mod._call_clients[key] = object()  # type: ignore[assignment]
    try:
        client_mod._drop_call_client(12.5, None)
        assert key not in client_mod._call_clients
        # Dropping an absent entry is a no-op, not an error.
        client_mod._drop_call_client(12.5, None)
    finally:
        client_mod._call_clients.pop(key, None)


@pytest.mark.asyncio
async def test_post_direct_self_heals_closed_client(monkeypatch) -> None:
    """A closed pooled client is recreated and the request retried once."""

    class _ClosedClient:
        async def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("Cannot send a request, as the client has been closed.")

    class _GoodClient:
        async def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return httpx.Response(200, json={"result": "0x1"})

    recreated: list[float] = []

    async def _fake_get_call_client(timeout, proxy=None):  # noqa: ANN001
        recreated.append(timeout)
        return _GoodClient()

    monkeypatch.setattr(client_mod, "_get_call_client", _fake_get_call_client)
    resp = await client_mod._post_direct(_ClosedClient(), "https://example.invalid", {"id": 1}, 30.0)
    assert resp.status_code == 200
    assert recreated == [30.0], "self-heal must recreate the client exactly once"


@pytest.mark.asyncio
async def test_post_direct_propagates_unrelated_runtime_errors() -> None:
    class _BadClient:
        async def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
            raise RuntimeError("some other runtime error")

    with pytest.raises(RuntimeError, match="some other runtime error"):
        await client_mod._post_direct(_BadClient(), "https://example.invalid", {"id": 1}, 30.0)
