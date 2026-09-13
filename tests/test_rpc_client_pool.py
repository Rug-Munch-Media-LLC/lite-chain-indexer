"""RPC client-pool bounds tests — the module-global client cache must not leak.

2026-09-13: ``_call_clients`` grew one ``httpx.AsyncClient`` per distinct
timeout, never evicted and never closed (150 open sockets). Contracts:

* the pool never exceeds ``RPC_CALL_CLIENTS_MAX``,
* an evicted client is closed,
* pooled clients receive explicit ``limits=``,
* identical timeouts reuse one client.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from lite_indexer.config import HTTP_MAX_CONNECTIONS
from lite_indexer.rpc import client as rc


class _FakeClient:
    """Stand-in for ``httpx.AsyncClient`` recording construction + close."""

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.is_closed = False
        self.close_calls = 0

    async def aclose(self) -> None:
        self.close_calls += 1
        self.is_closed = True


def _install_factory(monkeypatch: pytest.MonkeyPatch) -> list[_FakeClient]:
    created: list[_FakeClient] = []

    def factory(**kwargs: Any) -> _FakeClient:
        client = _FakeClient(**kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(rc.httpx, "AsyncClient", factory)
    return created


@pytest.fixture(autouse=True)
def _reset_pool() -> Any:
    rc._call_clients.clear()
    yield
    rc._call_clients.clear()


async def test_pool_never_exceeds_bound_and_closes_evicted(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _install_factory(monkeypatch)
    monkeypatch.setattr(rc, "RPC_CALL_CLIENTS_MAX", 3)

    for i in range(6):
        await rc._get_call_client(float(i + 1))

    assert len(rc._call_clients) <= 3
    # The three oldest clients were evicted and closed; the newest was not.
    assert created[0].close_calls == 1
    assert created[1].close_calls == 1
    assert created[2].close_calls == 1
    assert created[5].close_calls == 0


async def test_pool_reuses_client_for_same_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _install_factory(monkeypatch)
    monkeypatch.setattr(rc, "RPC_CALL_CLIENTS_MAX", 3)

    first = await rc._get_call_client(5.0)
    second = await rc._get_call_client(5.0)

    assert first is second
    assert len(created) == 1


async def test_pooled_client_receives_explicit_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _install_factory(monkeypatch)

    await rc._get_call_client(7.0)

    limits = created[0].kwargs["limits"]
    assert isinstance(limits, httpx.Limits)
    assert limits.max_connections == HTTP_MAX_CONNECTIONS


async def test_rpc_client_receives_explicit_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    created = _install_factory(monkeypatch)

    rpc = rc.RpcClient()
    client = await rpc._get_client()

    assert client is created[0]
    assert isinstance(created[0].kwargs["limits"], httpx.Limits)
    assert created[0].kwargs["limits"].max_connections == HTTP_MAX_CONNECTIONS
