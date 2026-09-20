"""Tests for the shared provider gate chain (rpc/gates.py).

Every outbound RPC POST path must run the same gate chain as
``RpcClient.call`` (throttle → provider throttle → autotune → budget)
before dispatching. These tests assert the gate runs BEFORE the POST on
the previously-bypassed paths: last-resort fallback and the rate-limited
crawl loop.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

import lite_indexer.strategies.evm.last_resort as lr
from lite_indexer.strategies.evm import transfer_handler
from lite_indexer.strategies.evm.last_resort import LastResortAttempt, try_last_resort


@contextmanager
def _patch(module: Any, name: str, value: Any) -> Iterator[None]:
    if isinstance(module, str):  # accept dotted paths for lazily-imported modules
        import importlib

        module = importlib.import_module(module)
    old = getattr(module, name)
    setattr(module, name, value)
    try:
        yield
    finally:
        setattr(module, name, old)


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self.status_code = status_code
        self.text = "x" * 20
        self._payload = payload
        self.headers: dict[str, str] = {}
        self.content = b"12345"

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict[str, Any], order: list[str]) -> None:
        self._payload = payload
        self._order = order

    async def post(self, url: str, json: Any = None, timeout: Any = None) -> _FakeResponse:  # noqa: A002
        self._order.append(f"post:{url}")
        return _FakeResponse(self._payload)


class _SlotCtx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: Any) -> None:
        return None


@pytest.mark.asyncio
async def test_last_resort_gates_before_post() -> None:
    order: list[str] = []
    gate_calls: list[str] = []

    async def fake_gate(endpoint: str, method: str) -> None:
        gate_calls.append(f"gate:{endpoint}:{method}")
        order.append(f"gate:{endpoint}")

    async def fake_backoff(url: str) -> None:
        order.append(f"backoff:{url}")

    client = _FakeClient({"jsonrpc": "2.0", "id": 1, "result": []}, order)
    attempt = LastResortAttempt(
        chain="testchain",
        body={"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": []},
        client=client,
        rotated=[],
        from_block=0,
        to_block=10,
        best_result=None,
        last_error=None,
    )
    with (
        _patch(lr, "provider_gates", fake_gate),
        _patch(lr, "apply_endpoint_backoff", fake_backoff),
        _patch(lr, "provider_slot", lambda key: _SlotCtx()),
        _patch(lr, "last_resort_endpoints", lambda chain: ["http://lr1"]),
    ):
        result, error = await try_last_resort(attempt)

    assert error is None
    assert gate_calls == ["gate:http://lr1:eth_getLogs"]
    # Gate must run BEFORE the POST.
    assert order.index("gate:http://lr1") < order.index("post:http://lr1")
    assert result == []


@pytest.mark.asyncio
async def test_crawl_loop_gates_before_post() -> None:
    order: list[str] = []
    gate_calls: list[str] = []

    async def fake_gate(endpoint: str, method: str) -> None:
        gate_calls.append(f"gate:{endpoint}:{method}")
        order.append(f"gate:{endpoint}")

    async def fake_backoff(url: str) -> None:
        order.append(f"backoff:{url}")

    with (
        _patch(transfer_handler, "provider_gates", fake_gate),
        _patch(transfer_handler, "apply_endpoint_backoff", fake_backoff),
        _patch(transfer_handler, "provider_slot", lambda key: _SlotCtx()),
        _patch(transfer_handler, "get_best_endpoint", lambda eps: eps[0]),
    ):
        result = await transfer_handler._fetch_range(
            "testchain",
            0,
            10,
            client=_FakeClient({"jsonrpc": "2.0", "id": 1, "result": []}, order),
            topic="0xabc",
            endpoints=["http://ep1"],
        )

    assert result == []
    assert gate_calls == ["gate:http://ep1:eth_getLogs"]
    assert order.index("gate:http://ep1") < order.index("post:http://ep1")


@pytest.mark.asyncio
async def test_provider_gates_runs_full_chain_in_order() -> None:
    ran: list[str] = []

    async def fake_throttle(endpoint: str, method: str, **kw: Any) -> None:
        ran.append("throttle")

    async def fake_provider(endpoint: str) -> None:
        ran.append("provider")

    async def fake_autotune(endpoint: str, last_call_at: Any = None) -> None:
        ran.append("autotune")

    async def fake_budget(endpoint: str) -> None:
        ran.append("budget")

    with (
        _patch("lite_indexer.rpc.throttle", "throttle_before", fake_throttle),
        _patch("lite_indexer.rpc.provider_caps", "provider_throttle_before", fake_provider),
        _patch("lite_indexer.rpc.autotune", "autotune_wait", fake_autotune),
        _patch("lite_indexer.rpc.client", "_budget_before", fake_budget),
    ):
        from lite_indexer.rpc.gates import provider_gates

        await provider_gates("http://ep1", "eth_getLogs")
    assert ran == ["throttle", "provider", "autotune", "budget"]
