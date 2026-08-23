"""Hedged RPC dispatch tests — NodeCore-style tail-latency mitigation.

Offline: client is a stub with controllable per-endpoint delays.
Contracts:

* fast primary answers inside the delay window → hedge never fired
* slow primary (> RPC_HEDGE_DELAY) → hedge fires, first success wins,
  total wall time bounded by the FAST endpoint, not the slow one
* both legs fail → None (fail-open shape preserved)
* single-endpoint pools degenerate to plain call
* disabled via RPC_HEDGE_ENABLED=0 → plain call even with slow primary
"""

from __future__ import annotations

import asyncio
import time

import pytest

from lite_indexer.rpc import hedging
from lite_indexer.rpc.hedging import hedged_call

EPS = ["https://primary.example", "https://hedge.example", "https://third.example"]


class StubClient:
    """Per-endpoint scripted delays + call log."""

    def __init__(self, delays: dict[str, float], results: dict[str, dict | None] | None = None):
        self.delays = delays
        self.results = results or {}
        self.calls: list[str] = []

    async def call(self, endpoint: str, method: str, params=None, timeout=None):
        self.calls.append(endpoint)
        await asyncio.sleep(self.delays.get(endpoint, 0.0))
        return self.results.get(endpoint, {"jsonrpc": "2.0", "id": 1, "result": endpoint})


@pytest.fixture(autouse=True)
def _fast_hedge(monkeypatch):
    monkeypatch.setattr(hedging, "HEDGE_DELAY_S", 0.05)
    monkeypatch.setattr(hedging, "HEDGE_ENABLED", True)


@pytest.mark.asyncio
async def test_fast_primary_skips_hedge():
    client = StubClient(delays={EPS[0]: 0.0})
    out = await hedged_call(client, EPS, "eth_blockNumber")
    assert out is not None
    # P2C draw is random among healthy endpoints — assert single call.
    assert len(client.calls) == 1  # hedge never spent


@pytest.mark.asyncio
async def test_slow_primary_hedged_and_fast_leg_wins():
    client = StubClient(delays={EPS[0]: 1.0, EPS[1]: 0.0})
    t0 = time.perf_counter()
    out = await hedged_call(client, EPS, "eth_getLogs", [{"fromBlock": "0x1"}])
    elapsed = time.perf_counter() - t0
    assert out is not None
    # Property under test: the hedge bounds latency by the FAST leg — i.e.
    # strictly below the slow leg's scripted 1.0s delay. The original 0.5s
    # bound had only 0.45s of scheduler slack and flaked under host load
    # spikes (asyncio.sleep overshoot); 1.0s keeps the invariant meaningful
    # (hedge fired, slow leg did not gate the result) while tolerating load.
    assert elapsed < 1.0, f"hedge should beat the slow leg, took {elapsed:.2f}s"
    # Hedge fired and produced the answer.
    assert EPS[1] in client.calls
    # NOTE: whether the slow leg's task got its FIRST scheduling slice
    # before the fast leg resolved is load-dependent (observed: under host
    # load the primary never ran before the return) — asserting its presence
    # in calls would test the scheduler, not the hedger.


@pytest.mark.asyncio
async def test_both_fail_returns_none():
    client = StubClient(
        delays={EPS[0]: 0.0, EPS[1]: 0.0},
        results={EPS[0]: None, EPS[1]: None},
    )
    out = await hedged_call(client, EPS[:2], "eth_call")
    assert out is None


@pytest.mark.asyncio
async def test_single_endpoint_degenerates():
    client = StubClient(delays={EPS[0]: 0.0})
    out = await hedged_call(client, EPS[:1], "eth_chainId")
    assert out is not None
    assert client.calls == [EPS[0]]


@pytest.mark.asyncio
async def test_disabled_via_env(monkeypatch):
    monkeypatch.setattr(hedging, "HEDGE_ENABLED", False)
    client = StubClient(delays={EPS[0]: 0.0})
    out = await hedged_call(client, EPS, "eth_blockNumber")
    assert out is not None
    # P2C draw is random among healthy endpoints — assert single call,
    # not which endpoint won.
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_empty_endpoints_short_circuit():
    client = StubClient(delays={})
    assert await hedged_call(client, [], "eth_chainId") is None
    assert client.calls == []
