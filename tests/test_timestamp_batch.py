"""Tests for the block-timestamp phase (rpc/client.fetch_block_timestamps).

Root-cause regression suite (2026-09-14): the sync loop issued one
``eth_getBlockByNumber`` per block for timestamps, the 429s tripped
``all_endpoints_down`` and the cycle hung before the ClickHouse insert.

Contracts:

* a 500-block range issues a bounded SAMPLE of batched calls (not 500),
  with batching's per-element fallback disabled
* cached block->timestamp results are never re-fetched (immutable)
* a slow/429-ing provider hits the phase deadline and the phase returns
  instead of blocking the cycle
* interpolation fills every unresolved block from the anchors
* ``_enrich_timestamps`` swallows a failing/timing-out fetcher so the
  cycle still reaches the insert (events always get a blockTimestamp)

Hermetic — no network, no disk.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from lite_indexer.indexer._rpc_fetch import RpcFetchMixin
from lite_indexer.rpc import client as rc
from lite_indexer.rpc.client import fetch_block_timestamps

BASE_TS = 1_700_000_000
BLOCK_TIME = 12


def _ts_for(block: int) -> int:
    return BASE_TS + block * BLOCK_TIME


class _FakeTsClient:
    """Test double exposing only ``call_batch`` (the sync-path contract)."""

    def __init__(
        self,
        *,
        resolved: set[int] | None = None,
        sleep: float = 0.0,
        raises: bool = False,
    ) -> None:
        self.resolved = resolved
        self.sleep = sleep
        self.raises = raises
        self.batch_calls: list[list[tuple[str, list[Any] | None]]] = []
        self.fallbacks: list[bool] = []

    async def call_batch(
        self,
        endpoint: str,
        calls: list[tuple[str, list[Any] | None]],
        *,
        fallback: bool = True,
        timeout: float = 0.0,
    ) -> list[dict[str, Any] | None]:
        self.batch_calls.append(list(calls))
        self.fallbacks.append(fallback)
        if self.sleep:
            await asyncio.sleep(self.sleep)
        if self.raises:
            raise RuntimeError("429 rate limited")
        out: list[dict[str, Any] | None] = []
        for _method, params in calls:
            block = int(str(params[0]), 16)
            if self.resolved is None or block in self.resolved:
                out.append({"result": {"timestamp": hex(_ts_for(block))}})
            else:
                out.append(None)
        return out


@pytest.fixture(autouse=True)
def _hermetic(monkeypatch: pytest.MonkeyPatch):
    rc.clear_timestamp_cache()
    monkeypatch.setattr(rc, "get_best_endpoint", lambda endpoints: endpoints[0])
    yield
    rc.clear_timestamp_cache()


async def test_500_block_range_uses_batched_sample_not_per_block() -> None:
    """Before: 500 calls. After: <= _TS_SAMPLE_MAX_FETCHES anchors, ONE batch call."""
    client = _FakeTsClient()
    blocks = set(range(118_990_983, 118_991_483))  # 500 blocks

    timestamps = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="bsc")

    assert len(client.batch_calls) == 1, "one call_batch invocation, not one per block"
    fetched = sum(len(c) for c in client.batch_calls)
    assert fetched <= rc._TS_SAMPLE_MAX_FETCHES
    assert fetched < len(blocks), "must sample, not fetch every block"
    assert client.fallbacks == [False], "per-element fallback must be OFF for timestamps"
    assert len(timestamps) == len(blocks), "interpolation must cover the sampled-away blocks"


async def test_timestamp_cache_avoids_refetch() -> None:
    client = _FakeTsClient()
    blocks = set(range(5000, 5010))

    first = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="bsc")
    calls_after_first = len(client.batch_calls)
    second = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="bsc")

    assert calls_after_first == 1
    assert len(client.batch_calls) == calls_after_first, "second call must be cache-only"
    assert second == first


async def test_slow_provider_hits_deadline_and_does_not_hang() -> None:
    client = _FakeTsClient(sleep=5.0)
    blocks = set(range(100, 200))

    started = time.monotonic()
    timestamps = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="bsc", deadline_s=0.05)
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, "deadline must bound the phase"
    assert timestamps == {}, "no anchors resolved → nothing to interpolate from"


async def test_batch_failure_returns_without_fallback_storm() -> None:
    client = _FakeTsClient(raises=True)
    blocks = set(range(100, 200))

    started = time.monotonic()
    timestamps = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="bsc")
    elapsed = time.monotonic() - started

    assert elapsed < 1.0
    assert len(client.batch_calls) == 1
    assert client.fallbacks == [False]
    assert timestamps == {}


async def test_interpolation_fills_unfetched_blocks_between_anchors() -> None:
    # Only every 4th block resolves; the rest must be interpolated between
    # real anchors (not dropped).
    resolved = set(range(1000, 1201, 4)) | {1000, 1200}
    client = _FakeTsClient(resolved=resolved)
    blocks = set(range(1000, 1201))

    timestamps = await fetch_block_timestamps(client, ["https://ts.example"], blocks, chain="ethereum")

    assert set(timestamps) == blocks
    # Monotonic and between the surrounding anchor timestamps.
    ordered = sorted(timestamps.items())
    assert all(a[1] <= b[1] for a, b in zip(ordered, ordered[1:], strict=False))
    assert timestamps[1001] >= timestamps[1000]


# ── _enrich_timestamps must never block or abort the cycle ──────────────


class _StubIndexer(RpcFetchMixin):
    def __init__(self, fetcher):
        self.chain = "bsc"
        self._chain_type = "evm"
        self._fetcher = fetcher

    async def fetch_block_timestamps(self, block_numbers):  # type: ignore[override]
        return await self._fetcher(block_numbers)


async def test_enrich_timestamps_survives_fetch_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _boom(_blocks):
        raise RuntimeError("all endpoints down")

    stub = _StubIndexer(_boom)
    events = [{"blockNumber": 10}, {"blockNumber": 11}]

    out = await stub._enrich_timestamps(events)

    assert len(out) == 2
    assert all("blockTimestamp" in e for e in out)


async def test_enrich_timestamps_survives_fetch_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    from lite_indexer.indexer import _rpc_fetch

    monkeypatch.setattr(_rpc_fetch, "TIMESTAMP_PHASE_DEADLINE_S", 0.05)

    async def _slow(_blocks):
        await asyncio.sleep(5)
        return {}

    stub = _StubIndexer(_slow)
    events = [{"blockNumber": 10}, {"blockNumber": 11}]

    started = time.monotonic()
    out = await stub._enrich_timestamps(events)
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, "enrichment deadline must bound the phase"
    assert all("blockTimestamp" in e for e in out)
