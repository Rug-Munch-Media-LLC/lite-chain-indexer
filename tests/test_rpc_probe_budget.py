"""Bounded capability-probe latency (2026-09-14 startup bottleneck).

The startup endpoint-verification pass used to binary-search the exact
``getlogs_max_range`` per endpoint (up to 8 extra eth_getLogs calls, each up
to the probe timeout) — measured ~40 s per chain on bsc. Contracts asserted:

* ``probe_getlogs_range`` spends at most ``GETLOGS_PROBE_MAX_CALLS`` calls and
  stops at the first failure → a conservative LOWER BOUND, no binary search.
* a timing-out endpoint cannot exceed that call budget and its wall time is
  bounded in a hermetic test.
* a warm capability record skips ALL probes (restart inside the interval).
* a cold range-capped endpoint still gets classified and its conservative
  range is persisted.
* ``get_max_block_range`` reuses the measured range to size the first getLogs
  range (static provider cap kept when it is the tighter bound).

Hermetic — no network.
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import patch

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc import verify as vf
from lite_indexer.rpc.capabilities import (
    GETLOGS_PROBE_MAX_CALLS,
    CapabilityRecord,
    GetLogsCapability,
    get_capability,
    set_capability,
)

EP = "https://probe-budget.example"


@pytest.fixture(autouse=True)
def _clean_state():
    cap._CAPABILITY_RECORDS.clear()
    vf.clear_startup_results()
    yield
    cap._CAPABILITY_RECORDS.clear()
    vf.clear_startup_results()


class _TipClient:
    """Minimal RpcClient stand-in: resolves the tip for range anchoring."""

    async def eth_block_number(self, endpoint: str) -> int:
        return 1_000_000


# ── Bounded range ladder ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_range_probe_stops_at_first_failure():
    calls = {"n": 0}

    async def _probe(endpoint, from_block, to_block, topic0, timeout=0.0):
        calls["n"] += 1
        return cap._GetLogsProbeStatus.RANGE_ERROR

    with patch.object(cap, "_probe_getlogs_once", _probe):
        result = await cap.probe_getlogs_range(_TipClient(), EP, "0xtopic", latest_block=1_000_000)

    assert result == 0
    assert calls["n"] == 1  # first failure bounds it; no binary search
    assert calls["n"] <= GETLOGS_PROBE_MAX_CALLS


@pytest.mark.asyncio
async def test_range_probe_returns_conservative_lower_bound():
    seen: list[int] = []

    async def _probe(endpoint, from_block, to_block, topic0, timeout=0.0):
        size = to_block - from_block + 1
        seen.append(size)
        return cap._GetLogsProbeStatus.OK if size <= 100 else cap._GetLogsProbeStatus.RANGE_ERROR

    with patch.object(cap, "_probe_getlogs_once", _probe):
        result = await cap.probe_getlogs_range(_TipClient(), EP, "0xtopic", latest_block=1_000_000)

    assert result == 100  # conservative bound, not the exact max
    assert seen == [10, 100, 1000]
    assert len(seen) <= GETLOGS_PROBE_MAX_CALLS


@pytest.mark.asyncio
async def test_timing_out_endpoint_respects_budget(monkeypatch):
    class _SlowClient:
        posts = 0

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def post(self, url, json=None):  # noqa: A002
            type(self).posts += 1
            raise cap.httpx.TimeoutException("probe timed out")

    with patch.object(cap.httpx, "AsyncClient", lambda **k: _SlowClient()):
        result = await cap.probe_getlogs_range(_TipClient(), EP, "0xtopic", latest_block=1_000_000)

    assert result == 0
    assert _SlowClient.posts <= GETLOGS_PROBE_MAX_CALLS


@pytest.mark.asyncio
async def test_range_probe_wall_time_bounded():
    per_call = 0.05

    async def _slow(endpoint, from_block, to_block, topic0, timeout=0.0):
        await asyncio.sleep(per_call)
        return cap._GetLogsProbeStatus.RANGE_ERROR

    with patch.object(cap, "_probe_getlogs_once", _slow):
        started = time.monotonic()
        await cap.probe_getlogs_range(_TipClient(), EP, "0xtopic", latest_block=1_000_000)
        elapsed = time.monotonic() - started

    assert elapsed < per_call * GETLOGS_PROBE_MAX_CALLS + 0.15


# ── Warm-cache reuse ───────────────────────────────────────────────────


async def _forbidden(*args, **kwargs):
    raise AssertionError("warm cache must not re-probe")


@pytest.mark.asyncio
async def test_warm_cache_with_range_skips_all_probes():
    set_capability(
        CapabilityRecord(endpoint=EP, chain="bsc", getlogs_max_range=64, logs_capable=None, probed_at=time.time())
    )
    with (
        patch.object(vf, "probe_archive", _forbidden),
        patch.object(vf, "probe_getlogs_range", _forbidden),
        patch.object(vf, "probe_logs_capability", _forbidden),
    ):
        status = await vf._probe_and_record_capabilities(_TipClient(), EP, "bsc", 1_000_000)  # noqa: SLF001

    assert status is GetLogsCapability.TRANSIENT  # never excluded on a timeout
    assert get_capability(EP).getlogs_max_range == 64  # range reused


@pytest.mark.asyncio
async def test_warm_cache_conclusive_verdict_skips_probes():
    set_capability(CapabilityRecord(endpoint=EP, chain="bsc", logs_capable=True, probed_at=time.time()))
    with (
        patch.object(vf, "probe_archive", _forbidden),
        patch.object(vf, "probe_getlogs_range", _forbidden),
        patch.object(vf, "probe_logs_capability", _forbidden),
    ):
        status = await vf._probe_and_record_capabilities(_TipClient(), EP, "bsc", 1_000_000)  # noqa: SLF001

    assert status is GetLogsCapability.CAPABLE


# ── Cold probe: classify + persist the conservative range ──────────────


@pytest.mark.asyncio
async def test_cold_probe_classifies_and_persists_conservative_range():
    async def _archive(*args, **kwargs):
        return False

    async def _range(*args, **kwargs):
        return 64

    async def _cap(endpoint, tip, timeout=0.0):
        return GetLogsCapability.CAPABLE

    with (
        patch.object(vf, "probe_archive", _archive),
        patch.object(vf, "probe_getlogs_range", _range),
        patch.object(vf, "probe_logs_capability", _cap),
    ):
        status = await vf._probe_and_record_capabilities(_TipClient(), EP, "bsc", 1_000_000)  # noqa: SLF001

    assert status is GetLogsCapability.CAPABLE
    record = get_capability(EP)
    assert record is not None
    assert record.getlogs_max_range == 64
    assert record.logs_capable is True


# ── Measured range sizes the first getLogs range ───────────────────────


def test_get_max_block_range_reuses_measured_capability():
    from lite_indexer.chains.helpers import get_max_block_range

    assert get_max_block_range(EP) == 500  # unlisted → DEFAULT_MAX_RANGE
    set_capability(CapabilityRecord(endpoint=EP, chain="bsc", getlogs_max_range=64, probed_at=time.time()))
    assert get_max_block_range(EP) == 64


def test_get_max_block_range_keeps_tighter_static_cap():
    from lite_indexer.chains.helpers import get_max_block_range

    endpoint = "https://node.tatum.io/rpc"  # static MAX_BLOCK_RANGE == 100
    set_capability(CapabilityRecord(endpoint=endpoint, chain="bsc", getlogs_max_range=5000, probed_at=time.time()))
    assert get_max_block_range(endpoint) == 100
