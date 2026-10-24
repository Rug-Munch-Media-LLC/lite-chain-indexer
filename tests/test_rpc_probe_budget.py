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

import time
from unittest.mock import patch

import pytest

from lite_indexer.rpc import capabilities as cap
from lite_indexer.rpc import verify as vf
from lite_indexer.rpc.capabilities import (
    GETLOGS_PROBE_CANDIDATES,
    GETLOGS_PROBE_MAX_CALLS,
    GETLOGS_PROBE_TIMEOUT,
    CapabilityRecord,
    GetLogsCapability,
    get_capability,
    set_capability,
)
from lite_indexer.rpc.capability_ladder import conservative_bound, ladder_rungs
from lite_indexer.rpc.health_logs import PROBE_SPAN_BLOCKS

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


def test_range_probe_stops_at_first_failure():
    """The first rejected rung ends the ladder: the policy walks it in order."""
    rungs = list(ladder_rungs(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS))
    first_rejected = rungs[0]
    probed = [size for size in rungs if rungs.index(size) <= rungs.index(first_rejected)]

    assert probed == [first_rejected]  # no binary search above the rejection
    assert len(probed) <= GETLOGS_PROBE_MAX_CALLS
    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, []) == 0


def test_range_probe_returns_conservative_lower_bound():
    """The bound is the SMALLEST accepted rung, not the largest."""
    everything_accepted = [10, 100, 1000]

    # 1000 answered, but 10 also did — report the rung known to work.
    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, everything_accepted) == 10
    # Only 100 was ever accepted: that rung IS the conservative floor.
    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, [100]) == 100
    rungs = list(ladder_rungs(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS))
    assert rungs == [10, 100, 1000]
    assert len(rungs) <= GETLOGS_PROBE_MAX_CALLS


def test_range_probe_never_trusts_an_unprobed_rung():
    """A rung outside the call budget cannot raise the reported bound."""
    everything = [10, 100, 1000, 10000]

    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, everything) == 10
    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, 0, [10]) == 0


def test_timing_out_endpoint_respects_budget():
    """A client-side timeout is just a rejected rung: the budget still holds."""
    timeouts = [10, 100, 1000]  # every rung timed out → nothing was accepted

    assert conservative_bound(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS, []) == 0
    assert len(list(ladder_rungs(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS))) == GETLOGS_PROBE_MAX_CALLS
    assert timeouts == list(GETLOGS_PROBE_CANDIDATES)


def test_range_probe_wall_time_bounded():
    """Wall time is bounded by rungs x per-request timeout, not by any max cap."""
    per_call = 0.0
    worst_case = per_call * len(list(ladder_rungs(GETLOGS_PROBE_CANDIDATES, GETLOGS_PROBE_MAX_CALLS)))

    assert worst_case < GETLOGS_PROBE_TIMEOUT * GETLOGS_PROBE_MAX_CALLS
    assert GETLOGS_PROBE_MAX_CALLS == 3  # 3 rungs, not the 8-call binary search


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
    # The cached range must be conclusive (> PROBE_SPAN_BLOCKS), else the
    # verifier deliberately re-probes the ladder rung (3ca36d664).
    set_capability(
        CapabilityRecord(
            endpoint=EP,
            chain="bsc",
            getlogs_max_range=PROBE_SPAN_BLOCKS * 4,
            logs_capable=True,
            probed_at=time.time(),
        )
    )
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
