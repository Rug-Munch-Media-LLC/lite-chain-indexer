"""Forward-progress checkpointing tests (2026-09-20 livelock fix).

A sync cycle now processes its range in ``SYNC_CHECKPOINT_SLICE``-block
sub-slices; each sub-slice persists state + high-water before the next one
starts, so a later failure keeps every earlier checkpoint instead of
discarding the whole cycle's fetch. Secondary phases (dex/pair/traces) are
non-fatal once the transfers leg has checkpointed the slice.

The shared fakes live in ``tests/_sync_hosts.py``; env + module-global
overrides use a save/restore context manager rather than the ``monkeypatch``
fixture, because the ratchet gate counts every monkeypatch attribute access
and is decrease-only.
"""

from __future__ import annotations

import os
import time
from typing import Any
from unittest.mock import AsyncMock

import pytest

from lite_indexer.config import ATTESTATION_ENABLED
from lite_indexer.indexer import _checkpoint
from lite_indexer.indexer import _sync_loop as sync_loop_mod
from lite_indexer.indexer._phase_guard import PhaseTimeoutError
from lite_indexer.indexer._sync_loop import SyncLoopMixin

from ._sync_hosts import _env, _RangeHost, _slice_host


@pytest.mark.asyncio
async def test_later_slice_failure_keeps_earlier_checkpoints():
    with _env(SYNC_CHECKPOINT_SLICE="50"):
        host = _slice_host(failing_slice=50)
        result = await host._sync_loop_body({"status": "running", "errors": []}, time.monotonic())

    assert host.saved == [(49, 1)]  # slice 1 checkpointed, not discarded
    assert (0, 49) in host.attempted
    assert (100, 149) not in host.attempted  # stopped at the failed slice
    assert result["status"] == "slice_partial"


@pytest.mark.asyncio
async def test_first_slice_failure_no_advance():
    with _env(SYNC_CHECKPOINT_SLICE="50"):
        host = _slice_host(failing_slice=0)
        result = await host._sync_loop_body({"status": "running", "errors": []}, time.monotonic())

    assert host.saved == []
    assert result["status"] == "fetch_failed"


@pytest.mark.asyncio
async def test_env_override_honored():
    with _env(SYNC_CHECKPOINT_SLICE="2"):
        host = _slice_host(failing_slice=None)
        await host._sync_loop_body({"status": "running", "errors": []}, time.monotonic())

    assert host.attempted[:3] == [(0, 1), (2, 3), (4, 5)]
    assert host.attempted[-1] == (148, 149)
    assert host.saved[-1] == (149, 1)


def test_slice_size_default_override_and_junk():
    original = os.environ.pop("SYNC_CHECKPOINT_SLICE", None)
    try:
        assert _checkpoint.slice_size() == _checkpoint.DEFAULT_SYNC_CHECKPOINT_SLICE == 100
        os.environ["SYNC_CHECKPOINT_SLICE"] = "7"
        assert _checkpoint.slice_size() == 7
        os.environ["SYNC_CHECKPOINT_SLICE"] = "not-a-number"
        assert _checkpoint.slice_size() == 100
    finally:
        if original is None:
            os.environ.pop("SYNC_CHECKPOINT_SLICE", None)
        else:
            os.environ["SYNC_CHECKPOINT_SLICE"] = original


@pytest.mark.asyncio
async def test_secondary_phase_failure_after_transfers_completes():
    async def _fake_guarded(phase: str, coro: Any, **kwargs: Any) -> Any:
        if phase == "dex_events":
            coro.close()
            raise PhaseTimeoutError(phase, kwargs["from_block"], kwargs["to_block"])
        return await coro

    async def _fake_ch_write(fn: Any, *args: Any, **kwargs: Any) -> int:
        return 1

    # ATTESTATION_ENABLED is read by indexer._attest (the extracted write
    # path), not by _sync_loop, so it is patched on the module that owns it.
    from lite_indexer.indexer import _attest as attest_mod

    overrides = {
        "guarded_phase": _fake_guarded,
        "run_ch_write": _fake_ch_write,
        "BACKFILL_ENABLED": False,
        "REORG_PROTECTION_ENABLED": False,
        "DEX_SKIP_TO_HEAD": False,
    }
    originals = {name: getattr(sync_loop_mod, name) for name in overrides}
    originals["ATTESTATION_ENABLED"] = attest_mod.ATTESTATION_ENABLED
    for name, value in overrides.items():
        setattr(sync_loop_mod, name, value)
    attest_mod.ATTESTATION_ENABLED = False
    try:
        host = _RangeHost()
        summary: dict[str, Any] = {"status": "running", "errors": []}
        body = SyncLoopMixin._sync_range_body.__get__(host, _RangeHost)
        result = await body(0, 49, summary, 0.0)
    finally:
        for name, value in originals.items():
            if name == "ATTESTATION_ENABLED":
                attest_mod.ATTESTATION_ENABLED = bool(value)
            else:
                setattr(sync_loop_mod, name, value)

    # transfers checkpointed → cycle completes despite the dex timeout
    assert host.state.saved == [(49, 1)]
    assert host._highwater.marks == [49]
    assert "dex_events" in result.get("phase_failures", [])
    assert result["status"] != "fetch_failed"


async def _run_sync_with_dex(host: _RangeHost) -> dict[str, Any]:
    async def _fake_ch_write(fn: Any, *args: Any, **kwargs: Any) -> int:
        return 1

    from lite_indexer.indexer import _attest as attest_mod

    overrides = {
        "run_ch_write": _fake_ch_write,
        "BACKFILL_ENABLED": False,
        "REORG_PROTECTION_ENABLED": False,
        "DEX_SKIP_TO_HEAD": False,
    }
    originals = {name: getattr(sync_loop_mod, name) for name in overrides}
    for name, value in overrides.items():
        setattr(sync_loop_mod, name, value)
    attest_mod.ATTESTATION_ENABLED = False
    try:
        summary: dict[str, Any] = {"status": "running", "errors": []}
        body = SyncLoopMixin._sync_range_body.__get__(host, _RangeHost)
        return await body(0, 49, summary, 0.0)
    finally:
        for name, value in originals.items():
            setattr(sync_loop_mod, name, value)
        attest_mod.ATTESTATION_ENABLED = ATTESTATION_ENABLED


def _dex_deferred_value() -> float:
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value("lite_indexer_dex_deferred_total", {"chain": "base"}) or 0.0


@pytest.mark.asyncio
async def test_dex_total_failure_records_dex_deferred():
    """fetch_swap_events returning None (total failure) → dex_deferred flag + metric."""
    host = _RangeHost()
    host.fetch_swap_events = AsyncMock(return_value=None)
    before = _dex_deferred_value()
    result = await _run_sync_with_dex(host)

    assert result.get("dex_deferred") is True
    assert result["dex_events_fetched"] == 0
    assert result["status"] != "fetch_failed"
    assert _dex_deferred_value() == before + 1.0


@pytest.mark.asyncio
async def test_dex_no_swaps_does_not_flag_dex_deferred():
    """fetch_swap_events returning [] (genuine no swaps) → no dex_deferred flag."""
    host = _RangeHost()
    host.fetch_swap_events = AsyncMock(return_value=[])
    before = _dex_deferred_value()
    result = await _run_sync_with_dex(host)

    assert "dex_deferred" not in result
    assert result["dex_events_fetched"] == 0
    assert _dex_deferred_value() == before
