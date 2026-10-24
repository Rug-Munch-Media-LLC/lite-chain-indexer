"""Arithmetic guards for the scheduler: turn bound, trace breaker, fairness.

These pin the three levers that decide whether a chain ADVANCES, not merely
whether it was granted a slot:

* a turn cannot outlive ``SYNC_SLICE_BUDGET_S`` + ``SYNC_SLICE_TIMEOUT_S``,
  so ``mean wait = (chains / slots - 1) x turn`` is a real number;
* a trace phase that times out every turn stops buying that timeout;
* the least-served-first table survives a container recreate.

No ``monkeypatch``: the ratchet is shrink-only, so state is saved and
restored explicitly.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from lite_indexer.cli import _slot_fairness
from lite_indexer.indexer import _slice_runner
from lite_indexer.traces import phase as trace_phase
from lite_indexer.traces.policy import TracePolicy


class _FakeIndexer:
    chain = "bsc"


@contextlib.contextmanager
def _patched(obj: Any, name: str, value: Any) -> Iterator[None]:
    original = getattr(obj, name)
    setattr(obj, name, value)
    try:
        yield
    finally:
        setattr(obj, name, original)


@contextlib.contextmanager
def _env(name: str, value: str) -> Iterator[None]:
    original = os.environ.get(name)
    os.environ[name] = value
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = original


# ── fairness survives a recreate ────────────────────────────────────────


def test_fairness_round_trips_and_ignores_corruption(tmp_path: Path) -> None:
    store = _slot_fairness.SlotFairnessStore(tmp_path / "slot_fairness.json")
    assert store.load() == ({}, 0)
    store.save({"bsc": 7, "celo": 3}, 21)
    assert store.load() == ({"bsc": 7, "celo": 3}, 21)

    (tmp_path / "slot_fairness.json").write_text("{not json")
    assert store.load() == ({}, 0)


def test_rotation_reloads_served_counts(tmp_path: Path) -> None:
    from lite_indexer.cli._slot_rotation import SlotRotation

    store = _slot_fairness.SlotFairnessStore(tmp_path / "slot_fairness.json")
    store.save({"bsc": 4, "celo": 9}, 13)

    rotation = SlotRotation(3, fairness=store)
    # celo was served most; bsc is the least-served waiter after a recreate.
    assert rotation._served == {"bsc": 4, "celo": 9}
    assert rotation._arrivals == 13


# ── a turn is time-bounded, not slice-count-bounded ─────────────────────


def test_hung_slice_returns_slice_timeout_without_advancing() -> None:
    async def _hang(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        await asyncio.sleep(10)
        return {"status": "ok"}

    with _patched(_slice_runner, "SYNC_SLICE_TIMEOUT_S", 0.05), _env("SYNC_CHECKPOINT_SLICE", "5"):
        summary: dict[str, Any] = {}
        result = asyncio.run(_slice_runner.run_slices(_FakeIndexer(), _hang, (0, 9), summary, 0.0))

    assert result["status"] == "slice_timeout"
    assert result["slices_done"] == 0
    assert summary["status"] == "slice_timeout"


# ── a phase that always times out stops consuming slot time ─────────────


def test_trace_phase_skips_after_consecutive_timeouts() -> None:
    calls = {"n": 0}

    async def _slow(*_args: Any) -> list[dict[str, Any]]:
        calls["n"] += 1
        await asyncio.sleep(1)
        return []

    host = _FakeIndexer()
    host._collect_internal_transfers = _slow  # type: ignore[attr-defined]
    policy = TracePolicy(max_blocks_per_cycle=5, phase_timeout_s=0.01, concurrency=1)

    with (
        _patched(trace_phase, "trace_policy", lambda _chain: policy),
        _patched(trace_phase, "_PHASE_STATE", {}),
    ):
        for _ in range(2):
            asyncio.run(trace_phase.run_trace_phase(host, {}, 1, 2))
        assert calls["n"] == 2
        # Third turn is skipped outright: no call, no timeout paid.
        asyncio.run(trace_phase.run_trace_phase(host, {}, 1, 2))

    assert calls["n"] == 2


def test_state_file_is_plain_json(tmp_path: Path) -> None:
    store = _slot_fairness.SlotFairnessStore(tmp_path / "state" / "slot_fairness.json")
    store.save({"ethereum": 1}, 1)
    assert json.loads((tmp_path / "state" / "slot_fairness.json").read_text())["served"] == {"ethereum": 1}
