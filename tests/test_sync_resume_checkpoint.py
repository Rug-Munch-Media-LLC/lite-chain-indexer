"""Slice-resume proof: a mid-window failure resumes at slice N, not zero.

A sync window is processed in ``SYNC_CHECKPOINT_SLICE``-block sub-slices and
every completed sub-slice persists state + high-water. The next cycle derives
its start from the persisted state (``last_block + 1``), so a failure at slice
4 must resume at slice 4, never rewind to the window start.

This reproduces the 48-day stall pattern deterministically: a 250-block range
(0..249) in 50-block slices, forced failure at the slice starting at 150.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from lite_indexer.indexer._sync_loop import SyncLoopMixin
from lite_indexer.state import StateHighWater, StateManager


@contextmanager
def _env(**values: str) -> Iterator[None]:
    """Set env vars for the block, restoring the prior state."""
    originals = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, original in originals.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


class _ResumeHost:
    """Minimal sync host backed by the REAL state + high-water files."""

    chain = "resume"

    def __init__(self, state: StateManager, highwater: StateHighWater, failing_slice: int | None) -> None:
        self.state = state
        self._highwater = highwater
        self.failing_slice = failing_slice
        self.to_block = 249
        self.attempted: list[tuple[int, int]] = []

    async def _determine_block_range(self, summary: dict[str, Any]) -> tuple[int, int]:
        # Mirrors production: resume from persisted state, never from genesis.
        from_block = self.state.load().get("last_block", 0) + 1
        if from_block <= 1:
            from_block = 0  # genesis normalisation, mirrors _determine_block_range
        return from_block, self.to_block

    def _save_current_state(self) -> None:
        return None

    async def _sync_range_body(
        self, from_block: int, to_block: int, summary: dict[str, Any], start_time: float
    ) -> dict[str, Any]:
        self.attempted.append((from_block, to_block))
        if self.failing_slice is not None and from_block == self.failing_slice:
            summary["status"] = "fetch_failed"
            return summary
        summary["status"] = "ok"
        self.state.save(to_block, 1)
        self._highwater.update(to_block)
        return summary


def test_retry_resumes_at_the_failing_slice(tmp_path) -> None:
    state = StateManager("resume", tmp_path)
    highwater = StateHighWater("resume", tmp_path)
    host = _ResumeHost(state, highwater, failing_slice=150)
    host._sync_loop_body = SyncLoopMixin._sync_loop_body.__get__(host, _ResumeHost)  # type: ignore[method-assign]

    with _env(SYNC_CHECKPOINT_SLICE="50"):
        first = asyncio.run(host._sync_loop_body({"status": "running", "errors": []}, 0.0))
        assert first["status"] == "slice_partial"
        # Slices 0-49, 50-99, 100-149 committed; 150 failed.
        assert state.load()["last_block"] == 149
        assert highwater.load() == 149
        assert host.attempted == [(0, 49), (50, 99), (100, 149), (150, 199)]

        host.attempted.clear()
        host.failing_slice = None
        second = asyncio.run(host._sync_loop_body({"status": "running", "errors": []}, 0.0))

    assert host.attempted[0] == (150, 199)  # resumed at slice N, not (0, 49)
    assert (0, 49) not in host.attempted
    assert second["status"] == "ok"  # last slice completed the window
    assert state.load()["last_block"] == 249
