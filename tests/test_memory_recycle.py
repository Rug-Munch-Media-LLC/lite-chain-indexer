"""Daemon hard memory ceiling (self-recycle) tests — 2026-09-13 OOM fix.

The daemon must exit cleanly for systemd to restart it with a fresh
heap once guarded memory reaches ``MEMORY_RECYCLE_MB``. Contracts:

* the decision helper is a strict ``>=`` ceiling and can be disabled,
* above the ceiling the process exits exactly once (``os._exit(0)``)
  and does not fall through / loop,
* below the ceiling it is a no-op,
* indexer clients are closed before the exit.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.cli import _daemon as daemon
from lite_indexer.config import MEMORY_RECYCLE_MB


class _FakeIndexer:
    """Minimal indexer stub recording ``aclose`` calls."""

    def __init__(self) -> None:
        self.closed = 0

    async def aclose(self) -> None:
        self.closed += 1


def test_recycle_threshold_is_inclusive_and_disableable() -> None:
    assert daemon.memory_recycle_needed(MEMORY_RECYCLE_MB) is True
    assert daemon.memory_recycle_needed(MEMORY_RECYCLE_MB + 1) is True
    assert daemon.memory_recycle_needed(MEMORY_RECYCLE_MB - 1) is False
    # 0 disables recycling (unbounded mode).
    assert daemon.memory_recycle_needed(999_999, threshold_mb=0) is False


async def test_recycle_exits_once_and_closes_clients(monkeypatch: pytest.MonkeyPatch) -> None:
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(code)

    monkeypatch.setattr(daemon.os, "_exit", fake_exit)
    indexer = _FakeIndexer()
    indexers: list[Any] = [indexer]

    # SystemExit proves the function does not swallow the exit and loop.
    with pytest.raises(SystemExit):
        await daemon.recycle_if_over_budget(MEMORY_RECYCLE_MB + 500, "ethereum", indexers)

    assert exits == [0]
    assert indexer.closed == 1


async def test_recycle_noop_below_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)

    monkeypatch.setattr(daemon.os, "_exit", fake_exit)
    indexer = _FakeIndexer()

    await daemon.recycle_if_over_budget(MEMORY_RECYCLE_MB - 1, "ethereum", [indexer])

    assert exits == []
    assert indexer.closed == 0
