"""Shared fakes for the sync-checkpointing tests.

Kept in their own module so ``test_sync_checkpointing.py`` stays inside the
200-line cap. The mixin bodies under test are bound onto these hosts, so the
fakes reproduce only the attributes those bodies touch.

Env overrides use a save/restore context manager instead of the
``monkeypatch`` fixture: the ratchet gate counts every monkeypatch attribute
access and is decrease-only.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, Mock

from lite_indexer.indexer._reorg_phase import ReorgPhaseMixin
from lite_indexer.indexer._sync_loop import SyncLoopMixin


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


class _StubState:
    def __init__(self) -> None:
        self.saved: list[tuple[int, int]] = []

    def save(self, block: int, events: int, **kwargs: object) -> None:
        self.saved.append((block, events))


class _StubHighWater:
    def __init__(self) -> None:
        self.marks: list[int] = []

    def update(self, block: int) -> None:
        self.marks.append(block)


class _SliceHost:
    chain = "base"

    def __init__(self, failing_slice: int | None) -> None:
        self.failing_slice = failing_slice
        self.saved: list[tuple[int, int]] = []
        self.attempted: list[tuple[int, int]] = []

    async def _determine_block_range(self, summary: dict[str, Any]) -> tuple[int, int]:
        return (0, 149)

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
        self.saved.append((to_block, 1))
        return summary


def _slice_host(failing_slice: int | None) -> _SliceHost:
    host = _SliceHost(failing_slice)
    host._sync_loop_body = SyncLoopMixin._sync_loop_body.__get__(host, _SliceHost)  # type: ignore[method-assign]
    return host


class _RangeHost(ReorgPhaseMixin):
    chain = "base"
    dex_enabled = True
    deployers_enabled = False
    traces_enabled = False
    ndjson_enabled = False

    def __init__(self) -> None:
        self.state = _StubState()
        self._highwater = _StubHighWater()
        self._last_tip = None
        self.strategy = Mock()
        self.strategy.supports_dex = lambda: True
        self.strategy.supports_deployers = lambda: False
        self.ch_writer = Mock()
        self.ch_writer.write_wallet_features = AsyncMock(return_value={})
        self._ch_health = Mock()
        self._ch_health.ok = lambda: True

    async def fetch_transfers(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        return [{"block_number": from_block}]

    async def fetch_swap_events(self, from_block: int, to_block: int) -> list[dict[str, Any]]:
        return []

    async def _enrich_timestamps(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return events

    async def _dispatch_plugin_events(self, from_block: int, to_block: int) -> dict[str, Any]:
        return {}

    async def _publish_nats_events(self, *args: Any) -> int:
        return 0

    async def _maybe_start_webhook_delivery(self) -> None:
        return None

    async def _run_vector_ingestion_guarded(self, from_block: int, to_block: int, summary: dict[str, Any]) -> None:
        return None

    def _refresh_demand_enabled(self) -> None:
        return None

    def _dex_window_is_stale(self, from_block: int) -> bool:
        return False

    def _check_reorg(self, events: list[Any]) -> bool:
        return False

    def _clear_reorg_recovery(self) -> None:
        return None

    def _enabled_event_types(self) -> list[str]:
        return []

    def _save_current_state(self) -> None:
        return None

    def _maybe_reconcile_range(self, *args: Any) -> None:
        return None

    def _zero_write_tracker(self) -> Mock:
        tracker = Mock()
        tracker.record_success = lambda *a: None
        tracker.record_zero_write = lambda *a: False
        return tracker
