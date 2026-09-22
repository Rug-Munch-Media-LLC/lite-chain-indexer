"""Tests for the continuous memory watchdog thread (2026-09-14 wedge fix).

The post-cycle ``recycle_if_over_budget`` check cannot fire when the sync
cycle itself is throttled by the kernel cgroup MemoryHigh handler — the
daemon wedged 5h45m in D-state with 0 writes. These tests cover the
independent watchdog thread in ``lite_indexer.cli._daemon``:

- fires ``os._exit(0)`` at the ``MEMORY_RECYCLE_MB`` ceiling
- no-op below the ceiling
- disabled when ``MEMORY_WATCHDOG_ENABLED=0``
- no-op when ``MEMORY_RECYCLE_MB=0``
- interval honored (sleep mocked)
- thread is daemon=True and does not leak across tests

Hermetic — no ClickHouse, no RPC, no network.
"""

from __future__ import annotations

import contextlib
import threading
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.cli._daemon import start_memory_watchdog

MODULE = "lite_indexer.cli._daemon"


@pytest.fixture()
def no_threads_left():
    """Fail the test if a memory-watchdog thread survives it."""
    before = {t for t in threading.enumerate() if t.name == "memory-watchdog"}
    yield
    alive = {t for t in threading.enumerate() if t.name == "memory-watchdog" and t.is_alive() and t not in before}
    assert not alive, f"watchdog thread leaked: {alive}"


@contextlib.contextmanager
def _run_one_iteration(mem_mb: int, exits: list[int]):
    """Drive exactly one watchdog loop iteration with mocked IO.

    The patches stay active for the whole test (the caller enters this
    context manager), matching the original function-scoped monkeypatch.
    """

    def fake_sleep(_seconds: int) -> None:
        # Stop the loop after the first iteration so the thread exits.
        raise SystemExit(0)

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(0)

    with (
        patch(f"{MODULE}.get_guarded_memory_mb", lambda: mem_mb),
        patch(f"{MODULE}._watchdog_sleep", fake_sleep),
        patch(f"{MODULE}.os._exit", fake_exit),
    ):
        yield


def test_watchdog_exits_at_ceiling(no_threads_left: None):
    exits: list[int] = []
    with _run_one_iteration(mem_mb=3072, exits=exits):
        thread = start_memory_watchdog(())
        assert thread is not None
        thread.join(timeout=5)
    assert exits == [0]


def test_watchdog_noop_below_ceiling(no_threads_left: None):
    exits: list[int] = []
    with _run_one_iteration(mem_mb=1024, exits=exits):
        thread = start_memory_watchdog(())
        assert thread is not None
        thread.join(timeout=5)
    assert exits == []


def test_watchdog_disabled_when_env_zero(no_threads_left: None):
    with patch(f"{MODULE}.MEMORY_WATCHDOG_ENABLED", False):
        assert start_memory_watchdog(()) is None


def test_watchdog_noop_when_recycle_zero(no_threads_left: None):
    with patch(f"{MODULE}.MEMORY_RECYCLE_MB", 0):
        assert start_memory_watchdog(()) is None


def test_watchdog_interval_honored(no_threads_left: None):
    sleeps: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleeps.append(seconds)
        raise SystemExit(0)

    with (
        patch(f"{MODULE}.get_guarded_memory_mb", lambda: 100),
        patch(f"{MODULE}._watchdog_sleep", fake_sleep),
        patch(f"{MODULE}.os._exit", lambda code: None),
        patch(f"{MODULE}.MEMORY_WATCHDOG_INTERVAL_S", 7),
    ):
        thread = start_memory_watchdog(())
        assert thread is not None
        thread.join(timeout=5)
    assert sleeps == [7]


def test_watchdog_thread_is_daemon(no_threads_left: None):
    def fake_sleep(_seconds: int) -> None:
        raise SystemExit(0)

    with (
        patch(f"{MODULE}.get_guarded_memory_mb", lambda: 100),
        patch(f"{MODULE}._watchdog_sleep", fake_sleep),
        patch(f"{MODULE}.os._exit", lambda code: None),
    ):
        thread = start_memory_watchdog(())
        assert thread is not None
        assert thread.daemon is True
        thread.join(timeout=5)


def test_watchdog_exit_is_idempotent(no_threads_left: None):
    """os._exit fires at most once even if the ceiling check re-triggers."""
    exits: list[int] = []
    calls = {"n": 0}

    def fake_mem() -> int:
        calls["n"] += 1
        return 3072

    def fake_sleep(_seconds: int) -> None:
        raise SystemExit(0)

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(0)

    with (
        patch(f"{MODULE}.get_guarded_memory_mb", fake_mem),
        patch(f"{MODULE}._watchdog_sleep", fake_sleep),
        patch(f"{MODULE}.os._exit", fake_exit),
    ):
        thread = start_memory_watchdog(())
        assert thread is not None
        thread.join(timeout=5)
    assert exits == [0]
    assert calls["n"] == 1


def test_watchdog_close_failure_still_exits(no_threads_left: None):
    """A client-close failure must not prevent the recycle exit."""
    exits: list[int] = []

    class Boom:
        def aclose(self) -> Any:
            raise RuntimeError("close failed")

    with _run_one_iteration(mem_mb=3072, exits=exits):
        thread = start_memory_watchdog((Boom(),))  # type: ignore[arg-type]
        assert thread is not None
        thread.join(timeout=5)
    assert exits == [0]


def test_watchdog_logs_pre_emergency_at_high(caplog: pytest.LogCaptureFixture, no_threads_left: None):
    """At MEMORY_HIGH_MB (below recycle) it logs pressure but does not exit."""
    exits: list[int] = []
    # Pin the high threshold so the test doesn't depend on the ambient env
    # value, and raise the recycle ceiling above it.
    # The watchdog resolves the recycle ceiling per tick — seam
    # resolve_recycle_mb to raise it above the high threshold.
    with (
        patch(f"{MODULE}.MEMORY_HIGH_MB", 4096),
        patch(f"{MODULE}.resolve_recycle_mb", lambda *a, **k: 8192),
        patch(f"{MODULE}.logger") as log,
        _run_one_iteration(mem_mb=4096, exits=exits),
    ):
        thread = start_memory_watchdog(())
        assert thread is not None
        thread.join(timeout=5)
        assert exits == []
        assert log.warning.call_args_list[-1].args[0] == "memory_high_watchdog"
