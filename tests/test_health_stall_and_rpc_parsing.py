"""Tests for daemon health tracking + RPC response-parsing contracts.

Scope:
- ``health.ChainStallTracker`` — the auto-disable state machine that pauses
  stalled chains in production. Fully deterministic via injected ``now``.
- ``rpc.client.RpcClient.eth_block_number`` — hex parsing and the
  fail-to-None contract for every malformed response shape.

All expectations probed against actual behavior first (fleet doctrine).
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from lite_indexer.health import ChainStallTracker
from lite_indexer.rpc.client import RpcClient


# ── ChainStallTracker ───────────────────────────────────────────────────────


class TestChainStallTracker:
    def test_fresh_tracker_not_disabled(self):
        t = ChainStallTracker(stall_seconds=100)
        assert t.disabled is False
        assert t.stalled_seconds == 0.0

    def test_first_no_progress_starts_clock_does_not_disable(self):
        t = ChainStallTracker(stall_seconds=100)
        assert t.observe(500, 500, now=1000.0) is False
        assert t.disabled is False

    def test_no_progress_past_window_disables(self):
        t = ChainStallTracker(stall_seconds=100)
        t.observe(500, 500, now=1000.0)
        t.observe(500, 500, now=1050.0)  # under window
        assert t.observe(500, 500, now=1101.0) is True  # past window
        assert t.disabled is True

    def test_any_state_advance_counts_as_progress(self):
        t = ChainStallTracker(stall_seconds=100)
        t.observe(500, 500, now=1000.0)
        # None -> block also counts (first-ever state write).
        assert t.observe(None, 501, now=1105.0) is False
        assert t.disabled is False

    def test_progress_after_disable_re_enables(self):
        t = ChainStallTracker(stall_seconds=100)
        t.observe(500, 500, now=1000.0)
        assert t.observe(500, 500, now=1101.0) is True
        # State moves again -> chain re-enables immediately.
        assert t.observe(500, 600, now=1102.0) is False
        assert t.disabled is False

    def test_reset_clears_everything(self):
        t = ChainStallTracker(stall_seconds=100)
        t.observe(500, 500, now=1000.0)
        t.observe(500, 500, now=1101.0)
        t.reset()
        assert t.disabled is False
        assert t.stalled_seconds == 0.0

    def test_stalled_seconds_uses_wall_clock_not_injected_now(self):
        # KNOWN ASYMMETRY (pinned): observe() accepts an injected ``now`` for
        # deterministic tests, but stalled_seconds always reads time.time().
        # Injecting a historical now therefore yields a huge stalled value.
        # Production always passes real time, so this is consistent there —
        # pinned so the asymmetry is a documented decision, not a surprise.
        import time as _time

        t = ChainStallTracker(stall_seconds=10_000)
        t.observe(1, 2, now=1000.0)
        assert t.stalled_seconds > 1_000_000  # wall clock minus epoch-1970

        t2 = ChainStallTracker(stall_seconds=10_000)
        t2.observe(1, 2, now=_time.time())
        assert 0 <= t2.stalled_seconds < 60


# ── RpcClient.eth_block_number parsing ──────────────────────────────────────


@pytest.mark.asyncio
async def test_eth_block_number_parses_hex():
    c = RpcClient()
    c.call = AsyncMock(return_value={"result": "0x3e8"})
    assert await c.eth_block_number("https://endpoint") == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_response",
    [
        None,
        {},
        {"error": {"code": -32000, "message": "rate limited"}},
        {"result": "zzz-not-hex"},
        {"result": None},
        {"result": ""},
    ],
)
async def test_eth_block_number_fails_closed_to_none(bad_response):
    """Every malformed response shape must yield None — never raise.

    This is the resilience contract the fetch loop depends on: a broken
    endpoint degrades to 'no tip available', never crashes the daemon.
    """
    c = RpcClient()
    c.call = AsyncMock(return_value=bad_response)
    assert await c.eth_block_number("https://endpoint") is None


@pytest.mark.asyncio
async def test_eth_block_number_zero_is_valid_tip():
    # '0x0' is a legitimate response (fresh chain) — must parse as 0, not
    # be conflated with failure/None.
    c = RpcClient()
    c.call = AsyncMock(return_value={"result": "0x0"})
    assert await c.eth_block_number("https://endpoint") == 0
