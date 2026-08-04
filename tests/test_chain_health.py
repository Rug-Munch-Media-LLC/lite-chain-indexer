"""Tests for chain-level health / auto-disable (lite_indexer/health.py).

Covers the ``ChainStallTracker`` used by the daemon to auto-disable a
chain whose state has not advanced for the stall window (default 2h):

1. State advance → chain stays enabled / re-enables
2. State frozen for the stall window → chain disables
3. Disabled chain re-enables as soon as its state moves again
4. The tracker is independent per chain (fresh instance each chain)

Hermetic — no network, no ClickHouse.
"""

from __future__ import annotations

import time

from lite_indexer.health import ChainStallTracker


class TestChainStallTracker:
    def test_state_advance_stays_enabled(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        assert t.observe(100, 100, now=now) is False
        assert t.observe(100, 150, now=now + 60) is False
        assert t.disabled is False

    def test_stalled_for_window_disables(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        # First cycle: no state advance, clock starts.
        t.observe(100, 100, now=now)
        # 2h later, still frozen → disabled.
        assert t.observe(100, 100, now=now + 7200) is True
        assert t.disabled is True

    def test_does_not_disable_before_window(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        t.observe(100, 100, now=now)
        # 1h later → not yet disabled.
        assert t.observe(100, 100, now=now + 3600) is False
        assert t.disabled is False

    def test_reenables_after_state_moves(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        t.observe(100, 100, now=now)
        assert t.observe(100, 100, now=now + 7200) is True
        assert t.disabled is True
        # State moves → re-enabled.
        assert t.observe(100, 101, now=now + 7300) is False
        assert t.disabled is False

    def test_no_state_file_is_treated_as_no_progress(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        t.observe(None, None, now=now)
        assert t.observe(None, None, now=now + 7200) is True

    def test_stalled_seconds_tracks(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        t.observe(100, 100, now=now)
        # Frozen from the first observation: elapsed is ~0 at this instant.
        assert 0.0 <= t.stalled_seconds < 1.0

    def test_reset_clears(self):
        t = ChainStallTracker(stall_seconds=7200)
        now = time.time()
        t.observe(100, 100, now=now)
        t.observe(100, 100, now=now + 7200)
        assert t.disabled is True
        t.reset()
        assert t.disabled is False
        assert t.stalled_seconds == 0.0
