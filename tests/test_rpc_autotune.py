"""Auto-tune soft rate limiting tests — NodeCore-style strain adaptation.

Contracts:

* healthy window (0% errors) → spacing stays 0
* error rate >= threshold → spacing widens (doubles per window, capped)
* clean window after strain → spacing halves back toward 0
* insufficient signal (<10 calls) → no adjustment
* disabled via RPC_AUTOTUNE_ENABLED=0 → no state, no adjustment
"""

from __future__ import annotations

import time

import pytest

from lite_indexer.rpc import autotune as at


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(at, "AUTOTUNE_ENABLED", True)
    at.reset()
    yield
    at.reset()


def _burst(ep: str, n: int, *, errors: int, start: float, spread: float = 1.0):
    for i in range(n):
        # first ``errors`` calls fail, remainder succeed
        at.record_call(ep, ok=(i >= errors and errors != n) or errors == 0, now=start + i * spread / max(n, 1))


def test_healthy_window_keeps_spacing_zero():
    ep = "https://healthy.example"
    _burst(ep, 20, errors=0, start=time.time() - 60)
    assert at.current_spacing(ep) == 0.0


def test_errors_widen_spacing():
    ep = "https://straining.example"
    t0 = time.time() - 300
    # Window 1: 40% errors → trips.
    _burst(ep, 10, errors=4, start=t0)
    s1 = at.current_spacing(ep)
    assert s1 > 0
    # Window 2: still bad → widens further.
    _burst(ep, 10, errors=5, start=t0 + 120)
    s2 = at.current_spacing(ep)
    assert s2 > s1


def test_spacing_capped():
    ep = "https://capped.example"
    t0 = time.time() - 1200
    for w in range(6):  # six consecutive bad windows
        _burst(ep, 10, errors=8, start=t0 + w * 120)
    assert at.current_spacing(ep) == at.AUTOTUNE_MAX_SPACING


def test_recovery_halves_spacing():
    ep = "https://recovering.example"
    t0 = time.time() - 600
    _burst(ep, 10, errors=6, start=t0)
    assert at.current_spacing(ep) > 0
    # Next window fully clean → halve.
    _burst(ep, 12, errors=0, start=t0 + 120)
    assert at.current_spacing(ep) < at.AUTOTUNE_STEP_S or at.current_spacing(ep) == 0.0


def test_insufficient_signal_no_adjustment():
    ep = "https://quiet.example"
    _burst(ep, 5, errors=3, start=time.time() - 30)  # all errors but <10 calls
    assert at.current_spacing(ep) == 0.0


def test_disabled_via_env(monkeypatch):
    monkeypatch.setattr(at, "AUTOTUNE_ENABLED", False)
    ep = "https://disabled.example"
    _burst(ep, 20, errors=15, start=time.time() - 60)
    assert at.current_spacing(ep) == 0.0


@pytest.mark.asyncio
async def test_autotune_wait_zero_spacing_is_instant():

    t0 = time.perf_counter()
    await at.autotune_wait("https://nospace.example")
    assert time.perf_counter() - t0 < 0.05
