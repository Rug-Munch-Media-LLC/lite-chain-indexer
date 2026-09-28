"""Stale-tip eviction tests (rpc/routing/health.py).

Contracts:

* An endpoint whose last reported height lags the pool by more than
  ``RPC_STALE_TIP_BLOCKS`` is reported stale and excluded from selection.
* Absence of height data never penalizes an endpoint (gate inactive).
* The demote transition is logged exactly once per episode and recovery is
  logged when the endpoint catches up.
* `select_p2c_healthy` never draws a stale endpoint.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.config import RPC_STALE_TIP_BLOCKS
from lite_indexer.rpc import routing
from lite_indexer.rpc.routing import health
from lite_indexer.rpc.routing.seams import set_selector

A = "https://a.stale-test.example"
B = "https://b.stale-test.example"
C = "https://c.stale-test.example"


class _RecLogger:
    """Captures structlog-style event calls for assertions."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **kw: Any) -> None:
        self.events.append((event, kw))

    def info(self, event: str, **kw: Any) -> None:
        self.events.append((event, kw))

    def debug(self, *a: Any, **k: Any) -> None:
        return None


@pytest.fixture(autouse=True)
def _fresh_health(monkeypatch: pytest.MonkeyPatch) -> _RecLogger:
    rec = _RecLogger()
    monkeypatch.setattr(health, "logger", rec)
    health.reset_latency_state()
    return rec


def _record(heights: dict[str, int]) -> None:
    for ep, h in heights.items():
        routing.record_endpoint_block_height(ep, h)


def test_threshold_is_env_configurable() -> None:
    assert health.STALENESS_MAX_BLOCKS == RPC_STALE_TIP_BLOCKS


def test_lagging_endpoint_is_stale() -> None:
    _record({A: 1000, B: 999, C: 990})  # median 999, C lags 9 > threshold
    assert routing.is_endpoint_stale(C, [A, B, C]) is True
    assert routing.is_endpoint_stale(A, [A, B, C]) is False
    assert routing.stale_endpoints([A, B, C]) == [C]


def test_gate_inactive_without_height_data() -> None:
    assert routing.stale_endpoints([A, B, C]) == []
    assert routing.is_endpoint_stale(C, [A, B, C]) is False


def test_demotion_logged_once_then_recovery(_fresh_health: _RecLogger) -> None:
    rec = _fresh_health
    _record({A: 1000, B: 999, C: 990})

    assert routing.stale_endpoints([A, B, C]) == [C]
    assert routing.stale_endpoints([A, B, C]) == [C]  # no duplicate log
    demoted = [e for e, _ in rec.events if e == "rpc_stale_tip_demoted"]
    assert len(demoted) == 1

    _record({C: 998})  # C catches up to within threshold
    assert routing.stale_endpoints([A, B, C]) == []
    assert [e for e, _ in rec.events if e == "rpc_stale_tip_recovered"]


def test_selector_excludes_stale_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    _record({A: 1000, B: 999, C: 980})  # C lags 19
    captured: list[list[str]] = []

    def _capture(candidates: list[str]) -> str:
        captured.append(list(candidates))
        return candidates[0]

    set_selector(_capture)
    chosen = routing.select_p2c_healthy([A, B, C])

    assert captured, "select_p2c was not called"
    assert C not in captured[0]
    assert chosen in {A, B}


def test_tip_failure_demotes_then_success_recovers() -> None:
    """A tip-incapable endpoint must be demoted, not left permanently fresh."""
    routing.record_endpoint_tip_failure(A)
    assert routing.is_endpoint_tip_failed(A) is False  # below threshold

    routing.record_endpoint_tip_failure(A)
    assert routing.is_endpoint_tip_failed(A) is True
    assert routing.tip_failed_endpoints([A, B, C]) == [A]

    routing.record_endpoint_block_height(A, 1000)  # serves a fresh tip
    assert routing.is_endpoint_tip_failed(A) is False
    assert routing.tip_failed_endpoints([A, B, C]) == []


def test_selector_excludes_tip_failed_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """select_p2c_healthy must not draw an endpoint stuck failing the tip poll."""
    routing.record_endpoint_tip_failure(C)
    routing.record_endpoint_tip_failure(C)
    captured: list[list[str]] = []

    def _capture(candidates: list[str]) -> str:
        captured.append(list(candidates))
        return candidates[0]

    set_selector(_capture)
    chosen = routing.select_p2c_healthy([A, B, C])

    assert captured, "select_p2c was not called"
    assert C not in captured[0]
    assert chosen in {A, B}


def test_historical_read_never_lowers_recorded_tip() -> None:
    """A backfill read (old block number) must not mark a healthy tip stale."""
    routing.record_endpoint_block_height(A, 1000)
    routing.record_endpoint_block_height(A, 1)  # old requested block
    assert routing.endpoint_block_height(A) == 1000
