"""Adaptive consensus budgeting tests (rpc/consensus.py).

Contracts:

* The effective sample rate rises after a simulated mismatch burst and decays
  back toward the floor when the pool is stable.
* It always stays within [RPC_CONSENSUS_SAMPLE_MIN, RPC_CONSENSUS_SAMPLE_MAX].
* Adaptive off → the fixed RPC_CONSENSUS_SAMPLE_RATE (clamped); consensus
  disabled → 0.0 and ``should_sample`` is False.
* The effective rate is exported as the ``rpc_consensus_effective_rate``
  gauge.

Hermetic — no network.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from lite_indexer import metrics
from lite_indexer.rpc import consensus


@pytest.fixture(autouse=True)
def _deterministic():
    """Pin config and neutralise live health signals for determinism."""
    consensus.reset_adaptive_state()
    metrics.reset()
    with (
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_ENABLED", True),
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_ADAPTIVE", True),
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_SAMPLE_RATE", 0.05),
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_SAMPLE_MIN", 0.01),
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_SAMPLE_MAX", 0.25),
        patch("lite_indexer.rpc.consensus._health_risk", lambda: 0.0),
        patch("lite_indexer.rpc.consensus._stale_risk", lambda: 0.0),
        patch("lite_indexer.rpc.consensus._unvetted_risk", lambda: 0.0),
    ):
        yield
    consensus.reset_adaptive_state()
    metrics.reset()


def test_rate_rises_after_mismatch_burst() -> None:
    initial = consensus.effective_sample_rate()
    for _ in range(30):
        consensus._record_verification(mismatch=True)  # noqa: SLF001
    elevated = consensus.effective_sample_rate()
    assert elevated > initial
    assert elevated == pytest.approx(0.25)


def test_rate_decays_when_stable() -> None:
    for _ in range(30):
        consensus._record_verification(mismatch=True)  # noqa: SLF001
    high = consensus.effective_sample_rate()

    rates = [high]
    for _ in range(60):
        consensus._record_verification(mismatch=False)  # noqa: SLF001
        rates.append(consensus.effective_sample_rate())

    assert rates[-1] < high
    assert rates[-1] == pytest.approx(0.01, abs=1e-3)
    # Monotonic decay (no re-rises while outcomes stay clean).
    assert all(b <= a + 1e-9 for a, b in zip(rates, rates[1:], strict=False))


def test_rate_stays_within_bounds() -> None:
    for _ in range(200):
        consensus._record_verification(mismatch=True)  # noqa: SLF001
        assert 0.01 - 1e-9 <= consensus.effective_sample_rate() <= 0.25 + 1e-9
    for _ in range(200):
        consensus._record_verification(mismatch=False)  # noqa: SLF001
        assert 0.01 - 1e-9 <= consensus.effective_sample_rate() <= 0.25 + 1e-9


def test_health_and_stale_signals_raise_rate() -> None:
    base = consensus.effective_sample_rate()
    with patch("lite_indexer.rpc.consensus._stale_risk", lambda: 1.0):
        assert consensus.effective_sample_rate() > base

    with (
        patch("lite_indexer.rpc.consensus._stale_risk", lambda: 0.0),
        patch("lite_indexer.rpc.consensus._health_risk", lambda: 1.0),
    ):
        assert consensus.effective_sample_rate() > base


def test_adaptive_disabled_uses_fixed_rate() -> None:
    with patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_ADAPTIVE", False):
        for _ in range(50):
            consensus._record_verification(mismatch=True)  # noqa: SLF001
        assert consensus.effective_sample_rate() == pytest.approx(0.05)


def test_adaptive_disabled_clamps_base_into_bounds() -> None:
    with (
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_ADAPTIVE", False),
        patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_SAMPLE_RATE", 0.9),
    ):
        assert consensus.effective_sample_rate() == pytest.approx(0.25)


def test_consensus_disabled_returns_zero() -> None:
    with patch("lite_indexer.rpc.consensus.RPC_CONSENSUS_ENABLED", False):
        assert consensus.effective_sample_rate() == 0.0
        assert consensus.should_sample() is False


def test_effective_rate_gauge_exported() -> None:
    for _ in range(30):
        consensus._record_verification(mismatch=True)  # noqa: SLF001
    rate = consensus.effective_sample_rate()
    assert metrics.gauge("rpc_consensus_effective_rate") == pytest.approx(rate)


def test_should_sample_honours_effective_rate() -> None:
    with patch("lite_indexer.rpc.consensus.effective_sample_rate", lambda: 1.0):
        assert consensus.should_sample() is True
    with patch("lite_indexer.rpc.consensus.effective_sample_rate", lambda: 0.0):
        assert consensus.should_sample() is False
