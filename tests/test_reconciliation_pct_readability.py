"""Regression guard for the bogus ``1e+04%`` reconciliation metric.

Observed 2026-09-25 (incident lane 4): reconciliation reported a mismatch of
``1e+04%`` for ``hyperliquid`` and ``robinhood``. Two distinct defects fed it:

1. ``_classify`` divided by a near-zero / not-like-for-like on-chain balance,
   producing astronomically large ratios (median ``5.2e+12%``, max
   ``9.6e+28%`` on robinhood) that ``json.dumps`` then rendered in scientific
   notation.
2. ``check_chain_lag`` / the lag report translated a block gap with a 12s
   default block time for chains without a ``_CHAIN_BLOCK_TIMES`` entry
   (robinhood, hyperliquid, blast, fraxtal, metis), turning a real gap into a
   nonsense "billions of seconds behind" figure.

These tests fail on the pre-fix code and pass after.
"""

from __future__ import annotations

import pytest

from lite_indexer.formatting import MAX_MEANINGFUL_PCT, format_duration, format_pct
from lite_indexer.health import _CHAIN_BLOCK_TIMES, SUPPORTED_CHAINS
from lite_indexer.storage.reconciliation import _classify


class TestClassifyClampsMeaninglessRatios:
    """A near-zero denominator must never emit a nonsense percentage."""

    def test_hyperliquid_style_ratio_is_clamped(self) -> None:
        """1.5e27 computed vs 1 wei on-chain must not yield ~1.5e29%."""
        _, pct = _classify(delta=1_518_644 * 10**15, on_chain=1)
        assert pct <= MAX_MEANINGFUL_PCT
        assert pct != pytest.approx(1e29, rel=0.5)

    def test_robinhood_style_ratio_is_clamped(self) -> None:
        """The observed robinhood max (9.59e+28%) clamps to the ceiling."""
        _, pct = _classify(delta=959_365_293_609_401_687_871_519_017, on_chain=0)
        assert pct == pytest.approx(MAX_MEANINGFUL_PCT)

    def test_pct_is_never_scientific_notation(self) -> None:
        """format_pct on a clamped ratio has no ``e+`` in it."""
        _, pct = _classify(delta=10**30, on_chain=0)
        rendered = format_pct(pct)
        assert "e+" not in rendered
        assert "e-" not in rendered

    def test_unit_mismatch_reported_as_absolute_not_ratio(self) -> None:
        """A mixed-decimal 1e27-vs-1e15 gap is 'absolute', not a percentage."""
        _, pct = _classify(delta=43514145269009532040035674, on_chain=1063254021860816)
        assert format_pct(pct) == ">1e6% (absolute)"


class TestFormatPct:
    def test_small_values_human_readable(self) -> None:
        assert format_pct(0.5) == "0.5000%"
        assert format_pct(2.0) == "2.00%"
        assert format_pct(42.0) == "42%"

    def test_large_values_suffix_not_scientific(self) -> None:
        assert format_pct(900.0) == "900%"
        assert format_pct(9000.0) == "9.0k%"
        assert format_pct(999_999.0) == "1000.0k%"
        assert format_pct(2_000_000.0) == ">1e6% (absolute)"
        assert format_pct(1e30) == ">1e6% (absolute)"

    def test_never_scientific_for_any_magnitude(self) -> None:
        for v in (1e-3, 1.0, 1e3, 1e6, 1e12, 1e28):
            assert "e+" not in format_pct(v)


class TestJsonOutputNeverScientific:
    """The reconcile CLI output must remain JSON-parseable and readable."""

    def test_delta_pct_human_present_and_readable(self) -> None:
        from lite_indexer.cli._integrity import _with_readable_pct

        results = {
            "0xabc": {"delta_pct": 9.593652936094016e28, "status": "MISMATCH"},
            "0xdef": {"delta_pct": 0.5, "status": "APPROXIMATE"},
        }
        out = _with_readable_pct(results)
        assert out["0xabc"]["delta_pct_human"] == ">1e6% (absolute)"
        assert out["0xdef"]["delta_pct_human"] == "0.5000%"
        for info in out.values():
            assert "e+" not in info["delta_pct_human"]


class TestBlockTimesCoverEveryChain:
    """Every configured chain needs a block time or lag_seconds is nonsense."""

    def test_every_supported_chain_has_a_block_time(self) -> None:
        missing = sorted(c for c in SUPPORTED_CHAINS if c not in _CHAIN_BLOCK_TIMES)
        assert missing == [], f"chains missing _CHAIN_BLOCK_TIMES: {missing}"

    def test_robinhood_and_hyperliquid_are_fast_chains(self) -> None:
        assert _CHAIN_BLOCK_TIMES["robinhood"] <= 0.5
        assert _CHAIN_BLOCK_TIMES["hyperliquid"] <= 2.0


class TestFormatDuration:
    def test_readable_units(self) -> None:
        assert format_duration(42) == "42s"
        assert format_duration(143 * 60) == "2h 23m"
        assert format_duration(544_257_900) == "6299d 6h"
        assert format_duration(-5) == "0s"
