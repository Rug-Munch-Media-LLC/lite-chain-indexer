"""Temporal freshness: the check that replaces the unreachable contiguity metric.

INCIDENT 2026-11-16
    The daily integrity audit measured

        contiguous_pct = uniqExact(block_number) / (max_block - min_block + 1)

    against a SAMPLING indexer, so it could never reach the 99% needed for
    ``status: ok``. It printed ``gaps`` on every chain for months and was
    ignored, while ten chains -- arbitrum, bsc, mantle, zksync, avalanche,
    fantom, tron, base, metis, robinhood -- held no rows for 20-24 days.

    A metric that can never pass is a mute gate, not a strict one. These tests
    pin the replacement: calendar-day freshness, which a sampling indexer can
    actually satisfy and which fails loudly when a writer dies.
"""

from __future__ import annotations

import pytest

from lite_indexer.freshness import (
    STALE_AFTER_DAYS,
    STATUS_EMPTY,
    STATUS_LAGGING,
    STATUS_OK,
    STATUS_STALE,
    FreshnessReport,
    classify,
    ok_for_scanner,
)


def test_fresh_chain_is_ok():
    """Yesterday's data with traffic in the last 24h is healthy."""
    v = classify("ethereum", "2026-11-16", 0, rows_recent=1_000)

    assert v.status == STATUS_OK
    assert v.detail == ""


def test_one_day_stale_is_tolerated():
    """A chain up to STALE_AFTER_DAYS behind absorbs a slow exporter run."""
    v = classify("base", "2026-11-15", STALE_AFTER_DAYS, rows_recent=5)

    assert v.status == STATUS_OK


def test_beyond_threshold_is_stale_and_names_the_day():
    """The real incident: arbitrum silent for 23 days must be STALE, not 'gaps'."""
    v = classify("arbitrum", "2026-10-24", 23, rows_recent=0)

    assert v.status == STATUS_STALE
    assert v.days_stale == 23
    assert "2026-10-24" in v.detail


def test_recent_day_but_silent_24h_is_lagging_not_ok():
    """The writer stopped partway through today: degraded, not healthy."""
    v = classify("polygon", "2026-11-16", 0, rows_recent=0)

    assert v.status == STATUS_LAGGING
    assert "nothing in the last 24h" in v.detail


def test_never_written_chain_is_empty_not_missing():
    """Silence is the failure, so a configured chain with no rows must appear."""
    v = classify("newchain", None, None, 0)

    assert v.status == STATUS_EMPTY
    assert "never produced" in v.detail


def test_report_orders_worst_first():
    """The most silent chain must lead the report, so triage starts there."""
    report = FreshnessReport(
        chains={
            "bsc": classify("bsc", "2026-10-24", 23, 0),
            "base": classify("base", "2026-10-27", 20, 0),
            "metis": classify("metis", "2026-10-23", 24, 0),
            "ethereum": classify("ethereum", "2026-11-16", 0, 900),
        }
    )

    assert [c.chain for c in report.worst()] == ["metis", "bsc", "base"]


def test_scanner_must_not_claim_fresh_with_a_stale_chain():
    """A scan answered from a 23-day-old chain is not a fresh scan."""
    report = FreshnessReport(chains={"arbitrum": classify("arbitrum", "2026-10-24", 23, 0)})

    assert ok_for_scanner(report) is False


def test_scanner_may_claim_fresh_when_all_chains_answer():
    """All-OK means freshness can be advertised."""
    report = FreshnessReport(
        chains={
            "ethereum": classify("ethereum", "2026-11-16", 0, 500),
            "solana": classify("solana", "2026-11-16", 0, 900_000),
        }
    )

    assert ok_for_scanner(report) is True


@pytest.mark.parametrize("days", [2, 10, 23, 24])
def test_every_day_past_threshold_is_stale(days: int):
    """No off-by-one at the boundary: threshold+ is always stale."""
    assert classify("x", "2026-10-01", days, 0).status == STATUS_STALE
