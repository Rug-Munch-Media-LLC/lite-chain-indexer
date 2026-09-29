"""Spend report: per-cost-class usage and avoidable-billable detection.

The report exists because nothing aggregated the per-endpoint usage ledger by
cost class, so "why am I burning provider quota when free sources exist" had no
answer. These tests pin the aggregation and, more importantly, the definition
of AVOIDABLE spend: a billable call made while a free endpoint was healthy is
a routing choice, not a necessity.
"""

from __future__ import annotations

import os

os.environ.setdefault("KEYED_RPC_KEY", "test-key")
os.environ.setdefault("SOLANA_PROVIDER_KEY", "test-key")

from lite_indexer.rpc.spend_report import (  # noqa: E402
    avoidable_calls,
    render,
    tally_by_class,
)

FREE = "https://arb1.arbitrum.io"
FREE2 = "https://arbitrum-rpc.publicnode.com"
FREEMIUM = "https://lb.drpc.org/ogrpc?network=arbitrum&dkey=abc123"


def _snap(**kw: dict) -> dict[str, dict]:
    base = {
        FREE: {"calls": 100, "errors": 0, "rate_limits": 0, "bytes_received": 1000, "methods": {}},
        FREEMIUM: {"calls": 10, "errors": 0, "rate_limits": 0, "bytes_received": 100, "methods": {}},
    }
    base.update(kw)
    return base


class TestTally:
    def test_separates_free_from_freemium(self) -> None:
        totals = tally_by_class(_snap())
        assert totals["free"].calls == 100
        assert totals["freemium"].calls == 10

    def test_counts_endpoints_not_calls(self) -> None:
        totals = tally_by_class(_snap(**{FREE2: {"calls": 5, "errors": 0, "rate_limits": 0, "bytes_received": 50}}))
        assert totals["free"].endpoints == 2
        assert totals["free"].calls == 105

    def test_sums_errors_and_rate_limits(self) -> None:
        snap = _snap(**{FREEMIUM: {"calls": 10, "errors": 3, "rate_limits": 2, "bytes_received": 100}})
        t = tally_by_class(snap)["freemium"]
        assert t.errors == 3
        assert t.rate_limits == 2
        assert t.error_ratio == 0.3

    def test_empty_ledger_does_not_divide_by_zero(self) -> None:
        assert tally_by_class({}) == {}
        totals = tally_by_class(_snap())
        assert all(t.error_ratio == 0.0 for t in totals.values())

    def test_missing_fields_default_to_zero(self) -> None:
        totals = tally_by_class({FREE: {"calls": 4}})
        assert totals["free"].calls == 4
        assert totals["free"].errors == 0


class TestAvoidableSpend:
    def test_billable_calls_while_free_healthy_are_avoidable(self) -> None:
        snap = _snap()
        totals = tally_by_class(snap)
        assert avoidable_calls(totals, snapshot=snap) == 10

    def test_nothing_avoidable_when_no_free_endpoint(self) -> None:
        snap = {FREEMIUM: {"calls": 10, "errors": 0, "rate_limits": 0, "bytes_received": 100}}
        totals = tally_by_class(snap)
        assert avoidable_calls(totals, snapshot=snap) == 0

    def test_nothing_avoidable_when_every_free_endpoint_is_failing(self) -> None:
        """This is the legitimate case: free was tried and genuinely broken."""
        snap = _snap(**{FREE: {"calls": 100, "errors": 40, "rate_limits": 0, "bytes_received": 1000}})
        totals = tally_by_class(snap)
        assert avoidable_calls(totals, snapshot=snap) == 0

    def test_one_healthy_free_endpoint_is_enough(self) -> None:
        """A second, healthy free endpoint is what makes the spend avoidable.

        One failing free endpoint does not excuse billable traffic when
        another free endpoint in the same ledger is clean.
        """
        snap = _snap(
            **{
                FREE: {"calls": 100, "errors": 9, "rate_limits": 0, "bytes_received": 100},
                FREE2: {"calls": 20, "errors": 0, "rate_limits": 0, "bytes_received": 200},
            }
        )
        totals = tally_by_class(snap)
        assert avoidable_calls(totals, snapshot=snap) == 10

    def test_pure_free_traffic_is_never_avoidable(self) -> None:
        snap = {FREE: {"calls": 500, "errors": 0, "rate_limits": 0, "bytes_received": 5000}}
        totals = tally_by_class(snap)
        assert avoidable_calls(totals, snapshot=snap) == 0


class TestRender:
    def test_reports_every_class_and_totals(self) -> None:
        snap = _snap()
        totals = tally_by_class(snap)
        out = render(totals, avoidable_calls(totals, snapshot=snap))
        assert "free" in out and "freemium" in out
        assert "total calls" in out
        assert "avoidable spend" in out
        assert "recent window" in out, "must not imply lifetime totals"

    def test_flags_avoidable_spend(self) -> None:
        snap = _snap()
        totals = tally_by_class(snap)
        out = render(totals, avoidable_calls(totals, snapshot=snap))
        assert "routing choice" in out

    def test_handles_empty_ledger(self) -> None:
        assert "total calls" in render({}, 0)
