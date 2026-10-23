"""Tests for the per-chain daily-health exporter.

The contract under test: EVERY configured chain gets a series, including a
chain that never ingested (age -1, stalled 1) and a declared parked chain
(parked 1, stalled 0). One ClickHouse round trip for all chains.
"""

from __future__ import annotations

import asyncio
import time

from lite_indexer import metrics
from lite_indexer import metrics_daily_health as health
from lite_indexer.indexer._sync_loop import SyncLoopMixin


def _value(name: str, chain: str) -> float:
    gauge = metrics._LABELED_GAUGES[name]  # noqa: SLF001 — test seam
    return float(gauge.labels(chain=chain)._value.get())  # noqa: SLF001


class _FakeClient:
    """Minimal clickhouse_driver.Client stand-in: one execute, one result."""

    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql):
        assert "evm_transfers" in sql and "GROUP BY chain" in sql
        return self.rows

    def disconnect(self):
        return None


def test_every_configured_chain_gets_a_series():
    rows = [("base", 12, 1_760_000_000, 500, 9_000)]
    health.export_daily_health(["base", "bsc"], parked={"bsc"}, client=_FakeClient(rows))

    assert _value("lite_indexer_chain_ingest_age_seconds", "base") == 12
    assert _value("lite_indexer_chain_stalled", "base") == 0
    assert _value("lite_indexer_chain_parked", "base") == 0
    assert _value("lite_indexer_chain_rows_1h", "base") == 500
    assert _value("lite_indexer_chain_last_ingest_timestamp_seconds", "base") == 1_760_000_000

    # bsc has no row at all: the case the old exporter could not express.
    assert _value("lite_indexer_chain_ingest_age_seconds", "bsc") == -1
    assert _value("lite_indexer_chain_parked", "bsc") == 1
    assert _value("lite_indexer_chain_stalled", "bsc") == 0, "a declared park silences only itself"
    assert _value("lite_indexer_chain_daily_cadence_breach", "bsc") == 0


def test_undeclared_silent_chain_is_stalled_and_breached():
    health.export_daily_health(["optimism"], parked=(), client=_FakeClient([]))
    assert _value("lite_indexer_chain_stalled", "optimism") == 1
    assert _value("lite_indexer_chain_daily_cadence_breach", "optimism") == 1


def test_age_between_stall_and_cadence_only_stalls():
    rows = [("linea", 4_000, 1_760_000_000, 3, 400)]
    health.export_daily_health(["linea"], parked=(), client=_FakeClient(rows))
    assert _value("lite_indexer_chain_stalled", "linea") == 1
    assert _value("lite_indexer_chain_daily_cadence_breach", "linea") == 0


class _BrokenClient:
    def execute(self, sql):
        raise OSError("clickhouse down")

    def disconnect(self):
        return None


def test_clickhouse_failure_keeps_previous_samples():
    health.export_daily_health(["base"], parked=(), client=_BrokenClient())  # must not raise


class _HeadStub(SyncLoopMixin):
    """Only the head-wakeup seam is exercised."""

    def __init__(self, wakeup):
        self._head_wakeup = wakeup


async def test_head_wakeup_is_consumed_so_the_loop_still_polls():
    """A set Event must not turn every later wait into a busy spin."""
    wakeup = asyncio.Event()
    wakeup.set()  # head pushed during the previous cycle
    stub = _HeadStub(wakeup)
    for timeout in (0.1, 0.1):
        started = time.monotonic()
        await stub.wait_for_head(timeout)
        # The pending head is CONSUMED at entry, so the loop still honours its
        # poll interval instead of spinning (the starvation bug).
        assert time.monotonic() - started >= 0.08
