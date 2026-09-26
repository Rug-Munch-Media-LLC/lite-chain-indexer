"""Regression: head-aware reconcile window (``LiteIndexerDataReconciliationMismatch``).

Bug (2026-09-26): the reconcile window was read AFTER the head had advanced
past it. ClickHouse returned ``count() == 0`` for the still-in-flight tail,
``discrepancy_pct`` came back at 100.0, and the indexer raised
``LiteIndexerDataReconciliationMismatch`` plus a bogus "state will be
rewound for backfill" warning.

Reproduced live on robinhood: head ``2626310``, durable
``max(block_number)`` ``2625507``, and the count for the head's tail window
``[2625508, 2626310]`` is ``0``.

Covered here:

1. ``resolve_reconcile_window`` — the head moved past the window, so the
   whole range sits above the durable max -> not verifiable, no mismatch
   (the regression itself).
2. Partial overlap -> window clamped to the durable max.
3. Range fully behind the durable max -> real drift is still caught.
4. End-to-end through ``_maybe_reconcile_range``: no backfill, no rewind,
   no discrepancy gauge, and no count query against the in-flight tail.

Hermetic — fake ClickHouse, no RPC, no network, no live indexer restart.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from lite_indexer.indexer import LiteIndexer
from lite_indexer.indexer._reconcile_window import resolve_reconcile_window

# Live robinhood numbers at the time of the incident.
HEAD = 2_626_310
DURABLE_MAX = 2_625_507


class FakeCH:
    """ClickHouse double modelling the in-flight tail.

    ``min/max(block_number)`` reports the durable max; any ``count()`` for a
    range that starts above it returns 0 — the exact live condition.
    """

    def __init__(self, durable_max: int = DURABLE_MAX) -> None:
        self.durable_max = durable_max
        self.queries: list[str] = []
        self.params: list[Any] = []

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        self.queries.append(query)
        self.params.append(params)
        if "min(" in query and "max(" in query:
            return [(0, self.durable_max, 1, 10)]
        if "count()" in query and "WHERE" in query:
            # Anything strictly above the durable max has no rows yet.
            seq = list(params or [])
            high = int(seq[2]) if len(seq) > 2 else 0
            if high > self.durable_max:
                return [(0,)]
            return [(10,)]
        return []


class FakeWriter:
    """ClickHouseWriter double exposing ``client()``."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def client(self) -> Any:
        return self._client


@pytest.fixture()
def indexer(tmp_path: Path):
    """A LiteIndexer backed by a fake store whose max lags the head."""
    with patch("lite_indexer.state.STATE_DIR", tmp_path / "state"):
        inst = LiteIndexer(chain="robinhood")
        inst._ch_writer = FakeWriter(FakeCH())
        yield inst


# ── 1. Window resolution (the regression) ─────────────────────────────


class TestReconcileWindowHeadAware:
    def test_range_above_durable_max_is_not_verifiable(self):
        """THE BUG: the head moved past the range -> no mismatch allowed."""
        window = resolve_reconcile_window(from_block=DURABLE_MAX + 1, to_block=HEAD, durable_max_block=DURABLE_MAX)
        assert window["verifiable"] is False
        assert window["ok"] is False
        assert window["reason"] == "ahead_of_durable_max"

    def test_not_verifiable_keeps_original_bounds(self):
        window = resolve_reconcile_window(from_block=DURABLE_MAX + 1, to_block=HEAD, durable_max_block=DURABLE_MAX)
        assert window["from_block"] == DURABLE_MAX + 1
        assert window["to_block"] == HEAD
        assert window["clamped"] is False

    def test_unknown_durable_max_is_not_verifiable(self):
        """Empty store / failed stats query must never yield a mismatch."""
        window = resolve_reconcile_window(from_block=1000, to_block=1100, durable_max_block=None)
        assert window["verifiable"] is False
        assert window["reason"] == "durable_max_unknown"

    def test_inverted_range_is_not_verifiable(self):
        window = resolve_reconcile_window(from_block=1100, to_block=1000, durable_max_block=5000)
        assert window["verifiable"] is False
        assert window["reason"] == "inverted_range"

    def test_partial_overlap_clamps_to_durable_max(self):
        """Head inside the range: verify only what the store can answer for."""
        window = resolve_reconcile_window(from_block=1000, to_block=HEAD, durable_max_block=HEAD - 50)
        assert window["verifiable"] is True
        assert window["from_block"] == 1000
        assert window["to_block"] == HEAD - 50
        assert window["clamped"] is True

    def test_window_fully_behind_durable_max_is_not_clamped(self):
        """Normal case — the whole range is durable, verify it in full."""
        window = resolve_reconcile_window(from_block=1000, to_block=1100, durable_max_block=1100)
        assert window["verifiable"] is True
        assert window["from_block"] == 1000
        assert window["to_block"] == 1100
        assert window["clamped"] is False

    def test_range_well_behind_durable_max_is_drift_territory(self):
        """A backwards-rewound range below the max is real drift, not a race."""
        window = resolve_reconcile_window(from_block=100, to_block=200, durable_max_block=50_000)
        assert window["verifiable"] is True
        assert window["to_block"] == 200
        assert window["clamped"] is False


# ── 2. End-to-end through _maybe_reconcile_range ──────────────────────


class TestReconcileDoesNotRewindWhenHeadMovedPast:
    def test_no_backfill_and_no_rewind_when_ahead_of_durable_max(self, indexer: LiteIndexer):
        indexer.state.save(HEAD, 0)
        indexer._cycles_since_reconcile = 0

        with (
            patch("lite_indexer.indexer._reconcile.DATA_RECONCILIATION_EVERY_N_CYCLES", 1),
            patch("lite_indexer.indexer._reconcile.rewind_cooldown_ok", return_value=True),
        ):
            summary: dict[str, Any] = {}
            # The cycle synced the in-flight tail the head has already passed.
            indexer._maybe_reconcile_range(DURABLE_MAX + 1, HEAD, expected=1000, summary=summary)

        # No mismatch, no backfill, no rewind, no discrepancy gauge.
        assert "reconcile_backfill_triggered" not in summary
        assert "reconcile_discrepancy_pct" not in summary
        assert summary.get("reconcile_skipped") == "ahead_of_durable_max"
        # State is untouched — the live indexer is never rewound here.
        assert indexer.state.load().get("last_block") == HEAD

    def test_no_count_query_against_the_in_flight_tail(self, indexer: LiteIndexer):
        """Skipped before the count — nothing is read from the store."""
        indexer.state.save(HEAD, 0)
        indexer._cycles_since_reconcile = 0
        fake = indexer._ch_writer.client()

        with patch("lite_indexer.indexer._reconcile.DATA_RECONCILIATION_EVERY_N_CYCLES", 1):
            indexer._maybe_reconcile_range(DURABLE_MAX + 1, HEAD, expected=1000, summary={})

        assert not any("count()" in q and "library" in q for q in fake.queries)

    def test_empty_store_is_skipped_not_reported_as_short(self, indexer: LiteIndexer):
        """A failed/empty stats query must not read as a 100% shortfall."""
        indexer._ch_writer = FakeWriter(FakeCH(durable_max=None))
        indexer.state.save(HEAD, 0)
        indexer._cycles_since_reconcile = 0

        with patch("lite_indexer.indexer._reconcile.DATA_RECONCILIATION_EVERY_N_CYCLES", 1):
            summary: dict[str, Any] = {}
            indexer._maybe_reconcile_range(1000, 1100, expected=1000, summary=summary)

        assert summary.get("reconcile_skipped") == "durable_max_unknown"
        assert "reconcile_backfill_triggered" not in summary

    def test_real_shortfall_below_durable_max_still_rewinds(self, indexer: LiteIndexer):
        """Guard the other direction: genuine drift must still be caught."""
        indexer.state.save(1100, 0)
        indexer._cycles_since_reconcile = 0
        # Store holds far fewer rows than the cycle fetched.
        indexer._ch_writer = FakeWriter(_ShortStoreCH(max_block=1100, count=100))

        with (
            patch("lite_indexer.indexer._reconcile.DATA_RECONCILIATION_EVERY_N_CYCLES", 1),
            patch("lite_indexer.indexer._reconcile.rewind_cooldown_ok", return_value=True),
        ):
            summary: dict[str, Any] = {}
            indexer._maybe_reconcile_range(1000, 1100, expected=1000, summary=summary)

        assert summary.get("reconcile_backfill_triggered") is True
        assert summary.get("reconcile_discrepancy_pct") == pytest.approx(90.0, abs=0.1)
        assert indexer.state.load().get("last_block") == 999

    def test_clamped_window_is_counted_up_to_durable_max(self, indexer: LiteIndexer):
        """Head inside the range -> the count uses the clamped upper bound."""
        indexer.state.save(HEAD, 0)
        indexer._cycles_since_reconcile = 0
        fake = indexer._ch_writer.client()

        with (
            patch("lite_indexer.indexer._reconcile.DATA_RECONCILIATION_EVERY_N_CYCLES", 1),
            patch("lite_indexer.indexer._reconcile.rewind_cooldown_ok", return_value=True),
        ):
            summary: dict[str, Any] = {}
            indexer._maybe_reconcile_range(1000, HEAD, expected=5, summary=summary)

        range_counts = [p for q, p in zip(fake.queries, fake.params, strict=True) if "count()" in q and "WHERE" in q and any(isinstance(v, int) for v in (p or {}).values())]
        # Clamped to the durable max, not the caller's HEAD.
        assert range_counts, "expected a numeric range count query"
        assert DURABLE_MAX in [v for v in range_counts[0].values() if isinstance(v, int)]
        assert summary.get("reconcile_window_clamped") is True


class _ShortStoreCH(FakeCH):
    """Store that reports a durable max but is short for the range."""

    def __init__(self, max_block: int, count: int) -> None:
        super().__init__(durable_max=max_block)
        self._count = count

    def execute(self, query: str, params: Any = None, with_column_types: bool = False) -> Any:
        self.queries.append(query)
        self.params.append(params)
        if "min(" in query and "max(" in query:
            return [(0, self.durable_max, 1, self._count)]
        if "count()" in query:
            return [(self._count,)]
        return []
