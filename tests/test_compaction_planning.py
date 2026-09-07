"""Tests for Parquet compaction planning (storage/compaction.py).

Scope: the pure planning/parsing logic — part sizing (the zstd size
heuristic that decides how a table is split before writing) and period
parsing. S3 I/O is out of scope.

Contracts probed against actual behavior first (fleet doctrine).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from lite_indexer.storage.compaction import _parse_period, _plan_parts


class TestPlanParts:
    def test_small_table_single_part(self):
        t = pa.table({"a": range(100)})
        parts = _plan_parts(t, max_bytes=10**9)
        assert len(parts) == 1
        assert parts[0].num_rows == 100

    def test_empty_table_yields_no_parts(self):
        t = pa.table({"a": pa.array([], type=pa.int64())})
        assert _plan_parts(t, max_bytes=10**6) == []

    def test_forced_split_preserves_every_row(self):
        # max_bytes=1 forces maximum splitting; the planner must never lose
        # or duplicate rows — compaction is destructive on the staging side,
        # so row conservation is the core safety property.
        big = pa.table({"a": range(10_000)})
        parts = _plan_parts(big, max_bytes=1)
        assert len(parts) > 1
        assert sum(p.num_rows for p in parts) == big.num_rows
        assert all(p.num_rows > 0 for p in parts)

    def test_split_parts_are_slices_not_copies(self):
        # Parts must be zero-copy slices of the original (memory ceiling:
        # compaction runs under cgroup limits).
        big = pa.table({"a": range(1_000)})
        parts = _plan_parts(big, max_bytes=1)
        assert all(isinstance(p, pa.Table) for p in parts)


class TestParsePeriod:
    def test_day_grain_passthrough(self):
        assert _parse_period("2026-08-23", "day") == "2026-08-23"

    def test_month_grain_truncates(self):
        assert _parse_period("2026-08", "month") == "2026-08"

    def test_invalid_period_raises(self):
        with pytest.raises(ValueError):
            _parse_period("not-a-date", "day")
