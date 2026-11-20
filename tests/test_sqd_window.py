"""Windowed SQD pull: the cursor must resume exactly, never duplicate or skip.

These tests exist because the whole reason for windowing is a measured portal
ceiling, and the failure mode of a windowed cursor is silent: a cursor that
resumes one block early duplicates rows, one late silently drops them, and both
produce a parquet that looks fine until someone reconciles it against the chain.
"""

from __future__ import annotations

import json
from pathlib import Path

from lite_indexer.query.sqd_window import (
    DEFAULT_ROWS_PER_SITTING,
    WindowState,
    assemble_parts,
    clear_parts,
    load_state,
    save_state,
)


def _state(tmp: Path, **kw) -> WindowState:
    base = dict(chain="polygon", table="evm_transfers", day="2026-11-16", next_block=100, last_block=199)
    base.update(kw)
    st = WindowState(**base)
    save_state(tmp, st)
    return st


def test_rows_per_sitting_is_below_the_measured_shed_point() -> None:
    # Measured 2026-11-18: the portal trips at 61,352 rows. The sitting cap must
    # stay under it with headroom, because the batch that crosses the ceiling is
    # already refused.
    assert DEFAULT_ROWS_PER_SITTING < 61_352
    assert DEFAULT_ROWS_PER_SITTING >= 25_000, "too low and a day takes forever"


def test_state_round_trips(tmp_path: Path) -> None:
    st = _state(tmp_path, next_block=150, rows_so_far=500, sittings=2)
    back = load_state(tmp_path, st.chain, st.table, st.day)
    assert back is not None
    assert back.next_block == 150
    assert back.rows_so_far == 500
    assert back.sittings == 2
    assert back.complete is False


def test_missing_state_is_none_not_an_error(tmp_path: Path) -> None:
    assert load_state(tmp_path, "polygon", "evm_transfers", "2026-01-01") is None


def test_corrupt_state_is_none_not_an_exception(tmp_path: Path) -> None:
    # A truncated cursor file must not crash a batch job; it must restart the day.
    p = tmp_path / "polygon__evm_transfers__2026-11-16.json"
    p.write_text("{not json")
    assert load_state(tmp_path, "polygon", "evm_transfers", "2026-11-16") is None


def test_save_is_atomic_leaving_no_tmp(tmp_path: Path) -> None:
    st = _state(tmp_path)
    save_state(tmp_path, st)
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == [], f"atomic rename left {leftovers}"


def test_remaining_blocks_clamps_at_zero() -> None:
    done = WindowState(chain="c", table="t", day="d", next_block=200, last_block=199)
    assert done.remaining_blocks == 0
    todo = WindowState(chain="c", table="t", day="d", next_block=100, last_block=199)
    assert todo.remaining_blocks == 100


def test_remaining_blocks_counts_both_ends() -> None:
    # next_block == last_block means ONE block is still owed, not zero.
    one = WindowState(chain="c", table="t", day="d", next_block=199, last_block=199)
    assert one.remaining_blocks == 1


def test_assemble_parts_is_ordered_and_complete(tmp_path: Path) -> None:
    d = tmp_path / "parts"
    d.mkdir()
    # Written out of order on purpose: the assembler sorts by part index.
    (d / "part-001.ndjson").write_text(json.dumps({"block_number": 2}) + "\n")
    (d / "part-000.ndjson").write_text(json.dumps({"block_number": 1}) + "\n" + json.dumps({"block_number": 2}) + "\n")
    rows = assemble_parts(d)
    assert [r["block_number"] for r in rows] == [1, 2, 2]


def test_assemble_ignores_blank_lines(tmp_path: Path) -> None:
    d = tmp_path / "parts"
    d.mkdir()
    (d / "part-000.ndjson").write_text(json.dumps({"block_number": 1}) + "\n\n\n")
    assert len(assemble_parts(d)) == 1


def test_clear_parts_removes_only_parts(tmp_path: Path) -> None:
    d = tmp_path / "parts"
    d.mkdir()
    (d / "part-000.ndjson").write_text("{}\n")
    (d / "keep.txt").write_text("state lives elsewhere")
    assert clear_parts(d) == 1
    assert (d / "keep.txt").exists()
