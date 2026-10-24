"""Coverage tests for ``lite_indexer.indexer._checkpoint`` slice logic.

Hermetic — no network, no ClickHouse. Complements
``tests/test_sync_checkpointing.py`` (which owns the sync-loop
orchestration) by pinning the pure slice helpers and the abort-status
classification directly.

Env overrides use a small save/restore helper instead of the
``monkeypatch`` fixture: the ratchet gate counts every monkeypatch
attribute access and is decrease-only.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from lite_indexer.indexer import _checkpoint


@contextmanager
def _env(**values: str) -> Iterator[None]:
    """Set env vars for the block, restoring the prior state."""
    originals = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, original in originals.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


# ── slice_size ──────────────────────────────────────────────────────────


def test_slice_size_default_when_unset() -> None:
    with _env(SYNC_CHECKPOINT_SLICE=""):
        os.environ.pop("SYNC_CHECKPOINT_SLICE", None)
        assert _checkpoint.slice_size() == _checkpoint.DEFAULT_SYNC_CHECKPOINT_SLICE == 30


def test_slice_size_honors_positive_override() -> None:
    with _env(SYNC_CHECKPOINT_SLICE="7"):
        assert _checkpoint.slice_size() == 7


def test_slice_size_floors_at_one() -> None:
    with _env(SYNC_CHECKPOINT_SLICE="0"):
        assert _checkpoint.slice_size() == 1
    with _env(SYNC_CHECKPOINT_SLICE="-9"):
        assert _checkpoint.slice_size() == 1


def test_slice_size_junk_falls_back_to_default() -> None:
    with _env(SYNC_CHECKPOINT_SLICE="not-a-number"):
        assert _checkpoint.slice_size() == _checkpoint.DEFAULT_SYNC_CHECKPOINT_SLICE


# ── iter_slices ─────────────────────────────────────────────────────────


def test_iter_slices_exact_multiple() -> None:
    assert list(_checkpoint.iter_slices(0, 99, 50)) == [(0, 49), (50, 99)]


def test_iter_slices_partial_tail() -> None:
    assert list(_checkpoint.iter_slices(0, 149, 50)) == [(0, 49), (50, 99), (100, 149)]


def test_iter_slices_single_block_range() -> None:
    assert list(_checkpoint.iter_slices(10, 10, 50)) == [(10, 10)]


def test_iter_slices_empty_range_yields_nothing() -> None:
    assert list(_checkpoint.iter_slices(100, 99, 50)) == []


def test_iter_slices_size_zero_is_floored_to_one() -> None:
    assert list(_checkpoint.iter_slices(0, 2, 0)) == [(0, 0), (1, 1), (2, 2)]


def test_iter_slices_covers_range_without_gaps_or_overlap() -> None:
    slices = list(_checkpoint.iter_slices(5, 42, 7))
    assert slices[0][0] == 5
    assert slices[-1][1] == 42
    for (_, end), (next_start, _) in zip(slices, slices[1:], strict=False):
        assert next_start == end + 1


# ── abort-status classification ─────────────────────────────────────────


def test_hard_abort_statuses_are_disjoint_from_soft() -> None:
    assert _checkpoint.HARD_ABORT_STATUSES.isdisjoint(_checkpoint.SOFT_ABORT_STATUSES)


def test_hard_abort_statuses_cover_reorg_and_ch_outage() -> None:
    assert {
        "reorg_detected",
        "reorg_rollback",
        "reorg_status_unknown",
    } <= _checkpoint.HARD_ABORT_STATUSES
    assert {"ch_unavailable", "no_rpc_available"} <= _checkpoint.HARD_ABORT_STATUSES


def test_soft_abort_statuses_keep_earlier_checkpoints() -> None:
    assert {"fetch_failed", "ch_write_failed", "dex_write_failed", "writer_incomplete"} <= (
        _checkpoint.SOFT_ABORT_STATUSES
    )
