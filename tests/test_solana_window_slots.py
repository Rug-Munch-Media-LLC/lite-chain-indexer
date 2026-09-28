"""Solana catch-up window sizing — every slot in the window is fetched.

Supersedes the adaptive-sampling-stride tests that lived here. Sampling
(``slot % stride == 0``) is gone: it discarded three of every four slots
while the highwater advanced past them, so throughput is now controlled by
the window depth alone. The window still absorbs a slow RPC — you shrink
the window, you never subsample it.
"""

from __future__ import annotations

import importlib
import inspect
import os
from unittest.mock import patch

from lite_indexer.strategies.solana import stride as stride_mod
from lite_indexer.strategies.solana.stride import (
    MAX_WINDOW_SLOTS,
    MIN_WINDOW_SLOTS,
    base_stride,
    window_slots,
)


def test_window_default_is_two_hundred_slots() -> None:
    with patch.dict(os.environ, {}, clear=True):
        assert window_slots() == 200


def test_window_env_override() -> None:
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "500"}, clear=True):
        assert window_slots() == 500


def test_legacy_knob_is_honoured_when_new_knob_absent() -> None:
    with patch.dict(os.environ, {"SOLANA_BLOCK_MAX_PER_SYNC": "77"}, clear=True):
        assert window_slots() == 77


def test_garbage_falls_back_to_default() -> None:
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "junk"}, clear=True):
        assert window_slots() == 200


def test_window_is_clamped_at_both_ends() -> None:
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "0"}, clear=True):
        assert window_slots() == MIN_WINDOW_SLOTS
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "99999999"}, clear=True):
        assert window_slots() == MAX_WINDOW_SLOTS


def test_negative_value_clamps_to_floor_not_a_modulus() -> None:
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "-8"}, clear=True):
        assert window_slots() == MIN_WINDOW_SLOTS


def test_base_stride_is_an_alias_for_the_window() -> None:
    with patch.dict(os.environ, {"SOLANA_WINDOW_SLOTS": "64"}, clear=True):
        assert base_stride() == 64
        assert base_stride() == window_slots()


def test_stride_modulo_is_gone() -> None:
    """The data-loss defect must not be reintroduced under a new name."""
    assert not hasattr(stride_mod, "next_sample_stride")
    src = importlib.import_module("lite_indexer.strategies.solana.strategy.transfers_blockwise")
    source = inspect.getsource(src)
    assert "% stride" not in source
    assert "% base_stride" not in source


def test_parquet_chunk_rows_env_and_cap() -> None:
    """Window rows: memory target, clamped by the safety cap (OOM lesson)."""
    from lite_indexer.storage.parquet import chunking as ch

    assert ch.chunk_rows(5_000_000) == 1_000_000  # default target
    assert ch.chunk_rows(250_000) == 250_000  # cap wins when smaller
    with patch.dict(os.environ, {"PARQUET_EXPORT_CHUNK_ROWS": "200000"}):
        reloaded = importlib.reload(ch)
        assert reloaded.chunk_rows(5_000_000) == 200_000
    importlib.reload(ch)


def test_module_reads_the_environment_at_call_time() -> None:
    """A reload must not be required for an operator to change the knob."""
    before = os.environ.get("SOLANA_WINDOW_SLOTS")
    try:
        os.environ["SOLANA_WINDOW_SLOTS"] = "321"
        assert window_slots() == 321
    finally:
        if before is None:
            os.environ.pop("SOLANA_WINDOW_SLOTS", None)
        else:
            os.environ["SOLANA_WINDOW_SLOTS"] = before
