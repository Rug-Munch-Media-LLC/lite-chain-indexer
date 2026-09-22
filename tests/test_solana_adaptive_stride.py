"""Adaptive sampling stride: escalate on slow RPC, recover fast, stay bounded."""
import os

import pytest

from lite_indexer.strategies.solana.stride import base_stride, next_sample_stride


def test_base_default_is_four() -> None:
    from unittest.mock import patch

    with patch.dict(os.environ, {}, clear=True):
        assert base_stride() == 4


def test_base_env_override_and_garbage() -> None:
    from unittest.mock import patch

    with patch.dict(os.environ, {"SOLANA_SAMPLE_STRIDE": "8"}, clear=True):
        assert base_stride() == 8
    with patch.dict(os.environ, {"SOLANA_SAMPLE_STRIDE": "junk"}, clear=True):
        assert base_stride() == 4


def test_slow_fetch_escalates_from_base() -> None:
    assert next_sample_stride(None, 46.0, 4) == 8


def test_slow_fetch_caps_at_two_x_base() -> None:
    assert next_sample_stride(8, 46.0, 4) == 8  # already at cap


def test_fast_fetch_recovers_to_base() -> None:
    assert next_sample_stride(8, 0.8, 4) == 4


def test_mid_range_keeps_current() -> None:
    assert next_sample_stride(8, 2.5, 4) == 8
    assert next_sample_stride(None, 2.5, 4) == 4


def test_never_below_base() -> None:
    assert next_sample_stride(2, 0.5, 4) == 4
