"""Env-tunable Solana concurrency knobs: defaults, overrides, garbage input."""
import importlib
import os
from unittest.mock import patch

import lite_indexer.strategies.solana.models as models


def _reload_with_env(env: dict[str, str]) -> int:
    """Reload models with patched env and return the knob value."""
    with patch.dict(os.environ, env, clear=True):
        mod = importlib.reload(models)
        value = mod.SOLANA_TX_CONCURRENCY
    importlib.reload(models)  # restore module state for other tests
    return value


def test_tx_concurrency_default_is_30() -> None:
    assert _reload_with_env({}) == 30


def test_tx_concurrency_env_override() -> None:
    assert _reload_with_env({"SOLANA_TX_CONCURRENCY": "12"}) == 12


def test_tx_concurrency_garbage_falls_back_to_default() -> None:
    assert _reload_with_env({"SOLANA_TX_CONCURRENCY": "not-a-number"}) == 30


def test_tx_concurrency_blank_falls_back_to_default() -> None:
    assert _reload_with_env({"SOLANA_TX_CONCURRENCY": "   "}) == 30


def test_block_max_per_sync_default_is_200() -> None:
    with patch.dict(os.environ, {}, clear=True):
        mod = importlib.reload(models)
        assert mod.SOLANA_BLOCK_MAX_PER_SYNC == 200
    importlib.reload(models)


def test_block_max_per_sync_env_override() -> None:
    with patch.dict(os.environ, {"SOLANA_BLOCK_MAX_PER_SYNC": "50"}, clear=True):
        mod = importlib.reload(models)
        assert mod.SOLANA_BLOCK_MAX_PER_SYNC == 50
    importlib.reload(models)


def test_block_max_per_sync_garbage_falls_back_to_default() -> None:
    with patch.dict(os.environ, {"SOLANA_BLOCK_MAX_PER_SYNC": "abc"}, clear=True):
        mod = importlib.reload(models)
        assert mod.SOLANA_BLOCK_MAX_PER_SYNC == 200
    importlib.reload(models)


def test_env_int_helper_direct() -> None:
    with patch.dict(os.environ, {"KNOB": "7"}, clear=True):
        assert models._env_int("KNOB", 3) == 7
    assert models._env_int("MISSING_KNOB", 3) == 3
