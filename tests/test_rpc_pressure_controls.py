"""Tests for the 2026-10-23 RPC-pressure controls.

Covers the knobs that bound how hard the daemon hits the shared free pool:

* the active-chain cap and the start stagger defaults,
* the env override path for both (a deployment tunes them without a rebuild),
* the 429-streak soft-ban depth (threshold / base / cap) derived from env.

Hermetic — no network, no disk, no daemon.
"""

from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture(autouse=True)
def _restore_config_modules():
    """Reload after each test so an env override never leaks into the suite."""
    yield
    importlib.reload(importlib.import_module("lite_indexer.config.indexing"))
    importlib.reload(importlib.import_module("lite_indexer.config.rpc"))
    importlib.reload(importlib.import_module("lite_indexer.rpc.ban_state"))


def test_pressure_defaults_bound_the_free_pool():
    """Defaults must keep the pool under its measured free-tier ceiling."""
    from lite_indexer.config import indexing

    # Six chains x two chunks = 12 in-flight getLogs, half the 24 that stormed
    # on 2026-10-23 (6 chains x CHUNK_PARALLELISM=4). Slots buy the round time
    # that keeps 15 chains inside the 900 s staleness bound; chunks are the
    # pool pressure. Raise neither without re-measuring 429 streaks.
    assert 1 <= indexing.MAX_CONCURRENT_CHAINS <= 6
    assert 1 <= indexing.CHUNK_PARALLELISM <= 2
    # A non-zero stagger keeps startup verification off the pool in one burst.
    assert indexing.CHAIN_START_STAGGER_SECONDS > 0


def test_pressure_knobs_are_env_overridable(monkeypatch: pytest.MonkeyPatch):
    """An operator can re-tune the controls from the deployment env file."""
    monkeypatch.setenv("MAX_CONCURRENT_CHAINS", "5")
    monkeypatch.setenv("CHUNK_PARALLELISM", "3")
    monkeypatch.setenv("CHAIN_START_STAGGER_SECONDS", "0")
    indexing = importlib.reload(importlib.import_module("lite_indexer.config.indexing"))

    assert indexing.MAX_CONCURRENT_CHAINS == 5
    assert indexing.CHUNK_PARALLELISM == 3
    assert indexing.CHAIN_START_STAGGER_SECONDS == 0.0


def test_softban_windows_deepen_with_streak(monkeypatch: pytest.MonkeyPatch):
    """A 429 streak must sit the endpoint out longer than the old 300s cap."""
    import lite_indexer.rpc.ban_state as ban_state

    monkeypatch.setattr(ban_state, "_endpoint_429_streaks", {})
    monkeypatch.setattr(ban_state, "_endpoint_bans", {})
    monkeypatch.setattr(ban_state, "_persist_ban", lambda endpoint: None)
    endpoint = "https://example.invalid/rpc"

    windows = [ban_state.record_429_streak(endpoint) for _ in range(6)]

    threshold = ban_state._RATE_LIMIT_STREAK_BAN_THRESHOLD
    assert windows[: threshold - 1] == [None] * (threshold - 1)
    enforced = [w for w in windows if w]
    assert enforced, "a streak at the threshold must produce a soft-ban window"
    assert max(enforced) >= 600
    assert max(enforced) <= ban_state._RATE_LIMIT_STREAK_BAN_CAP_S


def test_softban_env_is_read(monkeypatch: pytest.MonkeyPatch):
    """The deployment env file drives the soft-ban depth, not a constant."""
    monkeypatch.setenv("ENDPOINT_SOFTBAN_STREAK_THRESHOLD", "5")
    monkeypatch.setenv("ENDPOINT_SOFTBAN_BASE_SECONDS", "900")
    monkeypatch.setenv("ENDPOINT_SOFTBAN_CAP_SECONDS", "3600")
    ban_state = importlib.reload(importlib.import_module("lite_indexer.rpc.ban_state"))

    assert ban_state._RATE_LIMIT_STREAK_BAN_THRESHOLD == 5
    assert ban_state._RATE_LIMIT_STREAK_BAN_BASE_S == 900
    assert ban_state._RATE_LIMIT_STREAK_BAN_CAP_S == 3600


def test_softban_env_unset_restores_shipped_defaults(monkeypatch: pytest.MonkeyPatch):
    """No env override => the compiled defaults (10 min base, 30 min cap)."""
    for key in (
        "ENDPOINT_SOFTBAN_STREAK_THRESHOLD",
        "ENDPOINT_SOFTBAN_BASE_SECONDS",
        "ENDPOINT_SOFTBAN_CAP_SECONDS",
    ):
        monkeypatch.delenv(key, raising=False)
    ban_state = importlib.reload(importlib.import_module("lite_indexer.rpc.ban_state"))

    assert os.environ.get("ENDPOINT_SOFTBAN_BASE_SECONDS") is None
    assert ban_state._RATE_LIMIT_STREAK_BAN_THRESHOLD == 4
    assert ban_state._RATE_LIMIT_STREAK_BAN_BASE_S == 600
    assert ban_state._RATE_LIMIT_STREAK_BAN_CAP_S == 1800
