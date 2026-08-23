"""Tests for chain-lag health reporting (health.check_chain_lag).

The lag report is the operator's "which chains are falling behind?" view
and feeds the stall watchdog. These tests pin all five status outcomes
with mocked RPC tips and temp state files — no network.

Contracts probed against actual behavior first (fleet doctrine).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import pytest

import lite_indexer.health as H


def _write_state(state_dir: str, chain: str, block: int) -> str:
    path = os.path.join(state_dir, f"lite_{chain}.json")
    with open(path, "w") as f:
        json.dump({"last_block": block}, f)
    return path


def _run_lag(state_dir: str, tip_holder: dict[str, Any], chain: str = "base"):
    async def _tip():
        result = tip_holder["fn"]()
        if asyncio.iscoroutine(result):
            return await result
        return result

    # NOTE: create_strategy is called WITHOUT await — the factory itself
    # must be sync; only fetch_block_number is awaited.
    def _fake_strategy(*args: Any, **kwargs: Any):
        return type("_S", (), {"fetch_block_number": staticmethod(_tip)})()

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(H, "STATE_DIR", state_dir)
        mp.setattr(H, "create_strategy", _fake_strategy)
        return asyncio.run(H.check_chain_lag([chain]))[chain]


class TestChainLagStatuses:
    def test_fresh_state_behind_tip_reports_behind(self, tmp_path):
        _write_state(str(tmp_path), "base", 1000)
        info = _run_lag(str(tmp_path), {"fn": lambda: 1005})
        assert info["status"] == "behind"
        assert info["lag_blocks"] == 5
        # base block time from _CHAIN_BLOCK_TIMES; lag_seconds derived.
        assert info["lag_seconds"] >= 0

    def test_exact_tip_is_ok(self, tmp_path):
        _write_state(str(tmp_path), "base", 1000)
        info = _run_lag(str(tmp_path), {"fn": lambda: 1000})
        assert info["status"] == "ok"
        assert info["lag_blocks"] == 0

    def test_missing_state_file_is_no_state(self, tmp_path):
        info = _run_lag(str(tmp_path), {"fn": lambda: 500})
        assert info["status"] == "no_state"

    def test_old_state_file_is_stale_even_with_small_lag(self, tmp_path):
        path = _write_state(str(tmp_path), "base", 1000)
        old = time.time() - 100_000  # ~27h old — past any poll interval
        os.utime(path, (old, old))
        info = _run_lag(str(tmp_path), {"fn": lambda: 1001})
        assert info["status"] == "stale"

    def test_rpc_timeout_reports_error_not_crash(self, tmp_path):
        _write_state(str(tmp_path), "base", 1000)
        os.utime(_write_state(str(tmp_path), "base", 1000))  # fresh mtime

        async def boom():
            raise TimeoutError()

        info = _run_lag(str(tmp_path), {"fn": boom})
        assert info["status"] == "error"

    def test_corrupt_state_json_treated_as_no_state(self, tmp_path):
        path = tmp_path / "lite_base.json"
        path.write_text("{not json")
        info = _run_lag(str(tmp_path), {"fn": lambda: 500})
        assert info["status"] == "no_state"


class TestLagMath:
    def test_lag_seconds_uses_chain_block_time(self, tmp_path):
        _write_state(str(tmp_path), "base", 1000)
        info = _run_lag(str(tmp_path), {"fn": lambda: 1010})
        expected_block_time = H._CHAIN_BLOCK_TIMES.get("base", 12.0)
        assert info["lag_seconds"] == int(10 * expected_block_time)
