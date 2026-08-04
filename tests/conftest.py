"""
Shared pytest fixtures for LiteChainIndexer.

Isolation guarantees:

1. **RPC throttle tracker** — the module-level ``RATE_TRACKER`` singleton
   reads ``RPC_THROTTLE_FILE`` (default ``<DATA_DIR>/rpc_throttle.json``,
   which in production is shared with the live daemon). Tests that make
   real RPC calls (e.g. internal-transfer trace collection) would
   otherwise inherit the production backoff state — a persisted 429
   ``Retry-After`` for a real endpoint would make the suite sleep for
   minutes. Every test gets a fresh tracker backed by a temp file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lite_indexer.rpc.throttle import RPCRateTracker


@pytest.fixture(autouse=True)
def _isolate_rpc_throttle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the shared RPC throttle tracker at a temp file per test.

    Replaces the module-level singleton with a fresh tracker whose state
    lives under the test's tmp dir. Applied to every test (autouse) so no
    test can sleep on a backoff window recorded by another test or by the
    production daemon.
    """
    tracker = RPCRateTracker(path=tmp_path / "rpc_throttle.json")
    monkeypatch.setattr("lite_indexer.rpc.throttle.RATE_TRACKER", tracker)
