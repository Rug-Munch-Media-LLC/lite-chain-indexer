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

import os
from pathlib import Path

import pytest

# ── Live-infra isolation: NATS ──────────────────────────────────────────────
# NATS_ENABLED is captured at IMPORT time (lite_indexer/config.py) and a real
# NATS server listens on this host (127.0.0.1:4222). Without forcing it off,
# any test that constructs LiteIndexer and drives sync publishes REAL
# messages into local infra (found 2026-08-23 via pending-task spam from
# nats-py client loops). This line MUST stay above every lite_indexer import.
os.environ["NATS_ENABLED"] = "0"

# NODEREAL_KEY is REQUIRED at import time (lite_indexer/chains/endpoints.py
# raises a startup error when it is unset — the NodeReal BSC endpoint URL is
# built from it and must never silently fall out of the pool). Provide a
# stable test value so every module import below resolves it. This line MUST
# stay above every lite_indexer import (same as NATS_ENABLED above).
os.environ["NODEREAL_KEY"] = "test-key-123"

from lite_indexer.rpc.throttle import RPCRateTracker  # noqa: E402


@pytest.fixture(autouse=True)
def _no_live_nats(monkeypatch: pytest.MonkeyPatch) -> None:
    """Belt-and-braces: force the already-imported flag off for every test.

    Covers the case where some earlier importer captured NATS_ENABLED=1
    before conftest's env override could apply.
    """
    monkeypatch.setattr("lite_indexer.indexer.NATS_ENABLED", False)


@pytest.fixture(autouse=True)
def _no_live_clickhouse(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    """Default-isolate every test from the real ClickHouse on this host.

    ``ClickHouseWriter._connect()`` falls back to 127.0.0.1:9000 when no CH
    env vars are set — and a production-local ClickHouse actually listens
    there, so "no client" tests silently connected to it and even ran DDL
    (found 2026-08-23 in test_write_wallet_features_no_client). Pointing the
    port at 1 makes TCP refuse instantly → ``_connect()`` returns None.

    Tests that need a writer mock their client explicitly; smoke-marked
    tests (intentional live-service integration) are exempt.
    """
    if request.node.get_closest_marker("smoke"):
        return
    monkeypatch.setenv("CH_PORT", "1")
    monkeypatch.setenv("CLICKHOUSE_PORT", "1")


@pytest.fixture(autouse=True)
def _isolate_rpc_throttle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Point the shared RPC throttle tracker at a temp file per test.

    Replaces the module-level singleton with a fresh tracker whose state
    lives under the test's tmp dir. Applied to every test (autouse) so no
    test can sleep on a backoff window recorded by another test or by the
    production daemon.

    Both the ``lite_indexer.rpc.throttle`` attribute and the
    ``lite_indexer.rpc`` package re-export are patched: code that reaches
    the tracker through either path must see the isolated instance, never
    the production singleton (which loads the live backoff state from
    ``RPC_THROTTLE_FILE``).
    """
    tracker = RPCRateTracker(path=tmp_path / "rpc_throttle.json")
    monkeypatch.setattr("lite_indexer.rpc.throttle.RATE_TRACKER", tracker)
    monkeypatch.setattr("lite_indexer.rpc.RATE_TRACKER", tracker)
