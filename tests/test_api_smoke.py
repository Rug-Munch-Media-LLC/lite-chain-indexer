"""Smoke tests for the Chain Query API against a live ClickHouse.

Skipped by default unless ``CH_API_USER`` / ``CH_API_PASSWORD`` are set
(see the API service env). These verify the real queries execute against
the warehouse — run them once after deploying:

    CH_API_USER=chain_api CH_API_PASSWORD=... pytest tests/test_api_smoke.py -v
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.smoke


def _configured() -> bool:
    return bool(os.getenv("CH_API_PASSWORD") or os.getenv("CH_PASSWORD"))


@pytest.mark.skipif(not _configured(), reason="CH API credentials not set — skipping live smoke test")
def test_live_stats() -> None:
    from api.db import query

    row = query("SELECT count() AS c FROM wallet_memory.evm_transfers")[0]
    assert int(row["c"]) > 0


@pytest.mark.skipif(not _configured(), reason="CH API credentials not set — skipping live smoke test")
def test_live_ping() -> None:
    from api.db import ping

    ok, version = ping()
    assert ok
    assert version
