"""
Smoke / integration tests for LiteChainIndexer.

These verify the indexer can actually connect to its two external
dependencies — ClickHouse (event storage) and an RPC endpoint
(blockchain data). Both are skipped by default unless the relevant
environment variables are set, keeping CI runs hermetic and offline-safe.

Usage:
    CH_HOST=127.0.0.1 pytest tests/test_smoke.py -v
    RPC_URL=https://eth.drpc.org pytest tests/test_smoke.py -v
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.smoke


def _clickhouse_configured() -> bool:
    """True when ClickHouse connection env vars are present."""
    return bool(os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST"))


def _rpc_configured() -> bool:
    """True when an RPC URL env var is present."""
    return bool(os.getenv("RPC_URL"))


@pytest.mark.skipif(
    not _clickhouse_configured(),
    reason="CH_HOST not set — skipping ClickHouse smoke test",
)
def test_clickhouse_connect() -> None:
    """Verify ClickHouse connection works (skip if not configured).

    Uses the same lazy ``ClickHouseWriter._connect()`` path the indexer
    uses on first write, so this exercises the real connection handling
    (host/port/user resolution, ``SELECT 1`` ping) rather than a fake.
    """
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    writer = ClickHouseWriter()
    client = writer._connect()
    assert client is not None, "ClickHouseWriter._connect() returned None (is CH_HOST reachable?)"
    assert client.execute("SELECT 1") == [(1,)]


@pytest.mark.asyncio
@pytest.mark.skipif(
    not _rpc_configured(),
    reason="RPC_URL not set — skipping RPC smoke test",
)
async def test_rpc_pool_health() -> None:
    """Verify at least one RPC endpoint responds (skip if offline).

    Probes the explicit ``RPC_URL`` first, then falls back to the
    configured public ethereum endpoints (placeholder URLs like
    ``${ALCHEMY_KEY}`` are skipped — they only work once the key is set).
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.rpc.client import RpcClient

    client = RpcClient()
    candidates: list[str] = []
    if _rpc_configured():
        candidates.append(os.environ["RPC_URL"])
    for ep in GET_LOGS_ENDPOINTS.get("ethereum", []):
        if "${" not in ep:
            candidates.append(ep)

    assert candidates, "no RPC endpoints available to probe"

    for ep in candidates:
        block = await client.eth_block_number(ep)
        if block is not None and block > 0:
            return

    pytest.fail(f"no RPC endpoint responded (probed {len(candidates)})")
