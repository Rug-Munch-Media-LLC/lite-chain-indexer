"""
Critical-path tests for LiteChainIndexer.

These cover failure and edge-case behavior that the happy-path suite
does not exercise:

  1. State must NOT advance when RPC endpoints fail
  2. Deployer events are still captured when DEX indexing is enabled
  3. The reorg head-start flag resets after a successful sync
  4. Endpoints swapped in during verification are actually used for fetching
  5. TronGrid event queries use min/max_block_timestamp instead of block numbers
"""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.asyncio
async def test_state_skips_on_failure(tmp_path, monkeypatch):
    """When every RPC endpoint fails, the sync must NOT advance state."""
    from lite_indexer.chains import GET_LOGS_ENDPOINTS
    from lite_indexer.indexer import LiteIndexer
    from lite_indexer.rpc.verify import EndpointVerifyResult

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    indexer = LiteIndexer(chain="ethereum", data_dir=tmp_path)
    indexer.state.save(42, 0)  # seed: last indexed block = 42
    assert indexer.state.load()["last_block"] == 42

    # All endpoints fail: block number fetch returns None
    indexer.strategy.fetch_block_number = AsyncMock(return_value=None)
    indexer.rpc.eth_block_number = AsyncMock(return_value=None)

    # Endpoint verification succeeds but finds nothing (does not abort sync)
    original = list(GET_LOGS_ENDPOINTS["ethereum"])
    result = EndpointVerifyResult(
        chain="ethereum",
        original_endpoints=original,
        verified_endpoints=[],
    )
    monkeypatch.setattr("lite_indexer.indexer.verify_chain_endpoints", AsyncMock(return_value=result))
    monkeypatch.setattr("lite_indexer.indexer.check_disk_cap", lambda *a, **k: True)
    monkeypatch.setattr("lite_indexer.indexer.check_disk_space", lambda: True)

    summary = await indexer.sync_loop()

    assert summary["status"] == "no_rpc_available"
    # State must NOT have advanced past block 42
    assert indexer.state.load()["last_block"] == 42


@pytest.mark.asyncio
async def test_deployers_with_dex(tmp_path, monkeypatch):
    """Deployer events must still be captured when DEX indexing is enabled."""
    from lite_indexer.indexer import LiteIndexer

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    indexer = LiteIndexer(
        chain="ethereum",
        from_block=100,
        to_block=200,
        data_dir=tmp_path,
        ndjson_enabled=True,
    )
    indexer.dex_enabled = True
    indexer.deployers_enabled = True
    # This test exercises the DEX/deployer flow, not trace collection —
    # disable traces so the test does not make real (rate-limited) trace
    # RPC calls to live endpoints.
    indexer.traces_enabled = False

    strategy = indexer.strategy
    strategy.fetch_block_number = AsyncMock(return_value=500)
    strategy.fetch_transfers = AsyncMock(return_value=[])
    strategy.fetch_swap_events = AsyncMock(return_value=[])

    deployer_event = {"blockNumber": 150, "factory": "0xfactory", "type": "deployer"}

    async def fake_fetch_pair_created(from_block: int, to_block: int) -> None:
        strategy._last_pair_events = [deployer_event]

    strategy._fetch_pair_created_events = AsyncMock(side_effect=fake_fetch_pair_created)

    async def fake_enrich(events):
        return [dict(e, blockTimestamp="2026-01-01T00:00:00Z") for e in events]

    indexer._enrich_timestamps = AsyncMock(side_effect=fake_enrich)

    # Avoid real ClickHouse / NDJSON writes
    ch = Mock()
    ch.write_deployer_events = AsyncMock(return_value=1)
    ch.write_dex_events.return_value = 0
    ch.client.return_value = None  # ClickHouse unavailable → reorg staging skipped
    indexer._ch_writer = ch
    # The 2026-08-04 CH health gate would pause the sync when the fake
    # writer reports client=None. This test exercises the DEX/deployer
    # flow, so force the gate open (reorg staging is still skipped via
    # client=None above).
    indexer._ch_health = type("_ChHealthStub", (), {"ok": lambda self: True})()
    indexer._ndjson_writer.write_events = Mock(return_value=0)
    indexer._ndjson_writer.write_dex_events = Mock(return_value=0)
    # write_clickhouse is a sync method (called without await) → plain Mock
    strategy.write_clickhouse = Mock(return_value=0)

    summary = await indexer._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic())

    assert summary["deployer_events_written"] == 1
    ch.write_deployer_events.assert_awaited_once()


@pytest.mark.asyncio
async def test_reorg_flag_resets(tmp_path, monkeypatch):
    """After a reorg triggers head-start recovery, a successful sync clears it."""
    from lite_indexer.indexer import LiteIndexer

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    indexer = LiteIndexer(
        chain="ethereum",
        from_block=100,
        to_block=200,
        data_dir=tmp_path,
        ndjson_enabled=True,
    )
    strategy = indexer.strategy
    strategy.fetch_block_number = AsyncMock(return_value=500)

    # This test exercises the reorg flag reset, not trace collection —
    # disable traces so the test does not make real (rate-limited) trace
    # RPC calls to live endpoints.
    indexer.traces_enabled = False

    # The 2026-08-04 CH health gate would pause the sync when the fake
    # writer reports client=None. This test exercises the reorg flag
    # reset flow, so force the gate open for both cycles (reorg staging
    # is still skipped via client=None below).
    ch = Mock()
    ch.write_dex_events.return_value = 0
    ch.write_wallet_features = AsyncMock(return_value={})
    ch.client.return_value = None  # ClickHouse unavailable → reorg staging skipped
    indexer._ch_writer = ch
    indexer._ch_health = type("_ChHealthStub", (), {"ok": lambda self: True})()

    # Simulate block reversal: transfers come back as the reorg sentinel
    strategy.fetch_transfers = AsyncMock(return_value=["__CHAIN_REORG__"])
    summary1 = await indexer._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic())
    assert summary1["status"] == "reorg_detected"
    assert indexer.start_from_head is True

    # Next cycle succeeds → the head-start flag must clear
    strategy.fetch_transfers = AsyncMock(return_value=[{"blockNumber": 150}])

    async def fake_enrich(events):
        return [dict(e, blockTimestamp="2026-01-01T00:00:00Z") for e in events]

    indexer._enrich_timestamps = AsyncMock(side_effect=fake_enrich)
    indexer._ndjson_writer.write_events = Mock(return_value=100)
    indexer._ndjson_writer.write_dex_events = Mock(return_value=0)
    # write_clickhouse is a sync method (called without await) → plain Mock
    strategy.write_clickhouse = Mock(return_value=1)

    summary2 = await indexer._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic())
    assert summary2["status"] == "ok"
    assert indexer.start_from_head is False


@pytest.mark.asyncio
async def test_verified_endpoints_applied(tmp_path, monkeypatch):
    """The endpoints swapped in during verification are used for fetching."""
    from lite_indexer.indexer import LiteIndexer
    from lite_indexer.rpc.verify import EndpointVerifyResult

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    import lite_indexer.chains as chains_mod

    original_endpoints = list(chains_mod.GET_LOGS_ENDPOINTS["ethereum"])
    verified = ["https://verified-a.example", "https://verified-b.example"]

    indexer = LiteIndexer(chain="ethereum", from_block=100, to_block=200, data_dir=tmp_path)

    # Capture the endpoint actually used by the strategy during the sync
    used_endpoints: list[str] = []

    async def fake_eth_block_number(endpoint: str) -> int:
        used_endpoints.append(endpoint)
        return 1000

    indexer.rpc.eth_block_number = fake_eth_block_number

    result = EndpointVerifyResult(
        chain="ethereum",
        original_endpoints=original_endpoints,
        verified_endpoints=verified,
    )
    monkeypatch.setattr("lite_indexer.indexer.verify_chain_endpoints", AsyncMock(return_value=result))
    monkeypatch.setattr("lite_indexer.indexer.check_disk_cap", lambda *a, **k: True)
    monkeypatch.setattr("lite_indexer.indexer.check_disk_space", lambda: True)

    indexer.strategy.fetch_transfers = AsyncMock(return_value=[])

    await indexer.sync_loop()

    # The sync fetched via one of the verified (swapped) endpoints
    assert used_endpoints, "no RPC fetch occurred during sync"
    assert all(ep in verified for ep in used_endpoints), f"fetch used unverified endpoints: {used_endpoints}"

    # The original configuration is restored after the cycle
    assert chains_mod.GET_LOGS_ENDPOINTS["ethereum"] == original_endpoints


@pytest.mark.asyncio
async def test_tron_uses_timestamp_params():
    """TronGrid event queries must use min/max_block_timestamp, not block numbers."""
    from lite_indexer.strategies.tron import TronStrategy

    strategy = TronStrategy(
        chain="tron",
        tokens=[{"address": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "symbol": "USDT"}],
    )

    # Block numbers 100→ts 1_700_000_000_000, 200→ts 1_700_000_300_000
    strategy._block_to_timestamp_ms = AsyncMock(
        side_effect=lambda bn: {100: 1_700_000_000_000, 200: 1_700_000_300_000}[bn]
    )

    urls: list[str] = []

    class FakeResponse:
        status_code = 200

        def json(self) -> dict:
            return {"data": [], "meta": {}}

    class FakeClient:
        is_closed = False

        async def get(self, url: str, **kwargs):
            # Mimic httpx: merge `params` kwargs into the URL string so
            # every recorded URL reflects the full query, including the
            # min/max_block_timestamp params passed via tron_get().
            params = kwargs.get("params") or {}
            if params:
                query = "&".join(f"{k}={v}" for k, v in params.items())
                url = f"{url}?{query}" if "?" not in url else f"{url}&{query}"
            urls.append(url)
            return FakeResponse()

    strategy._client = FakeClient()

    events = await strategy.fetch_transfers(100, 200)

    assert events == []
    assert urls, "expected at least one TronGrid query"
    assert all("min_block_timestamp=1700000000000" in u for u in urls)
    assert all("max_block_timestamp=1700000300000" in u for u in urls)
    assert not any("from_block_number=" in u for u in urls)
    assert not any("to_block_number=" in u for u in urls)
