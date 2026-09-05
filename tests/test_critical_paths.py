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

import asyncio
import time
from typing import Any
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
    monkeypatch.setattr("lite_indexer.indexer.check_disk_cap_async", AsyncMock(return_value=True))
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

    # ISOLATION (found 2026-08-23): a live NATS server runs on this host and
    # NATS_ENABLED defaults on — without this flag the suite publishes REAL
    # messages into local infra during tests. Same for NDJSON writer (None
    # unless enabled) and traces/CH below.
    indexer.nats_enabled = False

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
    monkeypatch.setattr("lite_indexer.indexer.check_disk_cap_async", AsyncMock(return_value=True))
    monkeypatch.setattr("lite_indexer.indexer.check_disk_space", lambda: True)

    indexer.strategy.fetch_transfers = AsyncMock(return_value=[])

    # Isolation seams (mirror test_reorg_flag_resets): without these the
    # sync makes REAL trace-RPC / ClickHouse / NDJSON calls and hangs the
    # suite (found 2026-08-23 — this test wedged pytest at ~9% three runs
    # in a row under load).
    indexer.traces_enabled = False
    ch = Mock()
    ch.write_dex_events.return_value = 0
    ch.write_wallet_features = AsyncMock(return_value={})
    ch.client.return_value = None
    indexer._ch_writer = ch
    indexer._ch_health = type("_ChHealthStub", (), {"ok": lambda self: True})()
    indexer._enrich_timestamps = AsyncMock(side_effect=lambda events: events)
    if indexer._ndjson_writer is not None:
        indexer._ndjson_writer.write_events = Mock(return_value=0)

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


# ── Critical failure-mode tests (reorg / gap / reconciliation / failover) ──


class _FakeCH:
    """Minimal ClickHouse stand-in dispatching on the SQL shape."""

    def __init__(self, routes: dict[str, Any]) -> None:
        # routes: list of (predicate, value) evaluated in order
        self.routes = routes
        self.executed: list[str] = []

    def execute(self, sql: str, params: Any = None) -> Any:
        self.executed.append(sql)
        for predicate, value in self.routes:
            if predicate(sql):
                if isinstance(value, Exception):
                    raise value
                return value
        return []


def test_reorg_detection_evm():
    """EVM reorg staging: hash mismatch → reorg; match/no-checkpoint → clean."""
    from lite_indexer.strategies.evm.reorg import detect_reorg, reorg_depth

    assert reorg_depth("polygon") == 32  # fast chain gets deeper margin
    assert reorg_depth("unknown-chain") == 12

    stored = "0x" + "a" * 64
    current = "0x" + "b" * 64

    # Mismatch → reorg detected
    ch = _FakeCH([(lambda sql: "SELECT block_hash" in sql, [[stored]])])
    assert asyncio.run(detect_reorg("ethereum", 1000, current, ch)) is True

    # Same hash → no reorg
    ch = _FakeCH([(lambda sql: "SELECT block_hash" in sql, [[stored]])])
    assert asyncio.run(detect_reorg("ethereum", 1000, stored, ch)) is False

    # No checkpoint yet → cannot tell (False, not a false positive)
    ch = _FakeCH([])
    assert asyncio.run(detect_reorg("ethereum", 1000, current, ch)) is False

    # Missing hash → skip
    assert asyncio.run(detect_reorg("ethereum", 1000, "", _FakeCH([]))) is False


def test_reorg_detection_solana(monkeypatch):
    """Solana reorg staging: blockhash mismatch at depth-32 slot → fork slot."""
    from lite_indexer.strategies.solana import reorg as sol_reorg

    stored = "0x" + "a" * 64
    current = "0x" + "b" * 64

    ch = _FakeCH([(lambda sql: "SELECT blockhash" in sql, [[stored]])])
    monkeypatch.setattr(sol_reorg, "_get_blockhash", AsyncMock(return_value=current))
    # current_slot=100 verifies slot 100-32=68
    fork = asyncio.run(sol_reorg.detect_reorg(Mock(), 100, ch))
    assert fork == 68

    # Matching blockhash → no reorg
    monkeypatch.setattr(sol_reorg, "_get_blockhash", AsyncMock(return_value=stored))
    assert asyncio.run(sol_reorg.detect_reorg(Mock(), 100, ch)) is None

    # No checkpoint for the verify slot → None
    ch_empty = _FakeCH([])
    monkeypatch.setattr(sol_reorg, "_get_blockhash", AsyncMock(return_value=current))
    assert asyncio.run(sol_reorg.detect_reorg(Mock(), 100, ch_empty)) is None


def test_gap_detection(monkeypatch):
    """Gap scan: missing sampled blocks collapse into (from, to) ranges."""
    from lite_indexer import recovery

    stride = 10
    monkeypatch.setattr(
        recovery,
        "query_chain_stats",
        lambda ch, chain, chain_type="evm": {
            "ok": True,
            "min_block": 0,
            "max_block": 100,
            "distinct_blocks": 9,
            "total_rows": 9,
        },
    )

    present = [b for b in range(0, 101, stride) if b not in (50, 60)]
    ch = _FakeCH([(lambda sql: "DISTINCT" in sql, [[b] for b in present])])
    ranges = recovery.find_missing_ranges(ch, "ethereum", stride=stride)
    # Sampled holes at 50 and 60 collapse into one span 50..69
    assert ranges == [(50, 69)]

    # Fully populated range → no gaps
    ch_full = _FakeCH([(lambda sql: "DISTINCT" in sql, [[b] for b in range(0, 101, stride)])])
    assert recovery.find_missing_ranges(ch_full, "ethereum", stride=stride) == []

    # CH stats unavailable → best-effort empty
    monkeypatch.setattr(
        recovery,
        "query_chain_stats",
        lambda ch, chain, chain_type="evm": {"ok": False, "error": "down"},
    )
    assert recovery.find_missing_ranges(_FakeCH([]), "ethereum", stride=stride) == []


def test_balance_reconciliation():
    """Balance reconciliation classifies computed vs on-chain deltas."""
    from lite_indexer.storage.reconciliation import _classify, verify_transfer_completeness

    # Unit: classification boundaries
    assert _classify(0, 100) == ("MATCH", 0.0)
    assert _classify(1, 200)[0] == "APPROXIMATE"  # 0.5% < 1%
    assert _classify(50, 100) == ("MISMATCH", 50.0)
    assert _classify(10, 0)[0] == "MISMATCH"  # zero on-chain → absolute mismatch

    wallet = "0x" + "1" * 40
    # incoming=150, outgoing=50 → computed 100; on-chain 100 → MATCH
    ch = _FakeCH(
        [
            (lambda sql: "to_address" in sql, [[150]]),
            (lambda sql: "from_address" in sql, [[50]]),
        ]
    )

    class FakeRpc:
        async def call(self, endpoint: str, method: str, params: list[Any]) -> dict[str, Any]:
            assert method == "eth_getBalance"
            return {"result": hex(100)}

    results = asyncio.run(verify_transfer_completeness("ethereum", [wallet], FakeRpc(), ch))
    assert results[wallet]["status"] == "MATCH"
    assert results[wallet]["computed_balance"] == 100
    assert results[wallet]["delta"] == 0

    # RPC failure for the wallet → wallet skipped, not an exception
    class DeadRpc:
        async def call(self, *args: Any, **kwargs: Any) -> None:
            raise ConnectionError("rpc down")

    results = asyncio.run(verify_transfer_completeness("ethereum", [wallet], DeadRpc(), ch))
    assert results == {}

    # Empty sample → no-op
    assert asyncio.run(verify_transfer_completeness("ethereum", [], FakeRpc(), ch)) == {}


def test_failover_to_backup_rpc(monkeypatch):
    """Hedged RPC: slow/failing primary → backup endpoint answer wins."""
    from lite_indexer.rpc import hedging

    monkeypatch.setattr(hedging, "HEDGE_ENABLED", True)
    monkeypatch.setattr(hedging, "HEDGE_DELAY_S", 0.01)
    endpoints = ["https://primary.example", "https://backup.example"]
    monkeypatch.setattr(hedging, "get_best_endpoint", lambda eps: eps[0])

    calls: list[str] = []

    async def factory(endpoint: str) -> dict[str, Any] | None:
        calls.append(endpoint)
        if endpoint == endpoints[0]:
            await asyncio.sleep(5)  # primary hangs past the hedge delay
            return None
        return {"result": "from-backup"}

    result = asyncio.run(hedging.hedged_rpc(factory, endpoints))
    assert result == {"result": "from-backup"}
    assert endpoints[1] in calls, "hedge leg never fired"

    # Every endpoint fails → None (same failure shape as the client)
    async def always_none(endpoint: str) -> None:
        return None

    assert asyncio.run(hedging.hedged_rpc(always_none, endpoints)) is None

    # Empty pool → None
    assert asyncio.run(hedging.hedged_rpc(factory, [])) is None


def test_checkpoint_recovery(tmp_path, monkeypatch):
    """recover_chain_state resets state to the CH max and reports gaps."""
    from lite_indexer.recovery import recover_chain_state
    from lite_indexer.state import StateHighWater, StateManager

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    # Stats query returns min=0 max=2000; gap scan (stride 1000) finds the
    # sampled block at 1000 missing → hole spans 1000..1999
    ch = _FakeCH(
        [
            (lambda sql: "min(" in sql, [[0, 2000, 2, 5000]]),
            (lambda sql: "DISTINCT" in sql, [[0], [2000]]),
        ]
    )

    # Seed a stale (regressed) state file
    StateManager("ethereum").save(42, 0, source="seed")

    report = recover_chain_state(ch, "ethereum", "evm", state_dir=tmp_path / "state")
    assert report["ok"] is True
    assert report["previous_state"] == 42
    assert report["recovered_to"] == 2000
    assert report["missing_ranges"] == [(1000, 1999)]
    # State file + high-water mark now point at the CH max
    assert StateManager("ethereum").load()["last_block"] == 2000
    assert StateHighWater("ethereum").load() == 2000

    # backfill=True rewinds below the first gap so the next sync re-fetches it
    report = recover_chain_state(ch, "ethereum", "evm", state_dir=tmp_path / "state", backfill=True)
    assert report["backfill_from"] == 1000
    assert StateManager("ethereum").load()["last_block"] == 999

    # CH unavailable → clean failure, no state write
    ch_dead = _FakeCH([(lambda sql: True, RuntimeError("clickhouse down"))])
    report = recover_chain_state(ch_dead, "ethereum", "evm", state_dir=tmp_path / "state")
    assert report["ok"] is False
    assert report["error"]


def test_chain_health_check(monkeypatch):
    """check_chain_health reports ok/error per chain without raising."""
    import httpx

    from lite_indexer import health as health_mod

    class OkStrategy:
        async def fetch_block_number(self) -> int:
            return 12345

    class DeadStrategy:
        async def fetch_block_number(self) -> None:
            raise httpx.ConnectError("connection refused")

    def fake_create_strategy(chain: str, **kwargs: Any) -> Any:
        return OkStrategy() if chain == "ethereum" else DeadStrategy()

    monkeypatch.setattr(health_mod, "create_strategy", fake_create_strategy)

    results = asyncio.run(health_mod.check_chain_health(["ethereum", "solana"], timeout=2))
    assert results["ethereum"] == {
        "status": "ok",
        "block": 12345,
        "chain_type": "evm",
        "label": "Ethereum",
    }
    assert results["solana"]["status"] == "error"
    assert "connection refused" in results["solana"]["error"]

    # fetch returning None is an error, not a crash
    class NoneStrategy:
        async def fetch_block_number(self) -> None:
            return None

    monkeypatch.setattr(health_mod, "create_strategy", lambda chain, **kw: NoneStrategy())
    results = asyncio.run(health_mod.check_chain_health(["ethereum"], timeout=2))
    assert results["ethereum"]["status"] == "error"
    assert "returned None" in results["ethereum"]["error"]


@pytest.mark.asyncio
async def test_concurrent_chain_sync(tmp_path, monkeypatch):
    """Two chains syncing concurrently keep independent state and summaries."""
    from lite_indexer.indexer import LiteIndexer

    monkeypatch.setattr("lite_indexer.state.STATE_DIR", tmp_path / "state")

    def make_indexer(chain: str, to_block: int) -> LiteIndexer:
        indexer = LiteIndexer(
            chain=chain,
            from_block=100,
            to_block=to_block,
            data_dir=tmp_path,
            ndjson_enabled=True,
        )
        indexer.traces_enabled = False
        indexer.nats_enabled = False
        strategy = indexer.strategy
        strategy.fetch_block_number = AsyncMock(return_value=to_block + 500)
        strategy.fetch_transfers = AsyncMock(return_value=[{"blockNumber": 150, "value": "1"}])
        strategy.fetch_swap_events = AsyncMock(return_value=[])
        strategy._fetch_pair_created_events = AsyncMock(return_value=None)
        strategy.write_clickhouse = Mock(return_value=1)

        async def fake_enrich(events: list[Any]) -> list[Any]:
            return [dict(e, blockTimestamp="2026-01-01T00:00:00Z") for e in events]

        indexer._enrich_timestamps = AsyncMock(side_effect=fake_enrich)
        ch = Mock()
        ch.write_dex_events.return_value = 0
        ch.write_wallet_features = AsyncMock(return_value={})
        ch.client.return_value = None
        indexer._ch_writer = ch
        indexer._ch_health = type("_ChHealthStub", (), {"ok": lambda self: True})()
        indexer._ndjson_writer.write_events = Mock(return_value=1)
        indexer._ndjson_writer.write_dex_events = Mock(return_value=0)
        return indexer

    eth = make_indexer("ethereum", to_block=200)
    poly = make_indexer("polygon", to_block=300)

    summaries = await asyncio.gather(
        eth._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic()),
        poly._sync_loop_inner({"status": "ok", "errors": []}, time.monotonic()),
    )

    assert [s["status"] for s in summaries] == ["ok", "ok"]
    # Each chain advanced its OWN state file to its own to_block — no
    # cross-contamination between concurrent cycles.
    assert eth.state.load()["last_block"] == 200
    assert poly.state.load()["last_block"] == 300
