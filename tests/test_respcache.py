"""Tests for the persistent reorg-aware RPC response cache (L1 + L2 SQLite).

Contracts:

* L1 hit (in-process).
* L2 hit after L1 clear + across a dropped connection (persistence).
* TTL expiry on the L2 row.
* Eviction at the L2 max-entries cap (+ ``rpc_cache_evictions``).
* ``invalidate_range`` purges overlapping range/block entries (reorg).
* Volatile/tip reads are never cached (no hit/miss metric).
* A broken L2 database degrades to a transparent miss (reads still succeed).
* ``RpcClient.get_logs`` still returns live data when the L2 store raises.

Each test gets a fresh temp SQLite DB via the autouse conftest fixture.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from lite_indexer import metrics
from lite_indexer.rpc import cache_db, respcache


@pytest.fixture(autouse=True)
def _reset_metrics():
    metrics.reset()
    yield
    metrics.reset()


def _log_filter(f: int, t: int, tag: str = "aa") -> dict[str, Any]:
    return {"fromBlock": hex(f), "toBlock": hex(t), "topics": ["0x" + tag * 32]}


def _put_logs(f: int, t: int, tag: str = "aa") -> bool:
    return respcache.put(
        "eth_getLogs",
        [_log_filter(f, t, tag)],
        {"jsonrpc": "2.0", "id": 1, "result": [{"blockNumber": hex(f)}]},
        chain="ethereum",
        from_block=f,
        to_block=t,
        tip=1000,
    )


class TestL1:
    def test_l1_hit(self):
        assert respcache.put("eth_chainId", None, {"result": "0x1"}, chain="ethereum") is True
        assert respcache.get("eth_chainId", None) == {"result": "0x1"}
        assert metrics.get("rpc_cache_hits") == 1.0

    def test_empty_not_cached_for_identity(self):
        assert respcache.put("eth_chainId", None, {"result": None}, chain="ethereum") is False


class TestL2:
    def test_l2_hit_after_l1_clear(self):
        _put_logs(1, 10)
        respcache.clear()  # L1 only
        got = respcache.get("eth_getLogs", [_log_filter(1, 10)], chain="ethereum", from_block=1, to_block=10, tip=1000)
        assert got == {"jsonrpc": "2.0", "id": 1, "result": [{"blockNumber": "0x1"}]}
        assert respcache.stats().get("l2_entries", 0) >= 1

    def test_l2_survives_connection_drop(self):
        _put_logs(1, 10)
        respcache.clear()
        cache_db.reset_db()  # drop the singleton; same DB path is rebuilt
        got = respcache.get("eth_getLogs", [_log_filter(1, 10)], chain="ethereum", from_block=1, to_block=10, tip=1000)
        assert got is not None

    def test_ttl_expiry(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(respcache, "RPC_CACHE_TTL_S", 0.05)
        assert _put_logs(1, 10)
        respcache.clear()
        time.sleep(0.08)
        assert (
            respcache.get("eth_getLogs", [_log_filter(1, 10)], chain="ethereum", from_block=1, to_block=10, tip=1000)
            is None
        )

    def test_eviction_at_max_entries(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(cache_db, "RPC_CACHE_MAX_ENTRIES", 2)
        respcache.reset()  # rebuild L2 with the small cap
        for i in range(3):
            respcache.put("eth_chainId", [i], {"result": "0x1"}, chain="ethereum")
        assert respcache.stats().get("l2_entries") == 2
        assert metrics.get("rpc_cache_evictions") >= 1.0


class TestReorgInvalidation:
    def test_invalidate_range_purges_overlapping(self):
        assert _put_logs(1, 10, tag="aa")
        assert _put_logs(11, 20, tag="bb")
        assert _put_logs(100, 110, tag="cc")
        deleted = respcache.invalidate_range("ethereum", 5, 15)
        assert deleted == 2  # ranges [1,10] and [11,20] overlap

        # L1 was cleared wholesale; the untouched range still resolves via L2.
        assert (
            respcache.get(
                "eth_getLogs", [_log_filter(100, 110, "cc")], chain="ethereum", from_block=100, to_block=110, tip=1000
            )
            is not None
        )
        assert (
            respcache.get(
                "eth_getLogs", [_log_filter(1, 10, "aa")], chain="ethereum", from_block=1, to_block=10, tip=1000
            )
            is None
        )

    def test_invalidate_range_covers_block_number_entries(self):
        params = [hex(500), False]
        assert respcache.put(
            "eth_getBlockByNumber",
            params,
            {"result": {"number": hex(500)}},
            chain="ethereum",
            block_number=500,
            tip=1000,
        )
        assert respcache.invalidate_range("ethereum", 499, 501) == 1


class TestVolatileNotCached:
    def test_tip_reads_never_cached(self):
        assert respcache.put("eth_blockNumber", [], {"result": "0x10"}, chain="ethereum", tip=1000) is False
        assert respcache.get("eth_blockNumber", [], chain="ethereum", tip=1000) is None
        assert (
            respcache.put("eth_getBalance", ["0xabc", "latest"], {"result": "0x1"}, chain="ethereum", tip=1000) is False
        )
        # No lookup attempted for volatile → no hit/miss counted.
        assert metrics.get("rpc_cache_hits") == 0.0
        assert metrics.get("rpc_cache_misses") == 0.0

    def test_near_tip_block_not_cached(self):
        params = [hex(999), False]
        assert (
            respcache.put(
                "eth_getBlockByNumber",
                params,
                {"result": {"number": hex(999)}},
                chain="ethereum",
                block_number=999,
                tip=1000,
            )
            is False
        )

    def test_confirmed_block_is_cached(self):
        params = [hex(100), False]
        assert (
            respcache.put(
                "eth_getBlockByNumber",
                params,
                {"result": {"number": hex(100)}},
                chain="ethereum",
                block_number=100,
                tip=1000,
            )
            is True
        )


class TestFailureSafe:
    def test_db_error_is_transparent_miss(self, monkeypatch: pytest.MonkeyPatch):
        def _boom() -> Any:
            raise RuntimeError("db down")

        monkeypatch.setattr(respcache.cache_db, "get_db", _boom)
        assert respcache.get("eth_chainId", None) is None
        # L1 write still succeeds; the L2 failure is swallowed.
        assert respcache.put("eth_chainId", None, {"result": "0x1"}, chain="ethereum") is True

    @pytest.mark.asyncio
    async def test_get_logs_survives_broken_db(self, monkeypatch: pytest.MonkeyPatch):
        import lite_indexer.rpc.client as rc

        def _boom() -> Any:
            raise RuntimeError("db down")

        monkeypatch.setattr(respcache.cache_db, "get_db", _boom)

        class _Resp:
            status_code = 200
            headers: dict[str, str] = {}
            content = b'{"result": [{"blockNumber": "0x5"}]}'
            text = content.decode()

            def json(self):
                return {"result": [{"blockNumber": "0x5"}]}

            def raise_for_status(self) -> None:
                return None

        class _Http:
            async def post(self, url: str, **kw: Any):
                return _Resp()

        http = _Http()

        async def _noop(*a: Any, **k: Any) -> None:
            return None

        async def _get_client() -> _Http:
            return http

        monkeypatch.setattr(rc, "apply_provider_rate_limit", _noop)
        monkeypatch.setattr(rc, "provider_throttle_before", _noop)
        monkeypatch.setattr(rc, "record_provider_response", lambda *a, **k: None)
        monkeypatch.setattr(rc, "_safe_record", lambda *a, **k: None)
        monkeypatch.setattr(rc, "_defer_reason_for", lambda *a, **k: None)
        monkeypatch.setattr("lite_indexer.rpc.throttle.throttle_before", _noop)
        monkeypatch.setattr("lite_indexer.rpc.throttle.record_rpc_response", lambda *a, **k: None)
        monkeypatch.setattr("lite_indexer.rpc.consensus.should_sample", lambda: False)

        client = rc.RpcClient()
        client._get_client = _get_client  # noqa: SLF001 — test seam
        logs = await client.get_logs("https://archive.example", 1, 10, "0x" + "ab" * 32, chain="ethereum", tip=1000)
        assert logs == [{"blockNumber": "0x5"}]
