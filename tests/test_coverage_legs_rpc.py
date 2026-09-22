"""Coverage tests for ``lite_indexer.query.legs_rpc`` cache-key behaviour.

Hermetic — no network, no RPC. Pins the on-demand RPC leg's cache
contract: a cache hit short-circuits the fetcher, a miss fetches and
writes through, and an RPC failure degrades to an empty list without
poisoning the cache.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.query.leg_request import LegRequest
from lite_indexer.query.legs_rpc import rpc_route


class _FakeCache:
    """Minimal QueryCache stand-in recording get/set calls."""

    def __init__(self, initial: dict[tuple[Any, ...], Any] | None = None) -> None:
        self.data: dict[tuple[Any, ...], Any] = dict(initial or {})
        self.gets: list[tuple[Any, ...]] = []
        self.sets: list[tuple[Any, ...]] = []

    def get(self, key: tuple[Any, ...]) -> Any | None:
        self.gets.append(key)
        return self.data.get(key)

    def set(self, key: tuple[Any, ...], value: Any) -> None:
        self.sets.append(key)
        self.data[key] = value


class _FakeRpc:
    """RPCFetcher stand-in returning scripted rows or raising."""

    def __init__(
        self,
        transfer: dict[str, Any] | None = None,
        address_rows: list[dict[str, Any]] | None = None,
        deployer: dict[str, Any] | None = None,
        *,
        fail: bool = False,
    ) -> None:
        self.transfer = transfer
        self.address_rows = address_rows or []
        self.deployer = deployer
        self.fail = fail
        self.calls: list[str] = []

    async def fetch_transfer(self, chain: str, tx_hash: str) -> dict[str, Any] | None:
        self.calls.append("transfer")
        if self.fail:
            raise RuntimeError("rpc down")
        return self.transfer

    async def fetch_address_transfers(
        self,
        chain: str,
        address: str,
        from_block: int | None,
        to_block: int | None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.calls.append("address")
        if self.fail:
            raise RuntimeError("rpc down")
        return list(self.address_rows)[:limit]

    async def fetch_deployer(self, chain: str, contract: str) -> dict[str, Any] | None:
        self.calls.append("deployer")
        if self.fail:
            raise RuntimeError("rpc down")
        return self.deployer


class _FakeRouter:
    def __init__(self, rpc: _FakeRpc, cache: _FakeCache) -> None:
        self.rpc = rpc
        self.cache = cache

    @staticmethod
    def _address_from_filters(filters: Any) -> str | None:
        if isinstance(filters, list):
            vals = {str(i.get("value", "")).lower() for i in filters if i.get("value")}
            if len(vals) == 1:
                return next(iter(vals))
        return None

    @staticmethod
    def _single_address(filters: Any) -> str | None:
        if isinstance(filters, dict) and len(filters) == 1:
            return str(next(iter(filters.values()))).lower()
        return None


def _router(rpc: _FakeRpc, cache: _FakeCache) -> _FakeRouter:
    return _FakeRouter(rpc, cache)


@pytest.mark.asyncio
async def test_cache_hit_short_circuits_fetcher() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", filters={"tx_hash": "0xabc"})
    cached_row = {"tx_hash": "0xabc", "block_number": 1}
    cache = _FakeCache({req.cache_key(): [cached_row]})
    rpc = _FakeRpc(transfer={"tx_hash": "0xabc"})
    rows = await rpc_route(_router(rpc, cache), req)
    assert rows == [cached_row]
    assert rpc.calls == []
    assert cache.sets == []


@pytest.mark.asyncio
async def test_cache_miss_fetches_and_writes_through() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", filters={"tx_hash": "0xabc"})
    row = {"tx_hash": "0xabc", "block_number": 1}
    cache = _FakeCache()
    rpc = _FakeRpc(transfer=row)
    rows = await rpc_route(_router(rpc, cache), req)
    assert rows == [row]
    assert rpc.calls == ["transfer"]
    assert cache.sets == [req.cache_key()]


@pytest.mark.asyncio
async def test_empty_result_is_not_cached() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", filters={"tx_hash": "0xmissing"})
    cache = _FakeCache()
    rpc = _FakeRpc(transfer=None)
    assert await rpc_route(_router(rpc, cache), req) == []
    assert cache.sets == []


@pytest.mark.asyncio
async def test_rpc_failure_degrades_to_empty_list() -> None:
    req = LegRequest(table="evm_transfers", chain="ethereum", filters={"tx_hash": "0xabc"})
    cache = _FakeCache()
    rpc = _FakeRpc(fail=True)
    assert await rpc_route(_router(rpc, cache), req) == []
    assert cache.sets == []


@pytest.mark.asyncio
async def test_address_query_uses_address_fetcher() -> None:
    req = LegRequest(
        table="evm_transfers",
        chain="ethereum",
        from_block=1,
        to_block=2,
        filters=[{"col": "from_address", "value": "0x1"}, {"col": "to_address", "value": "0x1"}],
    )
    rows = [{"tx_hash": "0x1"}]
    cache = _FakeCache()
    rpc = _FakeRpc(address_rows=rows)
    assert await rpc_route(_router(rpc, cache), req) == rows
    assert rpc.calls == ["address"]


@pytest.mark.asyncio
async def test_deployer_query_uses_deployer_fetcher() -> None:
    req = LegRequest(table="deployer_events", chain="ethereum", filters={"pair_address": "0xpair"})
    row = {"deployer": "0xdeployer"}
    cache = _FakeCache()
    rpc = _FakeRpc(deployer=row)
    assert await rpc_route(_router(rpc, cache), req) == [row]
    assert rpc.calls == ["deployer"]


@pytest.mark.asyncio
async def test_unknown_table_returns_empty_without_fetching() -> None:
    req = LegRequest(table="unknown_table", chain="ethereum")
    cache = _FakeCache()
    rpc = _FakeRpc()
    assert await rpc_route(_router(rpc, cache), req) == []
    assert rpc.calls == []
