"""
Tests for the on-demand historical RPC query system (lite_indexer.query).

Covers the "no-full-indexing-needed" architecture:

* :class:`lite_indexer.query.cache.QueryCache` — TTL expiry, eviction, stats.
* :class:`lite_indexer.query.router.QueryRouter` — routing priority
  (ClickHouse → B2 cold → RPC), hot/cold merge + dedupe, RPC fallback
  decisions, caching, graceful degradation, per-method helpers.
* :class:`lite_indexer.query.rpc_fetcher.RPCFetcher` — targeted calls
  (tx receipt → Transfer log, address-window getLogs with range clamp,
  deployer via creation-tx / trace_filter, block fetch), unknown-chain
  safety, and the consecutive-call rate-limit pause.
* The CLI ``query`` subcommand wiring.

No network is touched: ClickHouse is a fake client, the cold store is a
fake DuckDB/parquet stand-in, and the RPC layer is a fake RpcClient.
"""

from __future__ import annotations

import re
import time
from typing import Any, cast

import pytest

from lite_indexer.config import TRANSFER_EVENT_TOPIC
from lite_indexer.query import QueryCache, QueryRouter, RPCFetcher
from lite_indexer.rpc import RpcClient
from lite_indexer.storage.cold_store import ColdStore


@pytest.fixture(autouse=True)
def _no_network_legs() -> Any:
    """Fallback must reach the injected fakes: real network legs return [].

    The legs are class attributes bound at import time, so tests patch them
    on the router class (string targets) — no real HTTP in the suite.
    """
    from unittest.mock import patch

    async def _empty(*_args: Any, **_kw: Any) -> list[Any]:
        return []

    async def _empty_biglake(*_args: Any, **_kw: Any) -> tuple[list[Any], bool]:
        # _biglake_route returns (rows, truncated); the other legs return rows.
        return [], False

    with (
        patch("lite_indexer.query.router.QueryRouter._biglake_route", _empty_biglake),
        patch("lite_indexer.query.router.QueryRouter._envio_route", _empty),
        patch("lite_indexer.query.router.QueryRouter._indexed_route", _empty),
        patch("lite_indexer.query.router.QueryRouter._sqd_traces_route", _empty),
    ):
        yield


# ── Fakes ───────────────────────────────────────────────────────────────

_EVM_COLS = (
    "chain",
    "block_number",
    "tx_hash",
    "from_address",
    "to_address",
    "token_address",
    "value",
    "block_timestamp",
    "log_index",
    "ingested_at",
)


def _tr(
    chain: str = "ethereum",
    block: int = 100,
    tx: str = "0xtx1",
    log: int = 0,
    frm: str = "0xfrom",
    to: str = "0xto",
    tok: str = "0xtok",
    value: str = "1000",
    ts: str = "2026-01-01T00:00:00Z",
) -> dict[str, Any]:
    return {
        "chain": chain,
        "block_number": block,
        "tx_hash": tx,
        "from_address": frm,
        "to_address": to,
        "token_address": tok,
        "value": value,
        "block_timestamp": ts,
        "log_index": log,
        "ingested_at": "",
    }


def _deployer_row(
    chain: str = "ethereum",
    block: int = 100,
    tx: str = "0xdeploytx",
    deployer: str = "0xdeployer",
    pair: str = "0xpair",
    token: str = "0xtoken",
) -> dict[str, Any]:
    return {
        "deployer": deployer,
        "deployer_eoa": deployer,
        "token_address": token,
        "token0": token,
        "token1": "0xother",
        "pair_address": pair,
        "chain": chain,
        "block_number": block,
        "block_timestamp": "2026-01-01T00:00:00Z",
        "tx_hash": tx,
        "type": "v2_pool",
        "factory": "0xfactory",
        "ingested_at": "",
        "source": "lite_indexer",
    }


class FakeCH:
    """ClickHouse client stand-in returning scripted rows per table."""

    def __init__(self, by_table: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.by_table = by_table or {}
        self.executed: list[tuple[str, dict[str, Any] | None]] = []

    @staticmethod
    def _table_from_sql(sql: str) -> str:
        match = re.search(r"FROM\s+\w+\.(\w+)", sql)
        return match.group(1) if match else ""

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        with_column_types: bool = False,
        settings: dict[str, Any] | None = None,
    ) -> Any:
        self.executed.append((sql, params))
        table = self._table_from_sql(sql)
        rows = [r for r in self.by_table.get(table, []) if r.get("chain") == (params or {}).get("chain")]
        names = list(rows[0].keys()) if rows else list(_EVM_COLS)
        values = [tuple(r[c] for c in names) for r in rows]
        if with_column_types:
            return values, [(n, "String") for n in names]
        return values


class FakeCold:
    """ColdStore stand-in recording query parameters and returning rows."""

    def __init__(self, by_table: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.by_table = by_table or {}
        self.calls: list[dict[str, Any]] = []

    def query(
        self,
        table: str,
        chain: str = "",
        *,
        from_block: int | None = None,
        to_block: int | None = None,
        from_time: Any = None,
        to_time: Any = None,
        where: str = "",
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        self.calls.append(
            {
                "table": table,
                "chain": chain,
                "from_block": from_block,
                "to_block": to_block,
                "where": where,
                "limit": limit,
            }
        )
        return list(self.by_table.get(table, []))[:limit]


class FakeRPC:
    """RPCFetcher stand-in recording calls and returning scripted results."""

    def __init__(
        self,
        transfers: dict[str, dict[str, Any]] | None = None,
        address_rows: list[dict[str, Any]] | None = None,
        deployer: dict[str, Any] | None = None,
        blocks: dict[int, dict[str, Any]] | None = None,
        *,
        fail: bool = False,
    ) -> None:
        self.transfer_results = transfers or {}
        self.address_rows = address_rows or []
        self.deployer_result = deployer
        self.block_results = blocks or {}
        self.fail = fail
        self.transfer_calls = 0
        self.address_calls = 0
        self.deployer_calls = 0
        self.block_calls = 0
        self.last_address_args: tuple[Any, ...] = ()
        self.last_deployer_args: tuple[Any, ...] = ()

    async def fetch_transfer(self, chain: str, tx_hash: str) -> dict[str, Any] | None:
        self.transfer_calls += 1
        if self.fail:
            raise RuntimeError("rpc unavailable")
        return self.transfer_results.get(tx_hash)

    async def fetch_address_transfers(
        self,
        chain: str,
        address: str,
        from_block: int | None,
        to_block: int | None,
        *,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.address_calls += 1
        self.last_address_args = (chain, address, from_block, to_block, limit)
        if self.fail:
            raise RuntimeError("rpc unavailable")
        return list(self.address_rows)[:limit]

    async def fetch_deployer(
        self,
        chain: str,
        contract_address: str,
        creation_tx: str | None = None,
    ) -> dict[str, Any] | None:
        self.deployer_calls += 1
        self.last_deployer_args = (chain, contract_address, creation_tx)
        if self.fail:
            raise RuntimeError("rpc unavailable")
        return self.deployer_result

    async def fetch_block_info(self, chain: str, block: int) -> dict[str, Any] | None:
        self.block_calls += 1
        if self.fail:
            raise RuntimeError("rpc unavailable")
        return self.block_results.get(block)


def _make_router(
    ch: FakeCH | None = None,
    cold: FakeCold | None = None,
    rpc: FakeRPC | None = None,
    cache: QueryCache | None = None,
    **kwargs: Any,
) -> QueryRouter:
    return QueryRouter(
        ch_client=ch,
        cold_store=cast(ColdStore | None, cold),
        rpc_pool=cast(RPCFetcher | None, rpc or FakeRPC()),
        cache=cache,
        **kwargs,
    )


# ── QueryCache ──────────────────────────────────────────────────────────


def test_cache_set_get() -> None:
    cache = QueryCache(ttl=3600)
    cache.set(("ethereum", "transfer", "0xtx"), {"block_number": 1})
    assert cache.get(("ethereum", "transfer", "0xtx")) == {"block_number": 1}
    assert cache.get(("ethereum", "transfer", "0xmissing")) is None


def test_cache_ttl_expiry() -> None:
    cache = QueryCache(ttl=0.05)
    cache.set(("k",), "v")
    assert cache.get(("k",)) == "v"
    time.sleep(0.06)
    assert cache.get(("k",)) is None


def test_cache_eviction() -> None:
    cache = QueryCache(ttl=3600, max_entries=2)
    cache.set(("a",), 1)
    cache.set(("b",), 2)
    cache.set(("c",), 3)
    assert len(cache) <= 2
    # The oldest entry (a) was evicted.
    assert cache.get(("a",)) is None


def test_cache_stats() -> None:
    cache = QueryCache(ttl=3600)
    cache.set(("a",), 1)
    assert cache.get(("a",)) == 1
    cache.get(("missing",))
    stats = cache.stats()
    assert stats["hits"] == 1
    assert stats["misses"] == 1


# ── QueryRouter.get_transfer ────────────────────────────────────────────


async def test_get_transfer_ch_hit() -> None:
    ch = FakeCH({"evm_transfers": [_tr(tx="0xtx1")]})
    rpc = FakeRPC()
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc)
    row = await router.get_transfer("ethereum", "0xtx1")
    assert row is not None
    assert row["tx_hash"] == "0xtx1"
    assert rpc.transfer_calls == 0


async def test_get_transfer_cold_fallback() -> None:
    cold = FakeCold({"evm_transfers": [_tr(tx="0xtx2", block=50)]})
    rpc = FakeRPC()
    router = _make_router(ch=FakeCH(), cold=cold, rpc=rpc)
    row = await router.get_transfer("ethereum", "0xtx2")
    assert row is not None
    assert row["block_number"] == 50
    assert rpc.transfer_calls == 0


async def test_get_transfer_rpc_fallback_and_cache() -> None:
    rpc_row = _tr(tx="0xtx3", block=999, frm="0xrpc_from")
    rpc = FakeRPC(transfers={"0xtx3": rpc_row})
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    row = await router.get_transfer("ethereum", "0xtx3")
    assert row is not None
    assert row["from_address"] == "0xrpc_from"
    assert rpc.transfer_calls == 1
    # Second call is served from cache — no second RPC call.
    row2 = await router.get_transfer("ethereum", "0xtx3")
    assert row2 == row
    assert rpc.transfer_calls == 1


async def test_get_transfer_rpc_disabled() -> None:
    rpc = FakeRPC(transfers={"0xtx": _tr()})
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc, on_demand_enabled=False)
    assert await router.get_transfer("ethereum", "0xtx") is None
    assert rpc.transfer_calls == 0


async def test_get_transfer_empty_tx() -> None:
    router = _make_router(ch=FakeCH(), cold=FakeCold())
    assert await router.get_transfer("ethereum", "") is None


async def test_get_transfer_rpc_failure_graceful() -> None:
    rpc = FakeRPC(fail=True)
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    # RPC raising must not propagate — the router degrades to None.
    assert await router.get_transfer("ethereum", "0xtx") is None


# ── QueryRouter.get_transfers ───────────────────────────────────────────


async def test_get_transfers_merges_hot_cold_dedup() -> None:
    ch_row_a = _tr(block=200, tx="0xdup", frm="0xwallet", to="0xto", value="5")
    cold_row_a = _tr(block=200, tx="0xdup", frm="0xwallet", to="0xto", value="5")
    cold_row_b = _tr(block=100, tx="0xother", frm="0xwallet", to="0xto", value="7")
    ch = FakeCH({"evm_transfers": [ch_row_a]})
    cold = FakeCold({"evm_transfers": [cold_row_a, cold_row_b]})
    rpc = FakeRPC()
    router = _make_router(ch=ch, cold=cold, rpc=rpc)
    rows = await router.get_transfers("ethereum", "0xwallet")
    assert [r["tx_hash"] for r in rows] == ["0xdup", "0xother"]
    assert len(rows) == 2  # deduped — the duplicate hot/cold row appears once
    assert rpc.address_calls == 0  # indexed rows → no RPC


async def test_get_transfers_rpc_fallback_narrow_range() -> None:
    rpc_rows = [_tr(block=150, tx="0xr1", frm="0xwallet")]
    rpc = FakeRPC(address_rows=rpc_rows)
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    rows = await router.get_transfers("ethereum", "0xwallet", from_block=100, to_block=200)
    assert rows == rpc_rows
    assert rpc.address_calls == 1


async def test_get_transfers_wide_range_trusts_indexed() -> None:
    ch = FakeCH({"evm_transfers": [_tr(block=50_000, tx="0xw1", frm="0xwallet")]})
    rpc = FakeRPC(address_rows=[_tr(block=99_999, tx="0xrpc")])
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc)
    rows = await router.get_transfers("ethereum", "0xwallet", from_block=0, to_block=100_000)
    # Wide bounded range with indexed rows → indexed store wins, no RPC.
    assert [r["tx_hash"] for r in rows] == ["0xw1"]
    assert rpc.address_calls == 0


async def test_get_transfers_unbounded_empty_triggers_rpc() -> None:
    expected = _tr(block=900, tx="0xrpc", frm="0xwallet")
    rpc = FakeRPC(address_rows=[expected])
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    rows = await router.get_transfers("ethereum", "0xwallet")
    assert rows == [expected]
    assert rpc.address_calls == 1
    assert rpc.last_address_args[0] == "ethereum"
    assert rpc.last_address_args[1] == "0xwallet"


async def test_get_transfers_unbounded_with_rows_no_rpc() -> None:
    ch = FakeCH({"evm_transfers": [_tr(block=300, tx="0xidx", frm="0xwallet")]})
    rpc = FakeRPC(address_rows=[_tr(block=900, tx="0xrpc", frm="0xwallet")])
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc)
    rows = await router.get_transfers("ethereum", "0xwallet")
    assert [r["tx_hash"] for r in rows] == ["0xidx"]
    assert rpc.address_calls == 0


async def test_get_transfers_empty_address() -> None:
    router = _make_router(ch=FakeCH(), cold=FakeCold())
    assert await router.get_transfers("ethereum", "") == []


# ── QueryRouter.get_block ───────────────────────────────────────────────


async def test_get_block_always_rpc_cached() -> None:
    block = {"number": hex(100), "hash": "0xdead", "transactions": []}
    rpc = FakeRPC(blocks={100: block})
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    result = await router.get_block("ethereum", 100)
    assert result == block
    assert rpc.block_calls == 1
    # Cached — no second RPC call.
    await router.get_block("ethereum", 100)
    assert rpc.block_calls == 1


async def test_get_block_rpc_disabled() -> None:
    rpc = FakeRPC(blocks={100: {"number": "0x64"}})
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc, on_demand_enabled=False)
    assert await router.get_block("ethereum", 100) == {}
    assert rpc.block_calls == 0


# ── QueryRouter.get_deployer_info ───────────────────────────────────────


async def test_get_deployer_info_ch_hit() -> None:
    ch = FakeCH({"deployer_events": [_deployer_row(pair="0xpair")]})
    rpc = FakeRPC(deployer=_deployer_row(pair="0xpair"))
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc)
    info = await router.get_deployer_info("ethereum", "0xpair")
    assert info is not None
    assert info["deployer"] == "0xdeployer"
    assert rpc.deployer_calls == 0


async def test_get_deployer_info_rpc_with_creation_tx_hint() -> None:
    # Indexed row exists but the EOA was never resolved → the router passes
    # the row's tx_hash as the creation-tx hint to the RPC leg.
    unresolved = _deployer_row(pair="0xpair", tx="0xcreatetx", deployer="")
    unresolved["deployer_eoa"] = ""
    ch = FakeCH({"deployer_events": [unresolved]})
    rpc_row = _deployer_row(pair="0xpair", tx="0xcreatetx")
    rpc = FakeRPC(deployer=rpc_row)
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc)
    info = await router.get_deployer_info("ethereum", "0xpair")
    assert info is not None
    assert info["deployer"] == "0xdeployer"
    assert rpc.deployer_calls == 1
    assert rpc.last_deployer_args[2] == "0xcreatetx"  # creation_tx hint


async def test_get_deployer_info_rpc_fallback_no_index() -> None:
    rpc_row = _deployer_row(pair="0xpair")
    rpc = FakeRPC(deployer=rpc_row)
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc)
    info = await router.get_deployer_info("ethereum", "0xpair")
    assert info is not None
    assert info["deployer"] == "0xdeployer"
    assert rpc.deployer_calls == 1
    assert rpc.last_deployer_args[2] is None


async def test_get_deployer_info_rpc_disabled_keeps_indexed() -> None:
    unresolved = _deployer_row(pair="0xpair", deployer="")
    unresolved["deployer_eoa"] = ""
    ch = FakeCH({"deployer_events": [unresolved]})
    rpc = FakeRPC(deployer=_deployer_row(pair="0xpair"))
    router = _make_router(ch=ch, cold=FakeCold(), rpc=rpc, on_demand_enabled=False)
    info = await router.get_deployer_info("ethereum", "0xpair")
    assert info is not None
    assert rpc.deployer_calls == 0


# ── QueryRouter._route_query ────────────────────────────────────────────


async def test_route_query_unknown_table() -> None:
    router = _make_router(ch=FakeCH(), cold=FakeCold())
    with pytest.raises(ValueError, match="unknown table"):
        await router._route_query("nope", "ethereum")  # noqa: SLF001


async def test_route_query_rpc_disabled_skips_rpc() -> None:
    rpc = FakeRPC(address_rows=[_tr(block=150, tx="0xr1")])
    router = _make_router(ch=FakeCH(), cold=FakeCold(), rpc=rpc, on_demand_enabled=False)
    rows = await router._route_query("evm_transfers", "ethereum", from_block=100, to_block=200, limit=10)  # noqa: SLF001
    assert rows == []
    assert rpc.address_calls == 0


# ── RPCFetcher ──────────────────────────────────────────────────────────

# Valid 40-hex addresses (unpad_address takes the last 40 hex chars).
ADDR_A = "0x" + "1" * 40
ADDR_B = "0x" + "2" * 40
ADDR_WALLET = "0x" + "4" * 40
ADDR_TOKEN = "0x" + "3" * 40


def _pad(addr: str) -> str:
    return "0x" + addr[2:].lower().rjust(64, "0")


def _raw_log(
    block: int,
    log_index: int,
    tx: str,
    frm: str,
    to: str,
    token: str,
    value: str = "1000000000000000000",
) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [TRANSFER_EVENT_TOPIC, _pad(frm), _pad(to)],
        "data": hex(int(value)),
        "blockNumber": hex(block),
        "logIndex": hex(log_index),
        "transactionHash": tx,
    }


class FakeRpcClient:
    """RpcClient stand-in for RPCFetcher unit tests (no httpx)."""

    def __init__(
        self,
        receipts: dict[str, dict[str, Any]] | None = None,
        logs: dict[tuple[int, int], list[dict[str, Any]]] | None = None,
        blocks: dict[int, dict[str, Any]] | None = None,
        txs: dict[str, dict[str, Any]] | None = None,
        tip: int = 1_000_000,
        trace_result: list[dict[str, Any]] | None = None,
    ) -> None:
        self.receipts = receipts or {}
        self.logs_by_range = logs or {}
        self.blocks = blocks or {}
        self.txs = txs or {}
        self.tip = tip
        self.trace_result = trace_result
        self.calls: list[tuple[str, Any]] = []
        self.get_logs_params: list[dict[str, Any]] = []

    async def call(self, endpoint: str, method: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        self.calls.append((method, params))
        if method == "eth_getTransactionByHash":
            return {"result": self.txs.get(params[0]) if params else None}
        if method == "eth_getBlockByNumber":
            block_num = int(params[0], 16) if params else 0
            return {"result": self.blocks.get(block_num)}
        if method == "trace_filter":
            return {"result": self.trace_result}
        return None

    async def eth_get_transaction_receipt(self, endpoint: str, tx_hash: str) -> dict[str, Any] | None:
        return self.receipts.get(tx_hash)

    async def eth_block_number(self, endpoint: str) -> int | None:
        return self.tip

    async def get_logs(
        self,
        endpoint: str,
        from_block: int,
        to_block: int,
        topic: str,
        address: str | None = None,
        max_retries: int = 3,
        *,
        chain: str | None = None,
    ) -> list[dict[str, Any]] | None:
        self.get_logs_params.append(
            {"from_block": from_block, "to_block": to_block, "topic": topic, "address": address}
        )
        return self.logs_by_range.get((from_block, to_block), [])

    async def eth_get_block_by_number(
        self, endpoint: str, block_number: int, full_tx: bool = False
    ) -> dict[str, Any] | None:
        return self.blocks.get(block_number)


_ENDPOINTS = {"ethereum": ["https://eth.drpc.org"]}


def _make_fetcher(client: FakeRpcClient, **kwargs: Any) -> RPCFetcher:
    return RPCFetcher(client=cast(RpcClient, client), endpoints=_ENDPOINTS, **kwargs)


async def test_fetch_transfer_parses_receipt() -> None:
    receipt = {
        "transactionHash": "0xtx1",
        "blockNumber": hex(21000000),
        "blockTimestamp": hex(1700000000),
        "logs": [_raw_log(21000000, 3, "0xtx1", ADDR_A, ADDR_B, ADDR_TOKEN)],
    }
    client = FakeRpcClient(receipts={"0xtx1": receipt})
    fetcher = _make_fetcher(client)
    row = await fetcher.fetch_transfer("ethereum", "0xtx1")
    assert row is not None
    assert row["tx_hash"] == "0xtx1"
    assert row["block_number"] == 21000000
    assert row["from_address"] == ADDR_A
    assert row["to_address"] == ADDR_B
    assert row["token_address"] == ADDR_TOKEN
    assert row["value"] == "1000000000000000000"
    assert row["source"] == "rpc"


async def test_fetch_transfer_no_transfer_log() -> None:
    receipt = {
        "transactionHash": "0xtx2",
        "blockNumber": hex(1),
        "blockTimestamp": hex(1),
        "logs": [{"topics": ["0xnotatransfer"], "data": "0x", "logIndex": "0x0"}],
    }
    client = FakeRpcClient(receipts={"0xtx2": receipt})
    fetcher = _make_fetcher(client)
    assert await fetcher.fetch_transfer("ethereum", "0xtx2") is None


async def test_fetch_address_transfers_params_and_order() -> None:
    raw = [
        _raw_log(100, 1, "0xta", ADDR_WALLET, ADDR_B, ADDR_TOKEN),
        _raw_log(90, 0, "0xtb", ADDR_A, ADDR_WALLET, ADDR_TOKEN),
    ]
    client = FakeRpcClient(
        logs={(90, 100): raw},
        blocks={90: {"timestamp": hex(1700000000)}, 100: {"timestamp": hex(1700000080)}},
    )
    fetcher = _make_fetcher(client)
    rows = await fetcher.fetch_address_transfers("ethereum", ADDR_WALLET, 90, 100)
    assert [r["block_number"] for r in rows] == [100, 90]  # newest first
    assert rows[0]["from_address"] == ADDR_WALLET
    assert rows[1]["to_address"] == ADDR_WALLET
    assert client.get_logs_params[0]["topic"] == TRANSFER_EVENT_TOPIC
    assert client.get_logs_params[0]["address"] == ADDR_WALLET


async def test_fetch_address_transfers_clamps_range() -> None:
    client = FakeRpcClient()
    fetcher = _make_fetcher(client, max_block_range=5000)
    await fetcher.fetch_address_transfers("ethereum", "0xwallet", 0, 100_000)
    assert client.get_logs_params[0]["from_block"] == 95_000
    assert client.get_logs_params[0]["to_block"] == 100_000


async def test_fetch_address_transfers_default_range() -> None:
    client = FakeRpcClient(tip=1_000_000)
    fetcher = _make_fetcher(client, max_block_range=5000)
    await fetcher.fetch_address_transfers("ethereum", "0xwallet")
    assert client.get_logs_params[0]["from_block"] == 995_000
    assert client.get_logs_params[0]["to_block"] == 1_000_000


async def test_fetch_deployer_creation_tx() -> None:
    tx = {
        "hash": "0xcreatetx",
        "from": "0xDeployer",
        "blockNumber": hex(200),
        "timestamp": hex(1700000000),
    }
    client = FakeRpcClient(txs={"0xcreatetx": tx})
    fetcher = _make_fetcher(client)
    row = await fetcher.fetch_deployer("ethereum", "0xpair", creation_tx="0xcreatetx")
    assert row is not None
    assert row["deployer"] == "0xdeployer"  # lowercased
    assert row["deployer_eoa"] == "0xdeployer"
    assert row["block_number"] == 200


async def test_fetch_deployer_trace_filter() -> None:
    trace = [
        {
            "action": {"from": "0xcreator", "to": None},
            "result": {"address": "0xpair"},
            "blockNumber": hex(300),
            "transactionHash": "0xtrace_tx",
        }
    ]
    client = FakeRpcClient(trace_result=trace, tip=500_000)
    fetcher = _make_fetcher(client)
    row = await fetcher.fetch_deployer("ethereum", "0xpair")
    assert row is not None
    assert row["deployer"] == "0xcreator"
    assert row["tx_hash"] == "0xtrace_tx"


async def test_fetch_block_info() -> None:
    block = {"number": hex(123), "hash": "0xbeef", "transactions": [{"hash": "0x1"}]}
    client = FakeRpcClient(blocks={123: block})
    fetcher = _make_fetcher(client)
    assert await fetcher.fetch_block_info("ethereum", 123) == block


async def test_fetch_unknown_chain_is_safe() -> None:
    fetcher = RPCFetcher(client=cast(RpcClient, FakeRpcClient()), endpoints={})
    assert await fetcher.fetch_transfer("nosuch", "0xtx") is None
    assert await fetcher.fetch_address_transfers("nosuch", "0xwallet", 1, 2) == []
    assert await fetcher.fetch_deployer("nosuch", "0xpair") is None
    assert await fetcher.fetch_block_info("nosuch", 1) is None


async def test_fetcher_consecutive_pause() -> None:
    client = FakeRpcClient(blocks={1: {"number": "0x1"}, 2: {"number": "0x2"}, 3: {"number": "0x3"}})
    fetcher = _make_fetcher(client, max_consecutive=2, pause_seconds=0.0)
    assert await fetcher.fetch_block_info("ethereum", 1) is not None
    assert fetcher.consecutive_calls == 1
    assert await fetcher.fetch_block_info("ethereum", 2) is not None
    assert fetcher.consecutive_calls == 2
    # Third call crosses the budget → pause resets the counter, then counts again.
    assert await fetcher.fetch_block_info("ethereum", 3) is not None
    assert fetcher.consecutive_calls == 1


async def test_fetcher_pause_disabled_when_zero() -> None:
    client = FakeRpcClient(blocks={1: {"number": "0x1"}})
    fetcher = _make_fetcher(client, max_consecutive=0, pause_seconds=0.0)
    await fetcher.fetch_block_info("ethereum", 1)
    assert fetcher.consecutive_calls == 1  # counting continues, never pauses


# ── CLI wiring ──────────────────────────────────────────────────────────


def test_cli_parse_query_modes() -> None:
    from lite_indexer.cli import parse_args

    tx = parse_args(["query", "--chain", "ethereum", "--tx", "0xtx"])
    assert tx.command == "query"
    assert tx.tx == "0xtx"
    assert tx.chain == "ethereum"

    addr = parse_args(["query", "--address", "0xwallet", "--from-block", "100", "--to-block", "200", "--limit", "50"])
    assert addr.address == "0xwallet"
    assert addr.from_block == 100
    assert addr.to_block == 200
    assert addr.limit == 50

    blk = parse_args(["query", "--block", "21000000"])
    assert blk.block == 21000000

    dep = parse_args(["query", "--deployer", "0xpair", "--no-rpc"])
    assert dep.deployer == "0xpair"
    assert dep.no_rpc is True


async def test_cli_query_requires_exactly_one_mode() -> None:
    from lite_indexer.cli import parse_args, run_query_cli

    for argv in (["query"], ["query", "--tx", "0x1", "--block", "100"]):
        args = parse_args(argv)
        # The mode check happens before any ClickHouse/RPC construction.
        assert await run_query_cli(args) == 2


async def test_cli_query_block_without_address_rejected() -> None:
    from lite_indexer.cli import parse_args, run_query_cli

    args = parse_args(["query", "--block", "100", "--from-block", "1"])
    assert await run_query_cli(args) == 2


# ── Config defaults ──────────────────────────────────────────────────────


def test_config_defaults() -> None:
    from lite_indexer.config import (
        ON_DEMAND_RPC_CACHE_TTL,
        ON_DEMAND_RPC_ENABLED,
        ON_DEMAND_RPC_MAX_BLOCK_RANGE,
        ON_DEMAND_RPC_MAX_CONSECUTIVE,
    )

    assert ON_DEMAND_RPC_ENABLED is True
    assert ON_DEMAND_RPC_CACHE_TTL == 3600
    assert ON_DEMAND_RPC_MAX_BLOCK_RANGE == 5000
    assert ON_DEMAND_RPC_MAX_CONSECUTIVE == 10


# ── API wiring (fallback through FastAPI) ────────────────────────────────


class _FakeApiRouter:
    """QueryRouter stand-in injected into the API for fallback tests."""

    def __init__(self, transfers: list[dict[str, Any]] | None = None, deployer: dict[str, Any] | None = None) -> None:
        self._transfers = transfers or []
        self._deployer = deployer
        self.calls: list[tuple[str, str]] = []

    async def get_transfers(
        self,
        chain: str,
        address: str,
        from_block: int | None = None,
        to_block: int | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        self.calls.append(("transfers", f"{chain}:{address}"))
        return list(self._transfers)

    async def get_deployer_info(self, chain: str, address: str) -> dict[str, Any] | None:
        self.calls.append(("deployer", f"{chain}:{address}"))
        return self._deployer


def _test_client(monkeypatch: pytest.MonkeyPatch, router: _FakeApiRouter) -> Any:
    """Build a TestClient with the indexed stores empty + a fake router."""
    from fastapi.testclient import TestClient

    from api.app import create_app

    monkeypatch.setattr("api.db.get_query_router", lambda: router)
    monkeypatch.setattr("api.routers.transfers.query", lambda sql, params=None: [])
    monkeypatch.setattr("api.routers.deployers.query", lambda sql, params=None: [])
    app = create_app()
    return TestClient(app)


def test_api_transfers_fallback_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _FakeApiRouter(transfers=[_tr(chain="ethereum", block=123, tx="0xondemand", frm=ADDR_WALLET, to=ADDR_B)])
    with _test_client(monkeypatch, router) as c:
        resp = c.get(f"/api/v1/transfers/{ADDR_WALLET}?chain=ethereum&include_counterparty=true")
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["tx_hash"] == "0xondemand"
    assert body["data"][0]["from_address"] == ADDR_WALLET
    assert body["data"][0]["direction"] == "out"
    assert router.calls == [("transfers", f"ethereum:{ADDR_WALLET}")]


def test_api_transfers_no_fallback_when_indexed_rows_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    """Indexed rows win — the on-demand router is never consulted."""
    router = _FakeApiRouter(transfers=[_tr(block=999, tx="0xrpc")])

    def fake_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return [
            {
                "chain": "ethereum",
                "block_number": 1,
                "block_timestamp": "2026-01-01 00:00:00",
                "tx_hash": "0xindexed",
                "log_index": 0,
                "from_address": ADDR_WALLET,
                "to_address": ADDR_B,
                "token_address": ADDR_TOKEN,
                "value": "42",
                "address": ADDR_WALLET,
                "direction": "out",
            }
        ]

    from fastapi.testclient import TestClient

    from api.app import create_app

    monkeypatch.setattr("api.db.get_query_router", lambda: router)
    monkeypatch.setattr("api.routers.transfers.query", fake_query)
    app = create_app()
    with TestClient(app) as c:
        resp = c.get(f"/api/v1/transfers/{ADDR_WALLET}?chain=ethereum&include_counterparty=true")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["tx_hash"] == "0xindexed"
    assert router.calls == []


def test_api_deployers_fallback_when_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    router = _FakeApiRouter(deployer=_deployer_row(pair=ADDR_WALLET))
    with _test_client(monkeypatch, router) as c:
        resp = c.get(f"/api/v1/deployers/{ADDR_WALLET}?chain=ethereum")
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["deployer"] == "0xdeployer"
    assert body["data"][0]["pair_address"] == ADDR_WALLET
    assert router.calls == [("deployer", f"ethereum:{ADDR_WALLET}")]
