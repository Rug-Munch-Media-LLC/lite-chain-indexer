"""Integration tests for deployer EOA resolution.

The PairCreated/PoolCreated log only exposes the *factory* address, not
the EOA that submitted the deployment transaction. The indexer resolves
the real deployer EOA via ``eth_getTransactionByHash`` (tx ``from``)
before writing to ``deployer_events``.

These tests verify:

1. ``_resolve_deployer_eoas`` maps tx hash → EOA ``from`` (lowercased)
2. Endpoint rotation recovers an EOA when the best endpoint fails
3. ``write_deployer_events`` stores the EOA in ``deployer`` /
   ``deployer_eoa`` — never the factory address
4. ``pair_address`` / ``token_address`` are mapped correctly

All ClickHouse/RPC interactions are mocked; no external services needed.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.storage.clickhouse import (
    ClickHouseWriter,
    _resolve_deployer_eoas,
)

# ── Fixtures ───────────────────────────────────────────────────────────

FACTORY = "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"  # Uniswap V2 factory
EOA = "0x1234567890abcdef1234567890abcdef12345678"  # the real deployer EOA
TX_HASH = "0x" + "ab" * 32


class FakeRpcClient:
    """RPC double for eth_getTransactionByHash.

    ``by_tx`` maps tx_hash → ``from``; endpoints in ``failing`` raise or
    return no result so the rotation logic is exercised.
    """

    def __init__(
        self,
        by_tx: dict[str, str] | None = None,
        failing_endpoints: set[str] | None = None,
    ) -> None:
        self.by_tx = by_tx or {}
        self.failing_endpoints = failing_endpoints or set()
        self.calls: list[tuple[str, str, list[Any]]] = []

    async def call(
        self,
        endpoint: str,
        method: str,
        params: list[Any] | None = None,
        timeout: float = 15.0,
    ) -> dict[str, Any] | None:
        self.calls.append((endpoint, method, params or []))
        if endpoint in self.failing_endpoints:
            return {"result": None}
        tx_hash = (params or [""])[0]
        from_addr = self.by_tx.get(tx_hash)
        if not from_addr:
            return {"result": None}
        return {"jsonrpc": "2.0", "id": 1, "result": {"from": from_addr}}


class FakeCH:
    """ClickHouse double for deployer write tests."""

    def __init__(self) -> None:
        self.queries: list[str] = []
        self.inserted: list[tuple[Any, ...]] = []

    def execute(
        self,
        query: str,
        params: Any = None,
        with_column_types: bool = False,
    ) -> Any:
        self.queries.append(query)
        if "system.columns" in query:
            # Pretend every column already exists → no ALTERs issued.
            return [("token0",), ("token1",), ("pair_address",), ("deployer_eoa",), ("source",)]
        if query.startswith("ALTER TABLE"):
            return []
        if query.startswith("INSERT INTO wallet_memory.deployer_events"):
            self.inserted.extend(params or [])
            return len(params or [])
        return []


@pytest.fixture()
def _monkeypatch_rpc(monkeypatch: pytest.MonkeyPatch, fake_rpc: FakeRpcClient):
    """Point _resolve_deployer_eoas at the fake RPC client + endpoints."""

    def _fake_get_best(endpoints: list[str]) -> str:
        return endpoints[0]

    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: fake_rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", _fake_get_best)
    return fake_rpc


@pytest.fixture()
def fake_rpc() -> FakeRpcClient:
    return FakeRpcClient(by_tx={TX_HASH: EOA})


# ═══════════════════════════════════════════════════════════════════════════
# _resolve_deployer_eoas — tx from → EOA mapping
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_returns_from_address(
    _monkeypatch_rpc: FakeRpcClient,
) -> None:
    """The EOA is the tx `from`, not the factory (which is not in the tx)."""
    mapping = await _resolve_deployer_eoas("ethereum", [{"transactionHash": TX_HASH}])
    assert mapping == {TX_HASH: EOA}
    assert mapping[TX_HASH] != FACTORY


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_lowercases_from(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRpcClient(by_tx={TX_HASH: "0xABC123ABC123ABC123ABC123ABC123ABC123ABc1"})
    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", lambda eps: eps[0])

    mapping = await _resolve_deployer_eoas("ethereum", [{"transactionHash": TX_HASH}])
    assert mapping[TX_HASH] == "0xabc123abc123abc123abc123abc123abc123abc1"


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_empty_events() -> None:
    assert await _resolve_deployer_eoas("ethereum", []) == {}
    assert await _resolve_deployer_eoas("ethereum", [{"transactionHash": ""}]) == {}


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_no_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    # _resolve_deployer_eoas imports GET_LOGS_ENDPOINTS from the chains
    # package at call time — patch the package attribute.
    monkeypatch.setattr(
        "lite_indexer.chains.GET_LOGS_ENDPOINTS",
        {"ethereum": []},
    )
    assert await _resolve_deployer_eoas("ethereum", [{"transactionHash": TX_HASH}]) == {}


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_rotates_to_healthy_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pool is rotated: a dead best endpoint must not blank the batch."""
    from lite_indexer.chains import GET_LOGS_ENDPOINTS

    # First (best) endpoint fails, a later one succeeds — resolution recovers.
    # The failing endpoint is derived from the live pool so the test stays
    # valid when the Ethereum endpoint ordering changes.
    first_endpoint = GET_LOGS_ENDPOINTS["ethereum"][0]
    rpc = FakeRpcClient(
        by_tx={TX_HASH: EOA},
        failing_endpoints={first_endpoint},
    )
    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", lambda eps: eps[0])

    mapping = await _resolve_deployer_eoas("ethereum", [{"transactionHash": TX_HASH}])
    assert mapping == {TX_HASH: EOA}
    # It tried more than one endpoint.
    assert len({call[0] for call in rpc.calls}) >= 2


@pytest.mark.asyncio
async def test_resolve_deployer_eoas_all_endpoints_fail_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rpc = FakeRpcClient(by_tx={}, failing_endpoints=set())
    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", lambda eps: eps[0])
    monkeypatch.setattr("lite_indexer.rpc.record_endpoint_failure", lambda ep: None)

    mapping = await _resolve_deployer_eoas("ethereum", [{"transactionHash": TX_HASH}])
    assert mapping == {}


# ═══════════════════════════════════════════════════════════════════════════
# write_deployer_events — stored row integrity
# ═══════════════════════════════════════════════════════════════════════════


@pytest.mark.asyncio
async def test_write_deployer_events_stores_eoa_not_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """deployer / deployer_eoa = real EOA; factory stays the factory."""
    events = [
        {
            "factory": FACTORY,
            "pair_address": "0x" + "11" * 20,
            "token0": "0x" + "22" * 20,
            "token1": "0x" + "33" * 20,
            "version": "v2",
            "blockNumber": 18_000_000,
            "blockTimestamp": "2026-07-30T10:00:00Z",
            "transactionHash": TX_HASH,
        }
    ]
    client = FakeCH()
    writer = ClickHouseWriter()
    writer._client = client  # inject the fake client (bypasses _connect)

    rpc = FakeRpcClient(by_tx={TX_HASH: EOA})
    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", lambda eps: eps[0])

    inserted = await writer.write_deployer_events("ethereum", events)

    assert inserted == 1
    assert len(client.inserted) == 1
    row = client.inserted[0]
    # Column order: (deployer, deployer_eoa, token_address, token0, token1,
    #                pair_address, chain, block_number, block_timestamp,
    #                tx_hash, type, factory, ingested_at, source)
    assert row[0] == EOA  # deployer
    assert row[1] == EOA  # deployer_eoa
    assert row[0] != FACTORY  # the key assertion: EOA, not factory
    assert row[2] == "0x" + "22" * 20  # token_address ← token0
    assert row[3] == "0x" + "22" * 20  # token0
    assert row[4] == "0x" + "33" * 20  # token1
    assert row[5] == "0x" + "11" * 20  # pair_address
    assert row[6] == "ethereum"
    assert row[7] == 18_000_000
    assert row[9] == TX_HASH
    assert row[10] == "v2_pool"
    assert row[11] == FACTORY  # factory field preserved for audit


@pytest.mark.asyncio
async def test_write_deployer_events_empty_events() -> None:
    writer = ClickHouseWriter()
    assert await writer.write_deployer_events("ethereum", []) == 0


@pytest.mark.asyncio
async def test_write_deployer_events_resolution_failure_keeps_empty_deployer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If EOA resolution fails, deployer stays empty — never the factory."""
    events = [
        {
            "factory": FACTORY,
            "pair_address": "0x" + "11" * 20,
            "token0": "0x" + "22" * 20,
            "token1": "0x" + "33" * 20,
            "version": "v3",
            "blockNumber": 123,
            "transactionHash": TX_HASH,
        }
    ]
    client = FakeCH()
    writer = ClickHouseWriter()
    writer._client = client

    # All endpoints fail → no EOA resolution.
    rpc = FakeRpcClient(by_tx={})
    monkeypatch.setattr("lite_indexer.rpc.RpcClient", lambda: rpc)
    monkeypatch.setattr("lite_indexer.rpc.get_best_endpoint", lambda eps: eps[0])
    monkeypatch.setattr("lite_indexer.rpc.record_endpoint_failure", lambda ep: None)

    inserted = await writer.write_deployer_events("ethereum", events)
    assert inserted == 1
    row = client.inserted[0]
    assert row[0] == ""  # deployer empty, NOT the factory
    assert row[1] == ""  # deployer_eoa empty
    assert row[11] == FACTORY  # factory retained separately
