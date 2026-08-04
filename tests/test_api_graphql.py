"""Hermetic tests for the GraphQL read API (api/graphql.py).

ClickHouse is mocked at the ``api.graphql.query`` boundary (the resolvers
bind ``query`` at import time, matching the pattern used by the REST route
tests). These run without a live warehouse; the schema itself is validated
by introspection.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with ClickHouse queries stubbed out for GraphQL resolvers."""
    app = create_app()

    def fake_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if params and params.get("chain") == "notachain":
            return []
        return [
            {
                "chain": "ethereum",
                "block_number": 4305316,
                "block_timestamp": datetime(2017, 9, 23, 18, 26, 42),
                "tx_hash": "0xabc",
                "log_index": 70,
                "from_address": "0xwallet",
                "to_address": "0xother",
                "token_address": "0xtoken",
                "value": "1000000000000000000",
                "address": "0xwallet",
                "direction": "out",
                "contract_address": "0xpair",
                "dex": "uniswap",
                "version": "v2",
                "token0": "0xtoken",
                "token1": "0xother",
                "sender": "0xwallet",
                "amount0": "100000000000000000000",
                "amount1": "0",
                "total_transfers": 1,
                "unique_senders": 1,
                "unique_receivers": 1,
                "unique_tokens": 1,
                "total_raw_value": 1.0,
                "min_ts": datetime(2017, 9, 23, 18, 26, 42),
                "max_ts": datetime(2017, 9, 23, 18, 26, 42),
                "count()": 1,
            }
        ]

    monkeypatch.setattr("api.graphql.query", fake_query)
    with TestClient(app) as c:
        yield c


def _gql(client: TestClient, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    resp = client.post("/graphql", json={"query": query, "variables": variables or {}})
    assert resp.status_code == 200
    return resp.json()


def test_graphql_endpoint_present(client: TestClient) -> None:
    """The GraphQL playground + schema are served at /graphql."""
    resp = client.get("/graphql")
    assert resp.status_code == 200


def test_graphql_transfers_query(client: TestClient) -> None:
    """transfers(address) returns address-first wallet history."""
    data = _gql(
        client,
        "query($addr: String!) { transfers(address: $addr, limit: 5) { chain blockNumber txHash direction value } }",
        {"addr": "0xwallet"},
    )
    assert "errors" not in data, data.get("errors")
    transfers = data["data"]["transfers"]
    assert len(transfers) == 1
    t = transfers[0]
    assert t["chain"] == "ethereum"
    assert t["txHash"] == "0xabc"
    assert t["direction"] == "out"


def test_graphql_transfers_empty_for_unknown_chain(client: TestClient) -> None:
    """An unknown chain filter yields an empty list, not an error."""
    data = _gql(
        client,
        'query { transfers(address: "0xwallet", chain: "notachain") { chain } }',
    )
    assert data["data"]["transfers"] == []


def test_graphql_chain_stats(client: TestClient) -> None:
    """chainStats returns aggregate counts."""
    data = _gql(client, 'query { chainStats(chain: "ethereum") { totalTransfers uniqueSenders dexSwaps } }')
    assert "errors" not in data, data.get("errors")
    stats = data["data"]["chainStats"]
    assert stats["totalTransfers"] == 1
    assert stats["uniqueSenders"] == 1
    assert stats["dexSwaps"] == 1


def test_graphql_whales(client: TestClient) -> None:
    """whales returns USD-valued large swaps."""
    data = _gql(client, "query { whales(minUsd: 100000, limit: 3) { chain usdValue pairAddress } }")
    assert "errors" not in data, data.get("errors")
    whales = data["data"]["whales"]
    assert len(whales) == 1
    assert whales[0]["pairAddress"] == "0xpair"


def test_graphql_invalid_query_returns_errors(client: TestClient) -> None:
    """Malformed GraphQL returns a 200 with errors, never a crash."""
    data = _gql(client, "query { noSuchField }")
    assert "errors" in data
    assert data["errors"]


def test_graphql_introspection(client: TestClient) -> None:
    """Schema introspection works (tools discover the surface)."""
    data = _gql(
        client,
        "query { __schema { queryType { fields { name } } } }",
    )
    names = {f["name"] for f in data["data"]["__schema"]["queryType"]["fields"]}
    assert {"transfers", "chainStats", "whales"} <= names
