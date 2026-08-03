"""Hermetic route tests for the Chain Query API.

ClickHouse is mocked at the ``api.db.query`` / ``api.db.query_rows``
boundary, so these run without a live warehouse. See test_api_smoke.py
for the real-warehouse smoke test.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with ClickHouse queries stubbed out."""
    app = create_app()

    def fake_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        # Unknown chain filters return no rows (matches ClickHouse).
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
                "deployer": "0xdeployer",
                "deployer_eoa": "0xdeployer",
                "token0": "0xtoken",
                "token1": "0xother",
                "pair_address": "0xpair",
                "type": "v2_pool",
                "factory": "0xfactory",
                "dex": "uniswap",
                "version": "v2",
                "contract_address": "0xpair",
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

    def fake_query_rows(
        sql: str,
        params: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
        return (
            [("ethereum", 1, "0xtx", "0xfrom", "0xto", "0xtok", "42", datetime(2026, 1, 1))],
            [
                ("chain", "String"),
                ("block_number", "UInt64"),
                ("tx_hash", "String"),
                ("from_address", "String"),
                ("to_address", "String"),
                ("token_address", "String"),
                ("value", "String"),
                ("block_timestamp", "DateTime"),
            ],
        )

    monkeypatch.setattr("api.routers.transfers.query", fake_query)
    monkeypatch.setattr("api.routers.deployers.query", fake_query)
    monkeypatch.setattr("api.routers.whales.query", fake_query)
    monkeypatch.setattr("api.routers.stats.query", fake_query)
    monkeypatch.setattr("api.routers.query.query_rows", fake_query_rows)
    with TestClient(app) as c:
        yield c


def test_root(client: TestClient) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["service"] == "chain-query-api"


def test_health(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200


def test_docs(client: TestClient) -> None:
    resp = client.get("/docs")
    assert resp.status_code == 200
    assert "swagger" in resp.text.lower()


def test_openapi_lists_endpoints(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/v1/transfers/{address}" in paths
    assert "/api/v1/deployers/{address}" in paths
    assert "/api/v1/whales" in paths
    assert "/api/v1/stats" in paths
    assert "/api/v1/query" in paths


def test_transfers_summary(client: TestClient) -> None:
    resp = client.get("/api/v1/transfers/0xwallet?chain=ethereum")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pagination"]["has_more"] is False
    assert body["data"][0]["direction"] == "out"


def test_transfers_unknown_chain_returns_empty(client: TestClient) -> None:
    resp = client.get("/api/v1/transfers/0xwallet?chain=notachain")
    assert resp.status_code == 200
    assert resp.json()["data"] == []


def test_transfers_invalid_direction(client: TestClient) -> None:
    resp = client.get("/api/v1/transfers/0xwallet?direction=bogus")
    assert resp.status_code == 422


def test_deployers(client: TestClient) -> None:
    resp = client.get("/api/v1/deployers/0xdeployer")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["chain"] == "ethereum"


def test_whales(client: TestClient) -> None:
    resp = client.get("/api/v1/whales?chain=ethereum&min_usd=100000")
    assert resp.status_code == 200


def test_whales_unknown_chain_empty(client: TestClient) -> None:
    resp = client.get("/api/v1/whales?chain=notachain")
    assert resp.status_code == 200
    assert resp.json()["data"] == []


def test_stats(client: TestClient) -> None:
    resp = client.get("/api/v1/stats")
    assert resp.status_code == 200
    assert resp.json()["total_transfers"] == 1


def test_query_valid(client: TestClient) -> None:
    resp = client.post("/api/v1/query", json={"query": "SELECT * FROM evm_transfers LIMIT 1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["columns"][0] == "chain"
    assert body["row_count"] == 1


def test_query_rejects_write(client: TestClient) -> None:
    resp = client.post("/api/v1/query", json={"query": "DROP TABLE evm_transfers"})
    assert resp.status_code == 422


def test_query_rejects_unknown_table(client: TestClient) -> None:
    resp = client.post("/api/v1/query", json={"query": "SELECT * FROM system.users"})
    assert resp.status_code == 422


def test_metrics_endpoint(client: TestClient) -> None:
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert b"chain_api_requests_total" in resp.content
