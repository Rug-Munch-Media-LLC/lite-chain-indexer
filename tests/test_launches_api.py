"""Hermetic tests for the launch + bundle endpoints.

ClickHouse is mocked at the ``api.routers.launches.query`` boundary, so
these run without a live warehouse (same convention as
test_api_routes.py). The raw ``/query`` allowlist test also verifies the
launch tables are reachable through :func:`api.security.validate_readonly`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app

# Naive datetime — matches what clickhouse_driver returns for DateTime
# columns (the router's _fmt_ts appends the 'Z' suffix).
_TS = datetime(2026, 8, 13, 12, 0, 0)


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with the launches router's ClickHouse queries stubbed."""
    app = create_app()

    def fake_launch_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        if "solana_bundles" in sql:
            return [
                {
                    "chain": "solana",
                    "slot": 331_000_123,
                    "block_timestamp": _TS,
                    "create_tx_signature": "createSig111",
                    "tip_tx_signature": "tipSig222",
                    "tip_account": "96gYZQnLbNiYQrrDE7VkyfmZ7i9KqE3R4T7z8qQ9mNx",
                    "mint": "mintLaunch222",
                    "buyer_count": 3,
                    "buyer_wallets": ["walletA", "walletB", "walletC"],
                }
            ]
        return [
            {
                "chain": "solana",
                "block_timestamp": _TS,
                "address": "mintLaunch111",
                "symbol": "MEME",
                "creator": "creator111",
                "dex": "pumpfun",
                "tx_hash": "sigLaunch111",
                "event_type": "launchpad",
                "launch_signal": 1,
                "graduation_signal": 0,
                "confidence": "high",
            }
        ]

    monkeypatch.setattr("api.routers.launches.query", fake_launch_query)

    def fake_query_rows(
        sql: str,
        params: dict[str, Any] | None = None,
        settings: dict[str, Any] | None = None,
    ) -> tuple[list[tuple[Any, ...]], list[tuple[str, str]]]:
        return (
            [("solana", 1, "0xtx", "2026-08-13T12:00:00Z")],
            [
                ("chain", "String"),
                ("block_number", "UInt64"),
                ("tx_hash", "String"),
                ("block_timestamp", "DateTime"),
            ],
        )

    monkeypatch.setattr("api.routers.query.query_rows", fake_query_rows)
    with TestClient(app) as c:
        yield c


def test_openapi_lists_launch_endpoints(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/v1/launches" in paths
    assert "/api/v1/bundles" in paths


def test_launches_returns_mapped_row(client: TestClient) -> None:
    resp = client.get("/api/v1/launches")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pagination"]["has_more"] is False
    assert len(body["data"]) == 1
    item = body["data"][0]
    assert item["chain"] == "solana"
    assert item["address"] == "mintLaunch111"
    assert item["symbol"] == "MEME"
    assert item["dex"] == "pumpfun"
    assert item["event_type"] == "launchpad"
    assert item["launch_signal"] == 1
    assert item["graduation_signal"] == 0
    assert item["confidence"] == "high"
    # Not tracked by the launch tables — always null by contract.
    assert item["liquidity"] is None
    assert item["mcap"] is None
    # age_seconds is computed from block_timestamp (past event → > 0).
    assert item["age_seconds"] > 0
    assert item["block_timestamp"] == "2026-08-13T12:00:00Z"


def test_launches_chain_filter_passed_through(client: TestClient) -> None:
    resp = client.get("/api/v1/launches?chain=solana")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["chain"] == "solana"


def test_launches_limit_validation(client: TestClient) -> None:
    # limit must be within [1, 1000]
    resp = client.get("/api/v1/launches?limit=0")
    assert resp.status_code == 422


def test_bundles_returns_mapped_row(client: TestClient) -> None:
    resp = client.get("/api/v1/bundles")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 1
    item = body["data"][0]
    assert item["chain"] == "solana"
    assert item["slot"] == 331_000_123
    assert item["create_tx_signature"] == "createSig111"
    assert item["tip_tx_signature"] == "tipSig222"
    assert item["mint"] == "mintLaunch222"
    assert item["buyer_count"] == 3
    assert item["buyer_wallets"] == ["walletA", "walletB", "walletC"]
    assert item["age_seconds"] > 0


def test_bundles_chain_filter_passed_through(client: TestClient) -> None:
    resp = client.get("/api/v1/bundles?chain=solana")
    assert resp.status_code == 200
    assert resp.json()["data"][0]["chain"] == "solana"


def test_query_allowlist_reaches_launch_tables(client: TestClient) -> None:
    """Raw /query can read every launch table (validate_readonly allowlist)."""
    for table in ("solana_launches", "tron_launches", "solana_bundles", "evm_launches"):
        resp = client.post(
            "/api/v1/query",
            json={"query": f"SELECT * FROM wallet_memory.{table} LIMIT 1"},
        )
        assert resp.status_code == 200, f"{table} not allowlisted: {resp.text}"


def test_query_launch_aliases(client: TestClient) -> None:
    """TABLE_ALIASES resolves launch-table aliases in raw /query."""
    for alias, _canonical in (
        ("solana_launch", "solana_launches"),
        ("tron_launch", "tron_launches"),
        ("evm_launch", "evm_launches"),
        ("bundle", "solana_bundles"),
        ("bundles", "solana_bundles"),
    ):
        resp = client.post("/api/v1/query", json={"query": f"SELECT * FROM {alias} LIMIT 1"})
        assert resp.status_code == 200, f"alias {alias} failed: {resp.text}"
