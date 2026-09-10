"""Hermetic tests for the wallet-tracing address endpoints.

ClickHouse is mocked at the ``api.routers.address.query`` boundary, so
these run without a live warehouse (same convention as
``test_launches_api.py`` / ``test_api_routes.py``).

Covers the three reads over the immutable keystone:

* ``/activity`` — ``address_activity`` history, ``FINAL``-deduplicated,
  time-range + pagination, empty-for-unknown, Solana-only chain gate.
* ``/funders``  — inbound value trace aggregated from ``solana_transfers``.
* ``/counters`` — outbound value trace aggregated from ``solana_transfers``.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app

# Naive datetime — matches clickhouse_driver's DateTime/DateTime64 return
# shape (the router's _fmt_ts appends the 'Z' suffix).
_TS = datetime(2026, 8, 13, 12, 0, 0)


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """TestClient with the address router's ClickHouse queries stubbed."""
    app = create_app()
    captured: dict[str, Any] = {"sql": [], "params": []}

    def fake_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        captured["sql"].append(sql)
        captured["params"].append(params or {})
        if "address_activity" in sql:
            # Unknown wallets return no rows — the empty-list contract.
            if params and params.get("address") == "unknownwallet":
                return []
            return [
                {
                    "address": "walletxyz",
                    "tx_signature": "sigActivity1",
                    "slot": 331_000_123,
                    "block_time": _TS,
                    "involved_accounts": ["walletxyz", "funderabc"],
                    "program_ids": ["systemProgram", "tokenProgram"],
                    "is_signer": True,
                }
            ]
        # solana_transfers counterparty aggregation.
        if "GROUP BY to_address" in sql:
            return [
                {
                    "counterparty": "recipientabc",
                    "transfer_count": 3,
                    "tx_count": 2,
                    "first_seen": _TS,
                    "last_seen": datetime(2026, 8, 14, 12, 0, 0),
                }
            ]
        return [
            {
                "counterparty": "funderabc",
                "transfer_count": 2,
                "tx_count": 2,
                "first_seen": datetime(2026, 8, 12, 12, 0, 0),
                "last_seen": _TS,
            }
        ]

    monkeypatch.setattr("api.routers.address.query", fake_query)
    with TestClient(app) as c:
        c.captured = captured  # type: ignore[attr-defined]
        yield c


def test_openapi_lists_address_endpoints(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/v1/address/{address}/activity" in paths
    assert "/api/v1/address/{address}/funders" in paths
    assert "/api/v1/address/{address}/counters" in paths


def test_activity_returns_deduped_mapped_row(client: TestClient) -> None:
    resp = client.get("/api/v1/address/walletxyz/activity")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pagination"]["has_more"] is False
    assert len(body["data"]) == 1
    item = body["data"][0]
    assert item["address"] == "walletxyz"
    assert item["tx_signature"] == "sigActivity1"
    assert item["slot"] == 331_000_123
    assert item["block_time"] == "2026-08-13T12:00:00Z"
    assert item["involved_accounts"] == ["walletxyz", "funderabc"]
    assert item["program_ids"] == ["systemProgram", "tokenProgram"]
    assert item["is_signer"] is True
    # Dedup is delegated to ReplacingMergeTree FINAL on (address, tx_signature).
    sql = client.captured["sql"][-1]  # type: ignore[attr-defined]
    assert "wallet_memory.address_activity FINAL" in sql
    assert "ORDER BY block_time DESC, slot DESC" in sql


def test_activity_time_range_and_pagination_params(client: TestClient) -> None:
    resp = client.get("/api/v1/address/walletxyz/activity?after=2026-08-01&before=2026-08-31&limit=10&offset=5")
    assert resp.status_code == 200
    params = client.captured["params"][-1]  # type: ignore[attr-defined]
    assert params["address"] == "walletxyz"
    assert params["after_ts"] == datetime(2026, 8, 1)
    assert params["before_ts"] == datetime(2026, 8, 31)
    sql = client.captured["sql"][-1]  # type: ignore[attr-defined]
    assert "LIMIT 10 OFFSET 5" in sql


def test_activity_unknown_address_returns_empty_list(client: TestClient) -> None:
    resp = client.get("/api/v1/address/unknownwallet/activity")
    assert resp.status_code == 200
    assert resp.json()["data"] == []


def test_activity_chain_filter_applied_in_sql(client: TestClient) -> None:
    resp = client.get("/api/v1/address/walletxyz/activity?chain=ethereum")
    assert resp.status_code == 200
    # Chain filter is applied in SQL (not a Solana-only short-circuit).
    assert any("chain = %(chain)s" in s for s in client.captured["sql"])  # type: ignore[attr-defined]


def test_funders_aggregates_inbound_counterparties(client: TestClient) -> None:
    resp = client.get("/api/v1/address/walletxyz/funders")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 1
    item = body["data"][0]
    assert item["address"] == "funderabc"
    assert item["tx_count"] == 2
    assert item["transfer_count"] == 2
    assert item["first_seen"] == "2026-08-12T12:00:00Z"
    assert item["last_seen"] == "2026-08-13T12:00:00Z"
    sql = client.captured["sql"][-1]  # type: ignore[attr-defined]
    assert "wallet_memory.solana_transfers" in sql
    assert "to_address = %(address)s" in sql
    assert "GROUP BY from_address" in sql


def test_counters_aggregates_outbound_counterparties(client: TestClient) -> None:
    resp = client.get("/api/v1/address/walletxyz/counters")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 1
    item = body["data"][0]
    assert item["address"] == "recipientabc"
    assert item["tx_count"] == 2
    assert item["transfer_count"] == 3
    sql = client.captured["sql"][-1]  # type: ignore[attr-defined]
    assert "from_address = %(address)s" in sql
    assert "GROUP BY to_address" in sql


def test_blank_address_rejected(client: TestClient) -> None:
    resp = client.get("/api/v1/address/%20/activity")
    assert resp.status_code == 422


def test_activity_limit_validation(client: TestClient) -> None:
    # limit must be within [1, 1000].
    resp = client.get("/api/v1/address/walletxyz/activity?limit=0")
    assert resp.status_code == 422
