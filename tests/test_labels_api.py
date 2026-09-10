"""Hermetic tests for the label-graph endpoints (``/api/v1/labels``).

ClickHouse is mocked at the ``api.routers.labels`` boundary (``query`` for
reads, ``insert_entity_labels`` for writes), so these run without a live
warehouse — same convention as ``test_address_api.py``.

Covers entity resolution (ranked labels, primary label, unknown = empty),
chain filtering, the label-growth write path, write auth, disabled-write
and store-unavailable degradation, and input validation.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app

_TS = datetime(2026, 9, 1, 12, 0, 0)


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """TestClient with the label router's reads/writes stubbed."""
    app = create_app()
    captured: dict[str, Any] = {"sql": [], "params": [], "inserted": []}

    def fake_query(sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        captured["sql"].append(sql)
        captured["params"].append(params or {})
        if params and params.get("address") == "0xunknown":
            return []
        return [
            {
                "address": "0xabc",
                "chain_id": "ethereum",
                "entity_type": "exchange",
                "label_type": "cex_hot_wallet",
                "name": "Binance 7",
                "source": "etherscan",
                "provenance": "etherscan-label-cloud",
                "confidence": 0.95,
                "first_seen": _TS,
            },
            {
                "address": "0xabc",
                "chain_id": "ethereum",
                "entity_type": "wallet",
                "label_type": "eoa_wallet",
                "name": "some whale",
                "source": "community",
                "provenance": "community-list",
                "confidence": 0.4,
                "first_seen": _TS,
            },
        ]

    def fake_insert(labels: list[dict[str, Any]]) -> int:
        captured["inserted"].append(labels)
        return len(labels)

    monkeypatch.setattr("api.routers.labels.query", fake_query)
    monkeypatch.setattr("api.routers.labels.insert_entity_labels", fake_insert)
    with TestClient(app) as c:
        c.captured = captured  # type: ignore[attr-defined]
        yield c


def test_get_label_resolves_primary(client: TestClient) -> None:
    resp = client.get("/api/v1/labels/0xABC")
    assert resp.status_code == 200
    body = resp.json()
    assert body["resolved"] is True
    assert body["address"] == "0xabc"  # normalized lowercase
    assert body["entity_type"] == "exchange"
    assert body["label_type"] == "cex_hot_wallet"
    assert body["name"] == "Binance 7"
    assert body["primary_label"] == "Binance 7"
    assert len(body["labels"]) == 2
    # Highest confidence first (ClickHouse ORDER BY is stubbed in the fake).
    assert body["labels"][0]["confidence"] == 0.95
    # No raw DB row leaked — response is the Pydantic shape only.
    assert set(body["labels"][0]) >= {"address", "chain", "entity_type", "label_type"}


def test_get_label_unknown_is_empty_not_404(client: TestClient) -> None:
    resp = client.get("/api/v1/labels/0xunknown")
    assert resp.status_code == 200
    body = resp.json()
    assert body["resolved"] is False
    assert body["labels"] == []
    assert body["entity_type"] is None


def test_get_label_chain_filter_and_limit_bound(client: TestClient) -> None:
    resp = client.get("/api/v1/labels/0xabc?chain=Ethereum&limit=50")
    assert resp.status_code == 200
    captured = client.captured  # type: ignore[attr-defined]
    assert captured["params"][-1]["chain"] == "ethereum"
    assert "LIMIT 50" in captured["sql"][-1]


def test_get_label_limit_over_max_422(client: TestClient) -> None:
    resp = client.get("/api/v1/labels/0xabc?limit=999")
    assert resp.status_code == 422


def test_get_label_empty_address_422(client: TestClient) -> None:
    resp = client.get("/api/v1/labels/%20")
    assert resp.status_code == 422


def test_post_labels_persists(client: TestClient) -> None:
    payload = {
        "labels": [
            {
                "address": "0xDeadBeef00000000000000000000000000000001",
                "chain": "ethereum",
                "entity_type": "malicious",
                "label_type": "phishing",
                "name": "Fake_Phishing1",
                "source": "label_growth",
                "provenance": "scanner-flywheel",
                "confidence": 0.9,
            }
        ]
    }
    resp = client.post("/api/v1/labels", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["inserted"] == 1
    assert body["addresses"] == ["0xdeadbeef00000000000000000000000000000001"]
    inserted = client.captured["inserted"][-1]  # type: ignore[attr-defined]
    assert inserted[0]["entity_type"] == "malicious"


def test_post_labels_validation_rejects_short_address(client: TestClient) -> None:
    resp = client.post("/api/v1/labels", json={"labels": [{"address": "0x1", "chain": "ethereum"}]})
    assert resp.status_code == 422


def test_post_labels_store_unavailable_503(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("api.routers.labels.insert_entity_labels", lambda labels: 0)
    resp = client.post(
        "/api/v1/labels",
        json={"labels": [{"address": "0x" + "a" * 40, "chain": "ethereum"}]},
    )
    assert resp.status_code == 503


def test_post_labels_disabled_503(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("api.routers.labels.LABEL_API_WRITE_ENABLED", False)
    resp = client.post(
        "/api/v1/labels",
        json={"labels": [{"address": "0x" + "a" * 40, "chain": "ethereum"}]},
    )
    assert resp.status_code == 503


def test_post_labels_token_guard(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("api.routers.labels.LABEL_API_TOKEN", "s3cret-token")
    payload = {"labels": [{"address": "0x" + "b" * 40, "chain": "ethereum"}]}
    unauth = client.post("/api/v1/labels", json=payload)
    assert unauth.status_code == 401
    ok = client.post(
        "/api/v1/labels",
        json=payload,
        headers={"Authorization": "Bearer s3cret-token"},
    )
    assert ok.status_code == 201
