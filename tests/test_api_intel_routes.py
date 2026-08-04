"""Hermetic route tests for the wallet intelligence endpoints.

The IntelReport singleton is replaced with a fake so no ClickHouse or
LanceDB connection is needed. Covers the five endpoints, the 503 path
(when the report builder errors), the 422 empty-address path, and the
critical route-ordering guarantee: ``/alerts`` must not be captured by
``/{address}``.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.app import create_app


class FakeResolver:
    async def resolve(self, address: str, chain: str | None = None) -> dict[str, Any]:
        return {"address": address, "name": "Example Exchange", "type": "cex", "confidence": "HIGH", "tags": []}

    async def cluster_addresses(self, address: str, chain: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "address": "0xsibling",
                "chain": "ethereum",
                "heuristic_type": "shared_funding",
                "confidence": 0.7,
                "evidence": "funded by same source 0xfund",
            }
        ]


class FakeScorer:
    async def score(self, address: str, chain: str | None = None) -> dict[str, Any]:
        return {
            "address": address,
            "chain": chain,
            "score": 12.5,
            "level": "low",
            "factors": [{"name": "known_scam_taint", "weight": 0.25, "score": 0.0, "contribution": 0.0, "evidence": []}],
            "flags": ["no high-risk signals (low)"],
            "sources_used": ["wallet_labels"],
        }


class FakeClusterer:
    async def find_similar_wallets(self, address: str, k: int = 10) -> list[dict[str, Any]]:
        return [{"address": "0xpeer", "chain": "ethereum", "distance": 0.1, "text": "similar"}]

    async def detect_anomalies(self, chain: str | None = None) -> list[dict[str, Any]]:
        return [
            {
                "address": "0xtainted",
                "chain": chain or "ethereum",
                "anomaly_type": "tainted_deployer",
                "severity": "high",
                "evidence": "self_tainted",
            }
        ]


class FakeReport:
    resolver = FakeResolver()
    scorer = FakeScorer()
    clusterer = FakeClusterer()

    async def full_report(self, address: str, chain: str | None = None) -> dict[str, Any]:
        return {
            "address": address,
            "chain": chain,
            "generated_at": "2026-08-04T00:00:00Z",
            "entity": await self.resolver.resolve(address, chain),
            "risk": await self.scorer.score(address, chain),
            "cluster": {"count": 1, "addresses": await self.resolver.cluster_addresses(address, chain)},
            "similar_wallets": await self.clusterer.find_similar_wallets(address),
            "activity": {"tx_count": 100, "unavailable": False},
            "sources_checked": [],
        }


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """TestClient with the intel report singleton replaced by a fake."""
    app = create_app()
    monkeypatch.setattr("api.routers.intel.get_report", lambda: FakeReport())
    with TestClient(app) as c:
        yield c


def test_openapi_lists_intel_endpoints(client: TestClient) -> None:
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    paths = resp.json()["paths"]
    assert "/api/v1/intel/alerts" in paths
    assert "/api/v1/intel/{address}" in paths
    assert "/api/v1/intel/{address}/risk" in paths
    assert "/api/v1/intel/{address}/cluster" in paths
    assert "/api/v1/intel/{address}/similar" in paths


def test_full_report(client: TestClient) -> None:
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60?chain=ethereum")
    assert resp.status_code == 200
    body = resp.json()
    assert body["address"] == "0x28c6c06298d514db089934071355e5743bf21d60"
    assert body["entity"]["type"] == "cex"
    assert body["risk"]["level"] == "low"
    assert body["cluster"]["count"] == 1


def test_risk_endpoint(client: TestClient) -> None:
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60/risk")
    assert resp.status_code == 200
    assert resp.json()["score"] == 12.5


def test_cluster_endpoint(client: TestClient) -> None:
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60/cluster")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["addresses"][0]["address"] == "0xsibling"


def test_similar_endpoint(client: TestClient) -> None:
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60/similar")
    assert resp.status_code == 200
    assert resp.json()["similar"][0]["address"] == "0xpeer"


def test_alerts_endpoint_not_captured_as_address(client: TestClient) -> None:
    """Critical ordering check: /alerts must not hit the /{address} handler."""
    resp = client.get("/api/v1/intel/alerts?chain=ethereum")
    assert resp.status_code == 200
    body = resp.json()
    assert "anomalies" in body
    assert body["anomalies"][0]["anomaly_type"] == "tainted_deployer"
    # The full-report handler would return "entity" — prove it was not used.
    assert "entity" not in body


def test_empty_address_422(client: TestClient) -> None:
    resp = client.get("/api/v1/intel/")
    assert resp.status_code in (404, 422)  # trailing-slash redirect semantics


def test_similar_unavailable_503(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    class BrokenClusterer(FakeClusterer):
        async def find_similar_wallets(self, address: str, k: int = 10) -> list[dict[str, Any]]:
            return [{"error": "vector store unavailable"}]

    class BrokenReport(FakeReport):
        clusterer = BrokenClusterer()

    monkeypatch.setattr("api.routers.intel.get_report", lambda: BrokenReport())
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60/similar")
    assert resp.status_code == 503


def test_report_builder_error_503(monkeypatch: pytest.MonkeyPatch, client: TestClient) -> None:
    def boom() -> Any:
        from fastapi import HTTPException

        raise HTTPException(status_code=503, detail="intel unavailable: warehouse down")

    monkeypatch.setattr("api.routers.intel.get_report", boom)
    resp = client.get("/api/v1/intel/0x28c6c06298d514db089934071355e5743bf21d60")
    assert resp.status_code == 503
