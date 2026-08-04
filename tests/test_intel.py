"""Unit tests for the wallet intelligence modules (fake ClickHouse).

The intel modules depend only on the ``ChQuery`` callable contract, so a
fake query router is enough to exercise entity resolution, risk scoring,
clustering, and anomaly detection without a live warehouse.
"""

from __future__ import annotations

from typing import Any

import pytest

from lite_indexer.intel import BehaviorClusterer, DataSources, EntityResolver, IntelReport, RiskScorer


class FakeVectorStore:
    """Minimal LanceVectorStore stand-in for similarity tests."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    async def similar_by_id(
        self, collection: str, row_id: str, k: int = 10, filter: str | None = None
    ) -> list[dict[str, Any]]:
        return [dict(r) for r in self.rows[:k]]


class FakeCh:
    """Routes SQL to canned rows by table + distinguishing keywords."""

    def __init__(
        self,
        labels: list[dict[str, Any]] | None = None,
        scams: list[dict[str, Any]] | None = None,
        profiles: list[dict[str, Any]] | None = None,
        entities: list[dict[str, Any]] | None = None,
        deploys: list[dict[str, Any]] | None = None,
        funders: list[dict[str, Any]] | None = None,
        shared: list[dict[str, Any]] | None = None,
        stats: dict[str, Any] | None = None,
        chain_presence: list[str] | None = None,
        anomalous_deployers: list[dict[str, Any]] | None = None,
        anomalous_new: list[dict[str, Any]] | None = None,
    ) -> None:
        self.labels = labels or []
        self.scams = scams or []
        self.profiles = profiles or []
        self.entities = entities or []
        self.deploys = deploys or []
        self.funders = funders or []
        self.shared = shared or []
        self.stats = stats or {}
        self.chain_presence = chain_presence or []
        self.anomalous_deployers = anomalous_deployers or []
        self.anomalous_new = anomalous_new or []
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def __call__(self, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((sql, params))
        if "wallet_labels" in sql:
            return self.labels
        if "wallet_profiles" in sql:
            return self.profiles
        if "wallet_entities" in sql:
            return self.entities
        if "deployer_events d" in sql:  # anomalous_deployers (LEFT JOIN scam)
            return self.anomalous_deployers
        if "INTERVAL 2 DAY" in sql:  # anomalous_new_wallets
            return self.anomalous_new
        if "deployer_events" in sql:
            return self.deploys
        if "scam_addresses" in sql:
            return self.scams
        if "GROUP BY from_address" in sql:  # funders
            return self.funders
        if "GROUP BY to_address" in sql:  # shared_recipients
            return self.shared
        if "count() AS tx_count" in sql:  # wallet_stats
            return [self.stats]
        if "GROUP BY chain ORDER BY chain" in sql:  # chain_presence
            return [{"chain": c} for c in self.chain_presence]
        return []  # recent_transfers etc. default empty


@pytest.fixture()
def cex_db() -> FakeCh:
    """Warehouse state for a known CEX wallet (Binance-style)."""
    return FakeCh(
        labels=[
            {
                "address": "0xcex",
                "chain_id": "eth",
                "label_name": "Example Exchange 14",
                "label_category": "exchange",
                "source": "etherscan_combined",
                "is_sanctioned": 0,
                "label_subtype": "",
                "loaded_at": __import__("datetime").datetime(2026, 6, 1),
            }
        ],
        stats={
            "tx_count": 388500,
            "unique_tokens": 1200,
            "unique_senders": 50000,
            "unique_receivers": 60000,
            "first_seen": __import__("datetime").datetime(2023, 1, 1),
            "last_seen": __import__("datetime").datetime(2026, 8, 3),
        },
        chain_presence=["ethereum", "bsc"],
        funders=[
            {
                "from_address": "0xfund-a",
                "fund_txs": 10,
                "first_funded": __import__("datetime").datetime(2023, 1, 1),
                "last_funded": __import__("datetime").datetime(2026, 8, 1),
            }
        ],
    )


@pytest.fixture()
def scam_db() -> FakeCh:
    """Warehouse state for a scam/exploit wallet."""
    return FakeCh(
        scams=[
            {
                "address": "0xbad",
                "chain_id": "eth",
                "source": "wallet_labels:etherscan_combined",
                "threat_type": "exploiter",
                "confidence": 0.85,
                "first_seen_at": __import__("datetime").datetime(2026, 6, 23),
                "evidence": "Example Hack Alert 3128",
            }
        ],
        stats={
            "tx_count": 12,
            "unique_tokens": 3,
            "unique_senders": 1,
            "unique_receivers": 5,
            "first_seen": __import__("datetime").datetime(2026, 7, 1),
            "last_seen": __import__("datetime").datetime(2026, 7, 10),
        },
        chain_presence=["ethereum"],
    )


@pytest.fixture()
def fresh_deployer_db() -> FakeCh:
    """Warehouse state for a fresh cross-chain deployer (rug pattern)."""
    return FakeCh(
        deploys=[
            {
                "chain": "base",
                "block_number": 1,
                "block_timestamp": __import__("datetime").datetime(2026, 8, 3),
                "tx_hash": "0x" + "a" * 64,
                "deployer": "0xnew",
                "deployer_eoa": "0xnew",
                "token_address": "0xtok",
                "token0": "0xtok",
                "token1": "0xweth",
                "pair_address": "0xpair",
                "type": "pair",
                "factory": "0xfact",
            },
            {
                "chain": "bsc",
                "block_number": 2,
                "block_timestamp": __import__("datetime").datetime(2026, 8, 4),
                "tx_hash": "0x" + "b" * 64,
                "deployer": "0xnew",
                "deployer_eoa": "0xnew",
                "token_address": "0xtok2",
                "token0": "0xtok2",
                "token1": "0xwbnb",
                "pair_address": "0xpair2",
                "type": "pair",
                "factory": "0xfact",
            },
        ],
        stats={
            "tx_count": 4,
            "unique_tokens": 2,
            "unique_senders": 1,
            "unique_receivers": 2,
            "first_seen": __import__("datetime").datetime(2026, 8, 3),
            "last_seen": __import__("datetime").datetime(2026, 8, 4),
        },
        chain_presence=["base", "bsc"],
        funders=[
            {
                "from_address": "0xmixer",
                "fund_txs": 1,
                "first_funded": __import__("datetime").datetime(2026, 8, 3),
                "last_funded": __import__("datetime").datetime(2026, 8, 3),
            }
        ],
    )


# ── Entity resolution ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_cex_wallet(cex_db: FakeCh) -> None:
    resolver = EntityResolver(DataSources(query=cex_db))
    doc = await resolver.resolve("0xCEX", chain="ethereum")
    assert doc["type"] == "cex"
    assert doc["confidence"] == "HIGH"
    assert doc["name"] == "Example Exchange 14"
    assert any(tag == "category:exchange" for tag in doc["tags"])


@pytest.mark.asyncio
async def test_resolve_scam_wallet(scam_db: FakeCh) -> None:
    resolver = EntityResolver(DataSources(query=scam_db))
    doc = await resolver.resolve("0xBAD", chain="ethereum")
    assert doc["type"] == "hack"  # threat_type exploiter -> hack
    assert doc["confidence"] == "HIGH"
    assert doc["threats"][0]["type"] == "exploiter"


@pytest.mark.asyncio
async def test_resolve_unknown() -> None:
    # No labels, scams, profiles, or deploys -> unknown/low.
    resolver = EntityResolver(DataSources(query=FakeCh()))
    doc = await resolver.resolve("0xdeadbeef", chain="ethereum")
    assert doc["type"] == "unknown"
    assert doc["confidence"] == "LOW"


@pytest.mark.asyncio
async def test_resolve_deployer(fresh_deployer_db: FakeCh) -> None:
    resolver = EntityResolver(DataSources(query=fresh_deployer_db))
    doc = await resolver.resolve("0xnew", chain=None)
    assert doc["type"] == "deployer"
    assert doc["deployments"] == 2


# ── Risk scoring ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_risk_cex_low(cex_db: FakeCh) -> None:
    scorer = RiskScorer(DataSources(query=cex_db))
    doc = await scorer.score("0xCEX", chain="ethereum")
    assert doc["level"] == "low"
    assert doc["score"] < 25.0
    assert doc["factors"][0]["name"] == "known_scam_taint"
    assert doc["factors"][0]["score"] == 0.0


@pytest.mark.asyncio
async def test_risk_scam_high(scam_db: FakeCh) -> None:
    scorer = RiskScorer(DataSources(query=scam_db))
    doc = await scorer.score("0xBAD", chain="ethereum")
    assert doc["level"] in ("high", "critical")
    assert doc["score"] >= 75.0  # floored by direct scam-database match
    known = next(f for f in doc["factors"] if f["name"] == "known_scam_taint")
    assert known["score"] == 100.0


@pytest.mark.asyncio
async def test_risk_fresh_deployer_cross_chain(fresh_deployer_db: FakeCh) -> None:
    scorer = RiskScorer(DataSources(query=fresh_deployer_db))
    doc = await scorer.score("0xnew", chain=None)
    rug = next(f for f in doc["factors"] if f["name"] == "deployer_rug_pattern")
    assert rug["score"] >= 100.0  # cross-chain deploys
    age = next(f for f in doc["factors"] if f["name"] == "wallet_age")
    assert age["score"] >= 100.0  # brand-new wallet
    # cross-chain rug (15) + young age (10) -> elevated, never "low"
    assert doc["score"] >= 25.0
    assert doc["level"] in ("medium", "high", "critical")
    assert any("deployer_rug_pattern" in flag or "wallet_age" in flag for flag in doc["flags"])


@pytest.mark.asyncio
async def test_risk_mixer_funding(fresh_deployer_db: FakeCh) -> None:
    # funder 0xmixer is labeled a mixer.
    fresh_deployer_db.labels = [
        {
            "address": "0xmixer",
            "chain_id": "eth",
            "label_name": "Tornado.Cash: 10",
            "label_category": "mixer",
            "source": "etherscan_combined",
            "is_sanctioned": 0,
            "label_subtype": "",
            "loaded_at": __import__("datetime").datetime(2026, 1, 1),
        }
    ]
    scorer = RiskScorer(DataSources(query=fresh_deployer_db))
    doc = await scorer.score("0xnew", chain=None)
    mixer = next(f for f in doc["factors"] if f["name"] == "mixer_interaction")
    assert mixer["score"] >= 80.0


# ── Clustering ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cluster_shared_funding(cex_db: FakeCh) -> None:
    cex_db.shared = [
        {
            "to_address": "0xsibling",
            "txs": 5,
            "first_funded": __import__("datetime").datetime(2026, 1, 1),
            "last_funded": __import__("datetime").datetime(2026, 8, 1),
        }
    ]
    resolver = EntityResolver(DataSources(query=cex_db))
    cluster = await resolver.cluster_addresses("0xCEX", chain="ethereum")
    assert any(item["address"] == "0xsibling" for item in cluster)
    sibling = next(item for item in cluster if item["address"] == "0xsibling")
    assert sibling["heuristic_type"] == "shared_funding"


@pytest.mark.asyncio
async def test_cluster_precomputed_entities(cex_db: FakeCh) -> None:
    cex_db.entities = [
        {
            "entity_id": "e1",
            "wallet_address": "0xcex",
            "chain_id": "eth",
            "heuristic_type": "shared_funding",
            "confidence_score": 0.9,
            "entity_label": "",
            "entity_category": "cex",
        },
        {
            "entity_id": "e1",
            "wallet_address": "0xcex-sister",
            "chain_id": "eth",
            "heuristic_type": "label_match",
            "confidence_score": 0.95,
            "entity_label": "",
            "entity_category": "cex",
        },
    ]
    resolver = EntityResolver(DataSources(query=cex_db))
    cluster = await resolver.cluster_addresses("0xCEX", chain="ethereum")
    sister = next(item for item in cluster if item["address"] == "0xcex-sister")
    assert sister["heuristic_type"] == "label_match"
    assert sister["confidence"] == 0.95


@pytest.mark.asyncio
async def test_cluster_behavioral_similarity() -> None:
    fake = FakeCh()
    store = FakeVectorStore(
        [
            {"id": "0xpeer", "chain": "ethereum", "_distance": 0.05, "text": "similar behavior"},
            {"id": "0xfar", "chain": "base", "_distance": 0.9, "text": "different"},
        ]
    )
    resolver = EntityResolver(DataSources(query=fake), vector_store=store)
    cluster = await resolver.cluster_addresses("0xme", chain=None)
    assert any(item["address"] == "0xpeer" for item in cluster)
    assert cluster[0]["heuristic_type"] == "behavioral_similarity"


# ── Behavior clusterer ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_find_similar_wallets() -> None:
    fake = FakeCh()
    store = FakeVectorStore([{"id": "0xpeer", "chain": "ethereum", "_distance": 0.1, "text": "trader"}])
    clusterer = BehaviorClusterer(DataSources(query=fake), vector_store=store)
    similar = await clusterer.find_similar_wallets("0xme", k=5)
    assert similar[0]["address"] == "0xpeer"
    assert similar[0]["distance"] == 0.1


@pytest.mark.asyncio
async def test_find_similar_wallets_no_store() -> None:
    clusterer = BehaviorClusterer(DataSources(query=FakeCh()))
    similar = await clusterer.find_similar_wallets("0xme")
    assert "error" in similar[0]


@pytest.mark.asyncio
async def test_detect_anomalies_merges_scans() -> None:
    db = FakeCh(
        anomalous_deployers=[
            {
                "chain": "base",
                "address": "0xtainted-deployer",
                "deploys": 3,
                "last_deploy": __import__("datetime").datetime(2026, 8, 4),
                "reason": "self_tainted",
            }
        ],
        anomalous_new=[
            {
                "chain": "ethereum",
                "address": "0xhot-new",
                "txs": 42,
                "counterparties": 8,
                "first_seen": __import__("datetime").datetime(2026, 8, 3),
                "last_seen": __import__("datetime").datetime(2026, 8, 4),
            }
        ],
    )
    clusterer = BehaviorClusterer(DataSources(query=db))
    anomalies = await clusterer.detect_anomalies(chain="ethereum")
    types = {a["anomaly_type"] for a in anomalies}
    assert "tainted_deployer" in types
    assert "new_wallet_volume" in types
    # Tainted deployer sorts first.
    assert anomalies[0]["anomaly_type"] == "tainted_deployer"


# ── Full report ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_report_shape(cex_db: FakeCh) -> None:
    report = IntelReport(DataSources(query=cex_db))
    doc = await report.full_report("0xCEX", chain="ethereum")
    assert doc["address"] == "0xcex"
    assert doc["entity"]["type"] == "cex"
    assert doc["risk"]["level"] == "low"
    assert doc["cluster"]["count"] == 0
    assert "similar_wallets" in doc
    assert doc["activity"]["tx_count"] == 388500
    assert "sources_checked" in doc
