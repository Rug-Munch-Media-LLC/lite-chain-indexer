"""
Wallet intelligence — entity resolution, behavioral clustering, risk scoring.

Free, self-hosted wallet profiling comparable to commercial products
(Arkham-style entity labels, Nansen-style behavior tags). Local-first:
the primary data source is the indexer's own ClickHouse warehouse
(``wallet_labels``, ``scam_addresses``, ``wallet_profiles``,
``wallet_entities``, ``deployer_events``, ``evm_transfers``), enriched
best-effort from free public sources (GoPlus, Chainabuse, rekt.news,
CryptoscamDB).

Components:

* :class:`EntityResolver` — map addresses to known entities
  (CEX/DEX/bridge/mixer/scam/hack/…) and cluster addresses controlled
  by the same entity (shared funding, precomputed clusters, behavioral
  similarity).
* :class:`RiskScorer` — 0-100 wallet risk score with explainable factor
  breakdown (scam taint, mixer interaction, exploit funding, deployer
  rug patterns, sanctions, wallet age, activity anomalies).
* :class:`BehaviorClusterer` — LanceDB vector similarity for
  behaviorally similar wallets plus SQL-driven anomaly detection
  (tainted deployers, suspicious new-wallet volume).
* :class:`IntelReport` — assembles the full wallet intelligence report.

Usage::

    from lite_indexer.intel import IntelReport, DataSources

    # Pass the API's ClickHouse query function (or any ChQuery callable).
    report = IntelReport(DataSources(query=api_db_query))
    doc = await report.full_report("0x28c6...", "ethereum")

The package is dependency-light: ClickHouse access is behind the
``ChQuery`` callable contract, LanceDB is optional, and external HTTP
sources degrade gracefully when unreachable.
"""

from __future__ import annotations

from lite_indexer.intel.behavior_clusterer import BehaviorClusterer
from lite_indexer.intel.config import RISK_LEVELS, RISK_WEIGHTS, normalize_chain
from lite_indexer.intel.entity_resolver import EntityResolver
from lite_indexer.intel.report import IntelReport
from lite_indexer.intel.risk_scorer import RiskScorer
from lite_indexer.intel.sources import (
    ChainabuseSource,
    CryptoscamDBSource,
    DataSources,
    GoPlusSource,
    LocalIntelSource,
    RektNewsSource,
)

__all__ = [
    "DataSources",
    "LocalIntelSource",
    "GoPlusSource",
    "ChainabuseSource",
    "CryptoscamDBSource",
    "RektNewsSource",
    "EntityResolver",
    "RiskScorer",
    "BehaviorClusterer",
    "IntelReport",
    "normalize_chain",
    "RISK_WEIGHTS",
    "RISK_LEVELS",
]
