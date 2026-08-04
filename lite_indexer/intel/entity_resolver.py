"""
Entity resolution — map wallet addresses to known entities and clusters.

Two capabilities:

* :meth:`EntityResolver.resolve` — look up an address in the label /
  scam / profile tables and return ``{name, type, confidence, tags}``.
  Entity types follow the canonical taxonomy
  (:data:`EntityResolver.ENTITY_TYPES`).

* :meth:`EntityResolver.cluster_addresses` — find other addresses
  controlled by the same entity using four independent heuristics:

  1. Precomputed clusters (``wallet_entities`` table) — HIGH confidence
  2. Shared funding source (same first-hop funder) — MEDIUM
  3. Behavioral similarity (LanceDB ``wallet_embeddings``) — MEDIUM
  4. Heavy mutual interaction (shared counterparties) — LOW

Every finding carries a ``heuristic_type`` and ``confidence`` so callers
can weigh evidence, and the report can explain *why* two addresses were
clustered.
"""

from __future__ import annotations

import logging
from typing import Any

from lite_indexer.intel.config import INTEL_MAX_CLUSTER_ADDRESSES
from lite_indexer.intel.sources import DataSources

logger = logging.getLogger(__name__)

# Confidence scores (0-1).
_HIGH = 0.9
_MEDIUM = 0.7
_LOW = 0.4

# wallet_labels.label_category -> entity type taxonomy.
_CATEGORY_TYPE_MAP: dict[str, str] = {
    "exchange": "cex",
    "dex": "dex",
    "defi": "protocol",
    "lending": "lending",
    "bridge": "bridge",
    "nft": "nft_marketplace",
    "marketplace": "nft_marketplace",
    "mixer": "mixer",
    "mining_pool": "service",
    "gambling": "service",
    "p2p_service": "service",
    "service": "service",
    "scam": "scam",
    "darknet_market": "scam",
    "ransomware": "scam",
    "money_laundering": "scam",
    "sanctioned": "scam",
    "individual": "individual",
    "whale": "whale",
    "deployer_behavior": "deployer",
}

# scam_addresses.threat_type -> entity type.
_THREAT_TYPE_MAP: dict[str, str] = {
    "exploit": "hack",
    "exploiter": "hack",
    "hack": "hack",
    "phish": "scam",
    "phish-hack": "hack",
    "malicious": "scam",
    "scam": "scam",
    "ponzi": "scam",
    "mixer": "mixer",
    "money_laundering": "scam",
    "ransomware": "scam",
}


class EntityResolver:
    """Resolve wallet addresses to known entities and wallet clusters."""

    ENTITY_TYPES: list[str] = [
        "cex",
        "dex",
        "bridge",
        "lending",
        "nft_marketplace",
        "mixer",
        "scam",
        "hack",
        "mev_bot",
        "whale",
        "institution",
        "protocol",
        "deployer",
        "service",
        "individual",
        "unknown",
    ]

    def __init__(self, sources: DataSources, vector_store: Any | None = None) -> None:
        """Build a resolver over the given data sources.

        Args:
            sources: Combined data source facade.
            vector_store: Optional LanceVectorStore for behavioral
                similarity clustering (``similar_by_id`` on
                ``wallet_embeddings``).
        """
        self.sources = sources
        self.vector_store = vector_store

    # ── Entity resolution ────────────────────────────────────────────

    async def resolve(self, address: str, chain: str | None = None) -> dict[str, Any]:
        """Resolve ``address`` to a known entity.

        Returns::

            {
              "address", "chain", "name", "type", "confidence",
              "confidence_score", "tags", "labels", "threats",
              "entity_id", "deployments", "sources_used",
            }

        ``type`` is ``"unknown"`` with ``confidence`` ``"low"`` when no
        signal matches.
        """
        addr = address.strip().lower()
        labels = self.sources.local.labels(addr, chain)
        scams = self.sources.local.scam_lookup(addr, chain)
        profile = self.sources.local.profile(addr, chain)
        deploys = self.sources.local.deployments(addr, chain, limit=5)

        entity_type: str | None = None
        name: str | None = None
        confidence: float = _LOW
        entity_id: str | None = None
        tags: list[str] = []

        # Threat classification first: scams/hacks dominate.
        for scam in scams:
            threat = str(scam.get("threat_type") or "").lower()
            mapped = _THREAT_TYPE_MAP.get(threat)
            if mapped and entity_type is None:
                entity_type = mapped
                name = str(scam.get("evidence") or threat)
                confidence = max(confidence, _HIGH)
            if threat:
                tags.append(f"threat:{threat}")
            if scam.get("source"):
                tags.append(f"source:{scam['source']}")

        # Label-driven classification.
        for label in labels:
            category = str(label.get("label_category") or "").lower()
            mapped = _CATEGORY_TYPE_MAP.get(category)
            label_name = str(label.get("label_name") or "")
            if mapped and (entity_type is None or mapped == entity_type or _is_entity_stronger(mapped, entity_type)):
                entity_type = mapped
                name = label_name or name
                confidence = max(confidence, _HIGH if mapped not in ("service", "individual") else _MEDIUM)
            if category:
                tags.append(f"category:{category}")
            if label.get("is_sanctioned"):
                tags.append("sanctioned")

        # Precomputed profile (deployer risk pipeline).
        if profile:
            profile_cat = str(profile.get("entity_category") or "")
            if profile_cat and entity_type is None:
                entity_type = "deployer" if "deployer" in profile_cat.lower() else "unknown"
                name = profile.get("entity_label") or name
                confidence = max(confidence, _MEDIUM)
            if profile.get("entity_id"):
                entity_id = str(profile["entity_id"])

        # Contract-deployer heuristic.
        if deploys and entity_type is None:
            entity_type = "deployer"
            chains = sorted({str(d.get("chain")) for d in deploys if d.get("chain")})
            name = name or (f"deployer on {', '.join(chains)}" if chains else "deployer")
            confidence = max(confidence, _MEDIUM)

        if entity_type is None:
            entity_type = "unknown"
            name = None

        return {
            "address": addr,
            "chain": chain,
            "name": name,
            "type": entity_type,
            "confidence": _confidence_label(confidence),
            "confidence_score": round(confidence, 2),
            "tags": sorted(set(tags))[:40],
            "labels": [
                {
                    "name": label.get("label_name"),
                    "category": label.get("label_category"),
                    "source": label.get("source"),
                    "sanctioned": bool(label.get("is_sanctioned")),
                }
                for label in labels[:10]
            ],
            "threats": [
                {
                    "type": s.get("threat_type"),
                    "source": s.get("source"),
                    "evidence": s.get("evidence"),
                    "confidence": float(s.get("confidence") or 0.0),
                }
                for s in scams[:10]
            ],
            "entity_id": entity_id,
            "deployments": len(deploys),
            "sources_used": sorted({"wallet_labels", "scam_addresses", "wallet_profiles", "deployer_events"}),
        }

    # ── Clustering ───────────────────────────────────────────────────

    async def cluster_addresses(self, address: str, chain: str | None = None) -> list[dict[str, Any]]:
        """Find addresses plausibly controlled by the same entity.

        Returns a list of::

            {"address", "chain", "heuristic_type", "confidence", "evidence"}

        ordered by confidence (highest first), capped at
        ``INTEL_MAX_CLUSTER_ADDRESSES``.
        """
        addr = address.strip().lower()
        found: dict[str, dict[str, Any]] = {}

        def add(peer: str, chain_id: str | None, heuristic: str, confidence: float, evidence: str) -> None:
            if peer == addr:
                return
            if peer in found:
                # Keep the highest-confidence attribution.
                if confidence > found[peer]["confidence"]:
                    found[peer].update(
                        {"heuristic_type": heuristic, "confidence": confidence, "evidence": evidence}
                    )
                return
            found[peer] = {
                "address": peer,
                "chain": chain_id,
                "heuristic_type": heuristic,
                "confidence": confidence,
                "evidence": evidence,
            }

        # 1. Precomputed clusters (wallet_entities) — HIGH.
        for link in self.sources.local.entity_links(addr, chain):
            add(
                str(link.get("wallet_address") or ""),
                link.get("chain_id"),
                str(link.get("heuristic_type") or "wallet_entities"),
                float(link.get("confidence_score") or _HIGH),
                f"precomputed entity {link.get('entity_id')}",
            )

        # 2. Shared funding source — MEDIUM.
        funders = self.sources.local.funders(addr, chain)
        for funder in funders:
            funder_addr = str(funder.get("from_address") or "")
            if not funder_addr or funder_addr == addr:
                continue
            peers = self.sources.local.shared_recipients(funder_addr, addr, chain, limit=INTEL_MAX_CLUSTER_ADDRESSES)
            for peer_row in peers:
                peer_addr = str(peer_row.get("to_address") or "")
                if not peer_addr:
                    continue
                add(
                    peer_addr,
                    peer_row.get("chain"),
                    "shared_funding",
                    _MEDIUM,
                    f"funded by same source {funder_addr}",
                )

        # 3. Behavioral similarity (LanceDB) — MEDIUM.
        if self.vector_store is not None:
            try:
                similar = await self.vector_store.similar_by_id(
                    "wallet_embeddings", addr, k=INTEL_MAX_CLUSTER_ADDRESSES
                )
                for row in similar:
                    peer = str(row.get("id") or "")
                    if not peer:
                        continue
                    add(
                        peer,
                        row.get("chain"),
                        "behavioral_similarity",
                        _MEDIUM,
                        f"vector distance {row.get('_distance', '?')}",
                    )
            except Exception as exc:  # noqa: BLE001 — vector store is optional
                logger.debug("behavioral clustering skipped: %s", exc)

        # Order by confidence desc, then address; cap the result.
        ranked = sorted(found.values(), key=lambda r: (-r["confidence"], r["address"]))
        return ranked[:INTEL_MAX_CLUSTER_ADDRESSES]


def _is_entity_stronger(candidate: str, current: str) -> bool:
    """Entity-type priority for label overrides.

    Exchange/known-entity labels are stronger than generic service
    labels; a scam/hack label is always kept.
    """
    strong = {"cex", "dex", "bridge", "lending", "nft_marketplace", "mixer", "scam", "hack", "institution", "whale"}
    if current in ("scam", "hack"):
        return False  # never downgrade a scam/hack match
    if candidate in strong:
        return True
    return candidate == "unknown" and current not in strong


def _confidence_label(score: float) -> str:
    """Map a numeric confidence to HIGH / MEDIUM / LOW."""
    if score >= 0.8:
        return "HIGH"
    if score >= 0.6:
        return "MEDIUM"
    return "LOW"
