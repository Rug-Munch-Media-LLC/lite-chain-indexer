"""
Wallet risk scoring — rug detection, scam probability, exploit taint.

The scorer returns a 0-100 risk score built from eight weighted factors
(weights in :mod:`lite_indexer.intel.config`)::

    score = Σ(weight_i × factor_i)

    level = low (0-25) / medium (25-50) / high (50-75) / critical (75-100)

Factors:

1. ``known_scam_taint`` — the wallet itself is listed in
   ``scam_addresses`` or carries a high-risk label category.
2. ``mixer_interaction`` — the wallet is a mixer, or was funded by a
   mixer / Tornado Cash-style address.
3. ``exploit_funding`` — a first-hop funder is tagged exploiter/hack/
   phish in ``scam_addresses``.
4. ``deployer_rug_pattern`` — cross-chain deploys and/or many recent
   contract deployments (the classic rug factory signature).
5. ``sanctioned_interaction`` — OFAC ``is_sanctioned`` flag on the
   wallet or its funder.
6. ``wallet_age`` — brand-new wallets are higher risk.
7. ``precomputed`` — corroboration from ``wallet_profiles.risk_score``
   (the deployer-risk pipeline).
8. ``activity_anomaly`` — unusually high transaction volume for a
   very young wallet (pump-and-dump behavior).

Every factor returns a ``{"score", "evidence"}`` pair so reports are
explainable; the ``flags`` list carries the human-readable reasons.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from lite_indexer.intel.config import (
    AGE_SUBSCORES,
    EXPLOIT_THREAT_TYPES,
    HIGH_RISK_LABEL_CATEGORIES,
    INTEL_MAX_FUNDERS,
    RISK_LEVELS,
    RISK_WEIGHTS,
)
from lite_indexer.intel.sources import DataSources

logger = logging.getLogger(__name__)

# Funders whose label matches these are treated as mixer funding.
_MIXER_KEYWORDS = ("tornado", "mixer", "railgun", "wasabi", "coinjoin")


class RiskScorer:
    """Wallet risk scoring — rug detection, scam probability."""

    def __init__(self, sources: DataSources) -> None:
        """Build a scorer over the given data sources."""
        self.sources = sources

    async def score(self, address: str, chain: str | None = None) -> dict[str, Any]:
        """Score ``address`` and return the full risk breakdown.

        Returns::

            {
              "address", "chain", "score", "level",
              "factors": [{"name", "weight", "score", "contribution", "evidence"}],
              "flags": [...],
              "sources_used": [...],
            }
        """
        addr = address.strip().lower()
        scams = self.sources.local.scam_lookup(addr, chain)
        labels = self.sources.local.labels(addr, chain)
        profile = self.sources.local.profile(addr, chain)
        deploys = self.sources.local.deployments(addr, chain, limit=20)
        funders = self.sources.local.funders(addr, chain)

        # Enrich funders once (labels + scam matches) for funding factors.
        funder_meta = self._funder_metadata(funders, chain)

        factors: dict[str, dict[str, Any]] = {}

        # 1. Known scam taint ─────────────────────────────────────────
        scam_evidence: list[str] = []
        scam_score = 0.0
        for s in scams:
            threat = str(s.get("threat_type") or "")
            scam_evidence.append(f"{s.get('source')}: {threat} ({s.get('evidence')})")
        for label in labels:
            category = str(label.get("label_category") or "").lower()
            if category in HIGH_RISK_LABEL_CATEGORIES:
                scam_evidence.append(f"label category {category}: {label.get('label_name')}")
        if scam_evidence:
            scam_score = 100.0
        factors["known_scam_taint"] = {
            "score": scam_score,
            "evidence": scam_evidence[:5],
        }

        # 2. Mixer interaction ────────────────────────────────────────
        mixer_evidence: list[str] = []
        mixer_score = 0.0
        for label in labels:
            if str(label.get("label_category") or "").lower() == "mixer":
                mixer_score = max(mixer_score, 100.0)
                mixer_evidence.append(f"labeled mixer: {label.get('label_name')}")
        for funder, meta in funder_meta.items():
            if meta["mixer"]:
                mixer_score = max(mixer_score, 80.0)
                mixer_evidence.append(f"funded by mixer-labeled {funder}")
        factors["mixer_interaction"] = {"score": mixer_score, "evidence": mixer_evidence[:5]}

        # 3. Exploit funding ──────────────────────────────────────────
        exploit_evidence: list[str] = []
        exploit_score = 0.0
        for funder, meta in funder_meta.items():
            if meta["exploit"]:
                exploit_score = max(exploit_score, 100.0)
                exploit_evidence.append(f"funded by {funder} tagged {meta['threats'][:2]}")
        factors["exploit_funding"] = {"score": exploit_score, "evidence": exploit_evidence[:5]}

        # 4. Deployer rug pattern ─────────────────────────────────────
        rug_evidence: list[str] = []
        rug_score = 0.0
        if deploys:
            chains = sorted({str(d.get("chain")) for d in deploys if d.get("chain")})
            if len(chains) > 1:
                rug_score = max(rug_score, 100.0)
                rug_evidence.append(f"deployed on multiple chains: {', '.join(chains)}")
            if len(deploys) >= 5:
                rug_score = max(rug_score, 80.0)
                rug_evidence.append(f"{len(deploys)} deployments")
            if len(chains) == 1 and len(deploys) < 5:
                rug_score = max(rug_score, 40.0)
                rug_evidence.append(f"{len(deploys)} deployment(s) on {chains[0]}")
        factors["deployer_rug_pattern"] = {"score": rug_score, "evidence": rug_evidence[:5]}

        # 5. Sanctioned interaction ───────────────────────────────────
        sanction_evidence: list[str] = []
        sanction_score = 0.0
        for label in labels:
            if bool(label.get("is_sanctioned")) or str(label.get("label_category") or "").lower() == "sanctioned":
                sanction_score = max(sanction_score, 100.0)
                sanction_evidence.append(f"sanctioned label: {label.get('label_name')}")
        for funder, meta in funder_meta.items():
            if meta["sanctioned"]:
                sanction_score = max(sanction_score, 80.0)
                sanction_evidence.append(f"funded by sanctioned {funder}")
        factors["sanctioned_interaction"] = {
            "score": sanction_score,
            "evidence": sanction_evidence[:5],
        }

        # 6. Wallet age ───────────────────────────────────────────────
        first_seen = self._first_seen(addr, chain, profile, deploys)
        age_evidence: list[str] = []
        age_score = 0.0
        if first_seen is not None:
            days = max(0.0, (datetime.now(UTC) - first_seen).total_seconds() / 86400.0)
            for bucket_days, sub in AGE_SUBSCORES:
                if days < bucket_days:
                    age_score = sub
                    age_evidence.append(f"wallet age {days:.0f} days")
                    break
        else:
            age_score = 100.0
            age_evidence.append("no on-chain activity observed (unknown age)")
        factors["wallet_age"] = {"score": age_score, "evidence": age_evidence[:2]}

        # 7. Precomputed profile ──────────────────────────────────────
        precomputed_evidence: list[str] = []
        precomputed_score = 0.0
        if profile and profile.get("risk_score") is not None:
            raw = float(profile["risk_score"])
            precomputed_score = min(100.0, max(0.0, raw * 100.0))
            precomputed_evidence.append(
                f"wallet_profiles risk_level={profile.get('risk_level')} entity={profile.get('entity_category')}"
            )
        factors["precomputed"] = {"score": precomputed_score, "evidence": precomputed_evidence[:2]}

        # 8. Activity anomaly ─────────────────────────────────────────
        anomaly_evidence: list[str] = []
        anomaly_score = 0.0
        stats = self.sources.local.wallet_stats(addr, chain)
        tx_count = int(stats.get("tx_count") or 0)
        if first_seen is not None and tx_count > 0:
            days = max(0.01, (datetime.now(UTC) - first_seen).total_seconds() / 86400.0)
            txs_per_day = tx_count / days
            if days < 2.0 and txs_per_day > 20:
                anomaly_score = 100.0
                anomaly_evidence.append(f"new wallet with {tx_count} txs in {days:.1f} days")
            elif days < 7.0 and txs_per_day > 10:
                anomaly_score = 70.0
                anomaly_evidence.append(f"young wallet with {tx_count} txs")
        factors["activity_anomaly"] = {"score": anomaly_score, "evidence": anomaly_evidence[:2]}

        # ── Aggregate ────────────────────────────────────────────────
        total = 0.0
        ordered: list[dict[str, Any]] = []
        for name, weight in RISK_WEIGHTS.items():
            factor = factors.get(name, {"score": 0.0, "evidence": []})
            contribution = weight * factor["score"]
            total += contribution
            ordered.append(
                {
                    "name": name,
                    "weight": weight,
                    "score": round(factor["score"], 1),
                    "contribution": round(contribution, 1),
                    "evidence": factor["evidence"],
                }
            )

        level = _risk_level(total)
        flags = _collect_flags(factors, level)

        # Hard floor: wallet listed as scam/exploiter is never "low".
        if scam_score >= 100.0 and level in ("low", "medium"):
            total = max(total, 75.0)
            level = _risk_level(total)
            flags.append("risk floored by direct scam-database match")

        return {
            "address": addr,
            "chain": chain,
            "score": round(total, 1),
            "level": level,
            "factors": ordered,
            "flags": flags,
            "sources_used": sorted(
                {"scam_addresses", "wallet_labels", "wallet_profiles", "deployer_events", "evm_transfers"}
            ),
        }

    # ── Helpers ──────────────────────────────────────────────────────

    def _funder_metadata(self, funders: list[dict[str, Any]], chain: str | None) -> dict[str, dict[str, Any]]:
        """Enrich the top funders with mixer/exploit/sanctioned flags."""
        meta: dict[str, dict[str, Any]] = {}
        for funder in funders[:INTEL_MAX_FUNDERS]:
            funder_addr = str(funder.get("from_address") or "")
            if not funder_addr:
                continue
            labels = self.sources.local.labels(funder_addr, chain, limit=10)
            scams = self.sources.local.scam_lookup(funder_addr, chain, limit=5)
            lower_names = " ".join(
                str(label.get("label_name") or "") + " " + str(label.get("label_category") or "")
                for label in labels
            ).lower()
            threats = [str(s.get("threat_type") or "") for s in scams]
            meta[funder_addr] = {
                "mixer": any(k in lower_names for k in _MIXER_KEYWORDS),
                "exploit": any(t in EXPLOIT_THREAT_TYPES for t in threats),
                "sanctioned": any(bool(label.get("is_sanctioned")) for label in labels)
                or any(str(label.get("label_category") or "").lower() == "sanctioned" for label in labels),
                "threats": threats[:3],
            }
        return meta

    def _first_seen(
        self,
        address: str,
        chain: str | None,
        profile: dict[str, Any] | None,
        deploys: list[dict[str, Any]],
    ) -> datetime | None:
        """Earliest observed activity across transfer/profile/deploy data."""
        candidates: list[datetime] = []
        stats = self.sources.local.wallet_stats(address, chain)
        parsed = _parse_dt(stats.get("first_seen"))
        if parsed:
            candidates.append(parsed)
        if profile:
            parsed = _parse_dt(profile.get("first_seen_at"))
            if parsed and parsed.year > 2000:  # 1970 default rows are noise
                candidates.append(parsed)
        for d in deploys:
            parsed = _parse_dt(d.get("block_timestamp"))
            if parsed:
                candidates.append(parsed)
        return min(candidates) if candidates else None


def _parse_dt(value: Any) -> datetime | None:
    """Parse a datetime from ClickHouse row values (datetime or str)."""
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed
    except ValueError:
        return None


def _risk_level(score: float) -> str:
    """Map a 0-100 score to a risk level."""
    for threshold, level in RISK_LEVELS:
        if score >= threshold:
            return level
    return "low"


def _collect_flags(factors: dict[str, dict[str, Any]], level: str) -> list[str]:
    """Human-readable flags derived from factor evidence."""
    flags: list[str] = []
    for name, factor in factors.items():
        if factor["score"] >= 80.0:
            flags.append(f"{name}: {factor['evidence'][0]}" if factor["evidence"] else name)
    if not flags:
        flags.append(f"no high-risk signals ({level})")
    return flags
