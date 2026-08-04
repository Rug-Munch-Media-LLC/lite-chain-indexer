"""
Full wallet intelligence report assembler.

:class:`IntelReport` combines entity resolution, risk scoring, clustering,
behavioral similarity, and a raw activity summary into one JSON-serializable
report — the payload behind ``GET /api/v1/intel/{address}``.
"""

from __future__ import annotations

from datetime import UTC
from typing import Any

from lite_indexer.intel.behavior_clusterer import BehaviorClusterer
from lite_indexer.intel.config import INTEL_MAX_SIMILAR_WALLETS
from lite_indexer.intel.entity_resolver import EntityResolver
from lite_indexer.intel.risk_scorer import RiskScorer
from lite_indexer.intel.sources import DataSources


class IntelReport:
    """Assemble wallet intelligence reports from the intel modules."""

    def __init__(self, sources: DataSources, vector_store: Any | None = None) -> None:
        """Build a report assembler over the given data sources."""
        self.sources = sources
        self.resolver = EntityResolver(sources, vector_store=vector_store)
        self.scorer = RiskScorer(sources)
        self.clusterer = BehaviorClusterer(sources, vector_store=vector_store)

    async def full_report(self, address: str, chain: str | None = None) -> dict[str, Any]:
        """Return the complete intelligence report for ``address``."""
        addr = address.strip().lower()

        entity = await self.resolver.resolve(addr, chain)
        risk = await self.scorer.score(addr, chain)
        cluster = await self.resolver.cluster_addresses(addr, chain)
        similar = await self.clusterer.find_similar_wallets(addr, k=INTEL_MAX_SIMILAR_WALLETS)

        # Activity summary (never raises; empty stats degrade gracefully).
        activity: dict[str, Any] = {"unavailable": False}
        try:
            stats = self.sources.local.wallet_stats(addr, chain)
            activity = {
                "tx_count": int(stats.get("tx_count") or 0),
                "unique_tokens": int(stats.get("unique_tokens") or 0),
                "unique_senders": int(stats.get("unique_senders") or 0),
                "unique_receivers": int(stats.get("unique_receivers") or 0),
                "first_seen": _fmt_ts(stats.get("first_seen")),
                "last_seen": _fmt_ts(stats.get("last_seen")),
                "chains_active": self.sources.local.chain_presence(addr),
                "recent_transfers": _trim_transfers(
                    self.sources.local.recent_transfers(addr, chain, limit=10)
                ),
            }
        except Exception:  # noqa: BLE001 — activity is best-effort
            activity = {"unavailable": True}

        return {
            "address": addr,
            "chain": chain,
            "generated_at": _now_iso(),
            "entity": entity,
            "risk": risk,
            "cluster": {
                "count": len(cluster),
                "addresses": cluster,
            },
            "similar_wallets": similar,
            "activity": activity,
            "sources_checked": self.sources.availability(),
        }


def _trim_transfers(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep JSON-safe transfer fields only."""
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "chain": r.get("chain"),
                "block_timestamp": _fmt_ts(r.get("block_timestamp")),
                "tx_hash": r.get("tx_hash"),
                "from": r.get("from_address"),
                "to": r.get("to_address"),
                "token": r.get("token_symbol") or r.get("token_address"),
                "value": r.get("value"),
            }
        )
    return out


def _fmt_ts(value: Any) -> str:
    """Format a ClickHouse datetime value as ISO-8601 (best-effort)."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"


def _now_iso() -> str:
    """Current UTC time as ISO-8601."""
    from datetime import datetime

    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
