"""
Behavioral clustering and anomaly detection.

Two capabilities:

* :meth:`BehaviorClusterer.find_similar_wallets` — ANN search over the
  LanceDB ``wallet_embeddings`` collection. Wallets whose behavior
  embeddings are close to ``address`` are returned with their vector
  distance. Best-effort: when no vector store is available the method
  returns an error marker instead of raising.

* :meth:`BehaviorClusterer.detect_anomalies` — SQL-driven anomaly scans
  over the ClickHouse warehouse (no vectors needed):

  - ``anomalous_deployers`` — recent deployers whose own address or
    funder is tainted (exploit-class scam tag).
  - ``anomalous_new_wallets`` — wallets younger than 48h already moving
    unusual transfer volume.

Both scans are bounded by ``INTEL_ANOMALY_MAX_RESULTS`` and windowed to
``INTEL_ANOMALY_WINDOW_DAYS`` (deployers) / 2 days (new wallets).
"""

from __future__ import annotations

import logging
from typing import Any

from lite_indexer.intel.config import (
    INTEL_ANOMALY_MAX_RESULTS,
    INTEL_ANOMALY_MIN_NEW_WALLET_TXS,
    INTEL_ANOMALY_WINDOW_DAYS,
    INTEL_MAX_SIMILAR_WALLETS,
)
from lite_indexer.intel.sources import DataSources

logger = logging.getLogger(__name__)


class BehaviorClusterer:
    """Cluster wallets by behavior using LanceDB vector similarity."""

    def __init__(self, sources: DataSources, vector_store: Any | None = None) -> None:
        """Build a clusterer over the given data sources.

        Args:
            sources: Combined data source facade (anomaly scans).
            vector_store: Optional LanceVectorStore used for similarity.
        """
        self.sources = sources
        self.vector_store = vector_store

    async def find_similar_wallets(self, address: str, k: int = INTEL_MAX_SIMILAR_WALLETS) -> list[dict[str, Any]]:
        """Find wallets with similar behavior patterns.

        Uses ``wallet_embeddings`` ANN search anchored at ``address``.
        Returns rows with ``address``, ``distance``, ``text`` and
        metadata; when the vector store is unavailable returns a single
        ``{"error": ...}`` entry.
        """
        if self.vector_store is None:
            return [{"error": "vector store unavailable"}]
        try:
            rows = await self.vector_store.similar_by_id(
                "wallet_embeddings", address.strip().lower(), k=k
            )
            out: list[dict[str, Any]] = []
            for row in rows:
                out.append(
                    {
                        "address": row.get("id"),
                        "chain": row.get("chain"),
                        "distance": row.get("_distance"),
                        "text": row.get("text"),
                        "labels": row.get("labels"),
                        "tx_count": row.get("tx_count"),
                        "first_seen": row.get("first_seen"),
                    }
                )
            return out
        except Exception as exc:  # noqa: BLE001 — vector store is optional
            logger.debug("similar-wallet search failed for %s: %s", address, exc)
            return [{"error": f"vector store unavailable: {exc}"}]

    async def detect_anomalies(self, chain: str | None = None) -> list[dict[str, Any]]:
        """Detect anomalous wallet activity (potential hacks, exploits).

        Merges the two anomaly scans into one list, each entry tagged
        with ``anomaly_type``: ``tainted_deployer`` or ``new_wallet_volume``.
        """
        anomalies: list[dict[str, Any]] = []

        deployers = self.sources.local.anomalous_deployers(
            chain, window_days=INTEL_ANOMALY_WINDOW_DAYS
        )
        for row in deployers:
            anomalies.append(
                {
                    "address": row.get("address"),
                    "chain": row.get("chain"),
                    "anomaly_type": "tainted_deployer",
                    "severity": "high",
                    "evidence": str(row.get("reason") or ""),
                    "detail": {
                        "deploys": row.get("deploys"),
                        "last_deploy": _fmt_ts(row.get("last_deploy")),
                    },
                }
            )

        new_wallets = self.sources.local.anomalous_new_wallets(
            chain, min_txs=INTEL_ANOMALY_MIN_NEW_WALLET_TXS
        )
        for row in new_wallets:
            anomalies.append(
                {
                    "address": row.get("address"),
                    "chain": row.get("chain"),
                    "anomaly_type": "new_wallet_volume",
                    "severity": "medium",
                    "evidence": f"{row.get('txs')} txs in <48h ({row.get('counterparties')} counterparties)",
                    "detail": {
                        "txs": row.get("txs"),
                        "counterparties": row.get("counterparties"),
                        "first_seen": _fmt_ts(row.get("first_seen")),
                        "last_seen": _fmt_ts(row.get("last_seen")),
                    },
                }
            )

        # Rank: tainted deployers first, then by address.
        anomalies.sort(key=lambda a: (a["severity"] != "high", a.get("address") or ""))
        return anomalies[:INTEL_ANOMALY_MAX_RESULTS]


def _fmt_ts(value: Any) -> str:
    """Format a ClickHouse datetime value as ISO-8601 (best-effort)."""
    if not value:
        return ""
    return str(value).replace(" ", "T") + "Z"
