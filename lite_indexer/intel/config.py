"""
Wallet intelligence configuration.

All tunable parameters for the intel package live here — chain aliasing,
risk factor weights and thresholds, anomaly windows, external-source
timeouts, and result caps. Values come from environment variables with
sane defaults, matching the style of :mod:`lite_indexer.vectors.config`.
"""

from __future__ import annotations

import os

# ── Chain normalization ────────────────────────────────────────────────
# The warehouse stores chain identifiers inconsistently: ``evm_transfers``
# uses full names (``ethereum``), while ``wallet_labels`` / ``scam_addresses``
# use short aliases for some chains (``eth``). Intel lookups query both
# spellings; ``normalize_chain`` canonicalizes a user-supplied chain name
# to the full name used by ``evm_transfers``.
CHAIN_ALIASES: dict[str, str] = {
    "eth": "ethereum",
    "ethereum": "ethereum",
    "bsc": "bsc",
    "bnb": "bsc",
    "base": "base",
    "polygon": "polygon",
    "matic": "polygon",
    "arbitrum": "arbitrum",
    "optimism": "optimism",
    "op": "optimism",
    "avalanche": "avalanche",
    "avax": "avalanche",
    "fantom": "fantom",
    "linea": "linea",
    "scroll": "scroll",
    "zksync": "zksync",
    "zksync_era": "zksync",
    "mantle": "mantle",
    "gnosis": "gnosis",
    "celo": "celo",
    "tron": "tron",
    "solana": "solana",
    "sol": "solana",
    "btc": "btc",
    "bitcoin": "btc",
}


def normalize_chain(chain: str | None) -> str | None:
    """Canonicalize a chain name to the full ``evm_transfers`` spelling.

    Returns ``None`` for unknown/empty chains (callers then search all
    chains).
    """
    if not chain:
        return None
    return CHAIN_ALIASES.get(chain.strip().lower(), chain.strip().lower())


# ── External public data sources ───────────────────────────────────────
# All external enrichments are best-effort: they run only when
# ``INTEL_EXTERNAL_SOURCES_ENABLED`` is set and each adapter degrades
# gracefully (timeout / HTTP error / missing API key => "unavailable").
INTEL_EXTERNAL_SOURCES_ENABLED: bool = (os.getenv("INTEL_EXTERNAL_SOURCES_ENABLED") or "1") == "1"
INTEL_HTTP_TIMEOUT: float = float(os.getenv("INTEL_HTTP_TIMEOUT", "8"))
INTEL_EXTERNAL_CACHE_TTL: int = int(os.getenv("INTEL_EXTERNAL_CACHE_TTL", "86400"))  # 24h

# Chainabuse API key (optional; the /v0/reports endpoint requires one).
CHAINABUSE_API_KEY: str = os.getenv("CHAINABUSE_API_KEY", "")

# ── Risk scoring weights (sums to 1.0) ─────────────────────────────────
# Mirrors the wallet-intelligence rubric: CEX/mixer/exploit/sanctioned
# signals dominate; wallet age and deployer rug patterns add signal;
# precomputed wallet_profiles contribute a small corroborating weight.
RISK_WEIGHTS: dict[str, float] = {
    "known_scam_taint": 0.25,      # direct match in scam_addresses / bad label categories
    "mixer_interaction": 0.15,     # mixer label, or funded by a Tornado Cash-style address
    "exploit_funding": 0.15,       # first-hop funder tagged exploiter/hack/phish
    "deployer_rug_pattern": 0.15,  # cross-chain deploys / many recent deploys
    "sanctioned_interaction": 0.10,  # OFAC / is_sanctioned label on the wallet or its funder
    "wallet_age": 0.10,            # brand-new wallets are higher risk
    "precomputed": 0.05,           # wallet_profiles.risk_score (deployer risk pipeline)
    "activity_anomaly": 0.05,      # abnormally high volume for a young wallet
}

# 0-100 per-factor thresholds.
RISK_LEVELS: list[tuple[float, str]] = [
    (75.0, "critical"),
    (50.0, "high"),
    (25.0, "medium"),
    (0.0, "low"),
]

# Age buckets in days -> 0-100 sub-score.
AGE_SUBSCORES: list[tuple[int, float]] = [
    (7, 100.0),     # < 1 week
    (30, 70.0),     # < 1 month
    (90, 40.0),     # < 3 months
    (365, 20.0),    # < 1 year
    (2**31, 0.0),   # older
]

# ── Anomaly detection ──────────────────────────────────────────────────
INTEL_ANOMALY_WINDOW_DAYS: int = int(os.getenv("INTEL_ANOMALY_WINDOW_DAYS", "7"))
INTEL_NEW_WALLET_DAYS: int = int(os.getenv("INTEL_NEW_WALLET_DAYS", "2"))
INTEL_ANOMALY_MIN_NEW_WALLET_TXS: int = int(os.getenv("INTEL_ANOMALY_MIN_NEW_WALLET_TXS", "10"))
INTEL_ANOMALY_MAX_RESULTS: int = int(os.getenv("INTEL_ANOMALY_MAX_RESULTS", "50"))

# ── Result caps ────────────────────────────────────────────────────────
INTEL_MAX_CLUSTER_ADDRESSES: int = int(os.getenv("INTEL_MAX_CLUSTER_ADDRESSES", "50"))
INTEL_MAX_SIMILAR_WALLETS: int = int(os.getenv("INTEL_MAX_SIMILAR_WALLETS", "20"))
INTEL_MAX_FUNDERS: int = int(os.getenv("INTEL_MAX_FUNDERS", "5"))

# Labels in wallet_labels.label_category that are inherently high-risk.
HIGH_RISK_LABEL_CATEGORIES: frozenset[str] = frozenset(
    {
        "scam",
        "darknet_market",
        "sanctioned",
        "ransomware",
        "mixer",
        "money_laundering",
    }
)

# Infrastructure / sink addresses excluded from the new-wallet anomaly
# scan (they receive millions of transfers and would always appear "new"
# because the scan's min() is window-relative).
EXCLUDED_SINK_ADDRESSES: frozenset[str] = frozenset(
    {
        "0x0000000000000000000000000000000000000000",  # null / burn
        "0x000000000000000000000000000000000000dead",  # dead address
        "0x0000000000000000000000000000000000000001",
        "0x0000000000000000000000000000000000000002",
        "0x00000000219ab540356cbb839cbe05303d7705fa",  # eth2 deposit contract
    }
)

# scam_addresses.threat_type values treated as exploit/stealing.
EXPLOIT_THREAT_TYPES: frozenset[str] = frozenset(
    {"exploit", "exploiter", "hack", "phish", "phish-hack", "malicious", "scam", "ponzi"}
)
