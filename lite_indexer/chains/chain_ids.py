"""Expected EVM chain ids for endpoint impostor detection.

Extracted from ``chains/endpoints.py`` (shrink-only size ratchet). An endpoint
answering with a different chainId is an impostor or a misconfigured endpoint
regardless of latency, so every probe compares against this table.
``chains.endpoints`` re-exports :data:`EXPECTED_CHAIN_IDS` so existing
importers keep working.
"""

from __future__ import annotations

# ── Expected chainId map ────────────────────────────────────────────
# Canonical EIP-155 ids per chain name. The probe script
# (scripts/probe-rpc-endpoints.py) hard-rejects any endpoint answering
# with a different id — a wrong chainId means impostor/misconfigured
# endpoint regardless of latency. Solana/Tron use non-EVM probes.
EXPECTED_CHAIN_IDS: dict[str, str] = {
    "ethereum": "0x1",
    "base": "0x2105",
    "bsc": "0x38",
    "polygon": "0x89",
    "arbitrum": "0xa4b1",
    "optimism": "0xa",
    "avalanche": "0xa86a",
    "fantom": "0xfa",
    "linea": "0xe708",
    "scroll": "0x82750",
    "zksync": "0x144",
    "mantle": "0x1388",
    "gnosis": "0x64",
    "celo": "0xa4ec",
    "blast": "0x13e31",  # 81457 — verified 2026-09-01
    "fraxtal": "0xfc",  # 252 — verified 2026-09-01
    "metis": "0x440",  # 1088 — verified 2026-09-01
    "robinhood": "0x1237",  # 4663 — old spec chainId 78600 was Vanguard
    "hyperliquid": "0x3e7",  # 999 — HyperEVM (live-verified 2026-09-13)
}
