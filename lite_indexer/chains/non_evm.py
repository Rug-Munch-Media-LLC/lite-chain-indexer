"""
Non-EVM RPC endpoints — Solana and Tron.

Solana uses JSON-RPC but with different methods (getSlot, etc.)
These are NOT eth_getLogs endpoints — they're for Solana-native RPC calls.
Tiers: T1=official/mainnet verified, T2=publicnode, T3=fallbacks.
Tested 2026-07-30: getSlot, getSignaturesForAddress, getTransaction.

Tron uses REST API (TronGrid) for event queries and JSON-RPC for blocks.
Only TronGrid (api.trongrid.io) supports the contract events API.
Others are fallbacks for block queries only.
Tested 2026-07-30: /wallet/getnowblock, /v1/contracts/*/events.
"""

from __future__ import annotations

from .endpoints import _resolve_endpoint_url

# ── Solana RPC Endpoints ─────────────────────────────────────────────

SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    # T3 — Fallbacks (may have lower rate limits or require API keys)
    "https://rpc.ankr.com/solana",
    "https://solana.drpc.org",
    # T4 — API-key free tiers (set env vars to activate)
    "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # Alchemy free
    "https://rpc.helius.xyz/?api-key=${HELIUS_KEY}",  # Helius free (25K req/mo)
]

# ── Tron RPC Endpoints ───────────────────────────────────────────────

TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
    # T3 — Fallback
    "https://rpc.tronlink.org",
]

# ── Endpoint URL template resolution ───────────────────────────────
# Resolve ${ENV_VAR} placeholders (${ALCHEMY_KEY}, ${HELIUS_KEY}) so the
# API-key free tiers are usable when keys are present in the environment.
SOLANA_ENDPOINTS = [_resolve_endpoint_url(url) for url in SOLANA_ENDPOINTS]
TRON_ENDPOINTS = [_resolve_endpoint_url(url) for url in TRON_ENDPOINTS]
