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

from .endpoints import _resolved_endpoints

# ── Solana RPC Endpoints ─────────────────────────────────────────────

SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    # T4 — API-key free tiers (set env vars to activate)
    "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # Alchemy free
    "https://rpc.helius.xyz/?api-key=${HELIUS_KEY}",  # Helius free (25K req/mo)
]
# NOTE: solana.drpc.org and rpc.ankr.com/solana were removed 2026-08-03 —
# confirmed unusable on the free plan ("chain is not available on free
# plan" / "API key is not allowed"). The two public endpoints above are
# verified alive via getSlot.

# ── Tron RPC Endpoints ───────────────────────────────────────────────

TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
]
# NOTE: rpc.tronlink.org was removed 2026-08-03 — confirmed dead (empty
# response on /wallet/getnowblock). The remaining three endpoints are
# verified alive.

# ── Endpoint URL template resolution ───────────────────────────────
# Resolve ${ENV_VAR} placeholders (${ALCHEMY_KEY}, ${HELIUS_KEY}) so the
# API-key free tiers are usable when keys are present in the environment.
# Endpoints with unset keys are dropped from the active pool (never dialed).
SOLANA_ENDPOINTS = _resolved_endpoints(SOLANA_ENDPOINTS)
TRON_ENDPOINTS = _resolved_endpoints(TRON_ENDPOINTS)
