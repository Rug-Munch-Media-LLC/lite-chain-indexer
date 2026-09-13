"""
Non-EVM RPC endpoints — Solana and Tron.

Solana uses JSON-RPC but with different methods (getSlot, etc.)
These are NOT eth_getLogs endpoints — they're for Solana-native RPC calls.
Tiers: T1=official/mainnet verified, T2=publicnode, T3=fallbacks.
Tested 2026-08-12: getSlot, getSignaturesForAddress, getTransaction.

Tron uses REST API (TronGrid) for event queries and JSON-RPC for blocks.
Only TronGrid (api.trongrid.io) supports the contract events API.
Others are fallbacks for block queries only.
Tested 2026-08-12: /wallet/getnowblock, /v1/contracts/*/events.
"""

from __future__ import annotations

from lite_indexer.env import load_env_file

from .endpoints import _resolved_endpoints

# Ensure ${ALCHEMY_KEY} / ${HELIUS_KEY} / ${HELIUS_KEY_2} / ${HELIUS_KEY_3}
# / ${TRONGRID_API_KEY} resolve before SOLANA/TRON endpoint resolution below
# (idempotent; also loaded by endpoints.py).
load_env_file()

# ── Solana RPC Endpoints ─────────────────────────────────────────────

SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    # T2 — Pocket Network (keyless, 2000 req/IP/min; getHealth→ok 2026-09-13)
    "https://solana.api.pocket.network",
    # T4 — API-key free tiers (set env vars to activate)
    "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # Alchemy free
    # Helius ×3 INDEPENDENT free accounts (capacity + failover). Live-verified
    # 2026-09-13: all three keys answered getHealth "ok" + getSlot 200 on the
    # canonical host mainnet.helius-rpc.com. The legacy host rpc.helius.xyz
    # now returns 403 — do not use it. Keyed entries stay after the keyless
    # Pocket/PublicNode/official endpoints (free-first ordering).
    "https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY}",  # Helius free — account 1 (live-verified 2026-09-13)
    "https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY_2}",  # Helius free — account 2 (live-verified 2026-09-13)
    "https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY_3}",  # Helius free — account 3 (live-verified 2026-09-13)
    # ── Freemium depth (added 2026-08-23 — activate via env) ──
    # NOTE: dRPC-lb solana is PAID-ONLY ("chain is not available on free plan"
    #       re-confirmed 2026-08-23 with a live free key) — no template added.
    "https://rpc.ankr.com/solana/${ANKR_KEY}",  # Ankr keyed (keyless was plan-blocked)
    "${QUICKNODE_SOLANA_URL}",  # QuickNode dedicated endpoint (full URL)
    "${CHAINSTACK_SOLANA_URL}",  # Chainstack shared node (live-verified 2026-08-23: getSlot ok, ~250ms)
    "${GETBLOCK_SOLANA_URL}",  # GetBlock shared (live-verified 2026-08-23)
    "https://sol.nownodes.io/${NOWNODES_API_KEY}",  # NOWNodes (verified 2026-08-23)
    # Uniblock unified JSON-RPC — fallback (live-verified getHealth 2026-09-13);
    # keyed template, dropped from the pool when UNIBLOCK_KEY is unset.
    "https://api.uniblock.dev/uni/v1/json-rpc?chainId=solana&apiKey=${UNIBLOCK_KEY}",  # Uniblock — live-verified 2026-09-13
]
# NOTE: solana.drpc.org and rpc.ankr.com/solana KEYLESS were removed
# 2026-08-03 — confirmed unusable on the free plan ("chain is not available
# on free plan" / "API key is not allowed"). The keyed variants above use
# paid/free-tier keys and are a different access path. The two public
# endpoints at the top are verified alive via getSlot.

# ── Tron RPC Endpoints ───────────────────────────────────────────────

TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Pocket Network (keyless; live getnowblock + eth_chainId 0x2b6653dc, 2026-09-13)
    "https://tron.api.pocket.network",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
    # T4 — Chainstack TRON Mainnet node — full-URL basic-auth endpoint
    # (live-verified 2026-09-13: eth_blockNumber + wallet/getblockbynum +
    # eth_getLogs). HTTPX handles userinfo-in-URL. Dropped from the pool
    # when CHAINSTACK_TRON_URL is unset.
    "${CHAINSTACK_TRON_URL}",
]
# NOTE: rpc.tronlink.org was removed 2026-08-03 — confirmed dead (empty
# response on /wallet/getnowblock). The remaining three endpoints are
# verified alive.
#
# TRONGRID_API_KEY (2026-09-13): the provisioned key is accepted BOTH as the
# header ``TRON-PRO-API-KEY`` and as a PATH-SCOPED query param
# (``POST /wallet/getnowblock?api_key=…`` → 200). It is NOT embeddable as a
# base URL: ``POST https://api.trongrid.io?api_key=…`` → 405 (openresty), and
# the Tron transport builds requests as ``f"{base_url}{path}"``
# (strategies/tron/rpc.py), so a query-carrying base would corrupt every call.
# The keyless host already serves 200 (TronGrid does not gate this endpoint on
# the key). No keyed base template is added — it needs strategy-level
# query-param support; a broken template would only add retry noise.

# ── Endpoint URL template resolution ───────────────────────────────
# Resolve ${ENV_VAR} placeholders (${ALCHEMY_KEY}, ${HELIUS_KEY}) so the
# API-key free tiers are usable when keys are present in the environment.
# Endpoints with unset keys are dropped from the active pool (never dialed).
SOLANA_ENDPOINTS = _resolved_endpoints(SOLANA_ENDPOINTS)
TRON_ENDPOINTS = _resolved_endpoints(TRON_ENDPOINTS)
