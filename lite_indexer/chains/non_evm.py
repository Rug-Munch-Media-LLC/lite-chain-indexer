"""
Non-EVM RPC endpoints — Solana and Tron (PUBLIC free/keyless pool).

This module ships only FREE, KEYLESS public RPC endpoints. Curated providers
that need an API key or a provisioned URL (e.g. ``${PROVIDER_KEY}`` templates)
are supplied by the PRIVATE runtime overlay — see
:mod:`lite_indexer.chains._overlay` and ``docs/PROVIDER_OVERLAY.md``. The
overlay is merged AFTER the builtin list (free-first order) and is never
committed to this repository.

Solana uses JSON-RPC but with different methods (getSlot, etc.) — these are
not eth_getLogs endpoints. Tiers: T1=official/mainnet verified, T2=publicnode,
T3=fallbacks. Tested 2026-08-12: getSlot, getSignaturesForAddress,
getTransaction.

Tron uses the REST API (TronGrid) for event queries and JSON-RPC for blocks.
Only TronGrid (api.trongrid.io) supports the contract events API; the other
public endpoints are fallbacks for block queries only.
Tested 2026-08-12: /wallet/getnowblock, /v1/contracts/*/events.
"""

from __future__ import annotations

from lite_indexer.env import load_env_file

from ._overlay import load_endpoint_overlay, merge_endpoints
from .endpoints import _resolved_endpoints

# Load the shared env file BEFORE placeholder resolution below (idempotent;
# also loaded by endpoints.py).
load_env_file()

# ── Solana RPC Endpoints (free/keyless) ──────────────────────────────

SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    "https://solana.publicnode.com",  # T2 — publicnode canonical host — live-verified getHealth 2026-09-13
    # T2 — Pocket Network (keyless, 2000 req/IP/min; getHealth→ok 2026-09-13)
    "https://solana.api.pocket.network",
]
# NOTE: some keyless Solana providers were removed 2026-08-03 as unusable on
# their free plan. Curated/keyed Solana providers are supplied by the private
# runtime overlay, not this public list.

# ── Tron RPC Endpoints (free/keyless) ────────────────────────────────

TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Pocket Network (keyless; live getnowblock + eth_chainId 0x2b6653dc, 2026-09-13)
    "https://tron.api.pocket.network",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
    "https://tron.publicnode.com",  # T2 — publicnode canonical host — live-verified getnowblock 2026-09-13
]
# NOTE: rpc.tronlink.org was removed 2026-08-03 — confirmed dead (empty
# response on /wallet/getnowblock). A TronGrid key can be supplied by the
# overlay only in URL-embeddable form; TronGrid's key is header/path-scoped
# (``TRON-PRO-API-KEY`` or ``?api_key=``), which the base-URL transport does
# not support, so keyed TronGrid is out of scope for the public pool.

# ── Overlay merge + placeholder resolution ───────────────────────────
# Builtin free/keyless endpoints keep their order; the private overlay (if
# present) is appended per chain. Unresolved ``${VAR}`` endpoints are dropped.
_ENDPOINT_OVERLAY = load_endpoint_overlay()
SOLANA_ENDPOINTS = merge_endpoints({"solana": SOLANA_ENDPOINTS}, _ENDPOINT_OVERLAY).get("solana", [])
TRON_ENDPOINTS = merge_endpoints({"tron": TRON_ENDPOINTS}, _ENDPOINT_OVERLAY).get("tron", [])
SOLANA_ENDPOINTS = _resolved_endpoints(SOLANA_ENDPOINTS)
TRON_ENDPOINTS = _resolved_endpoints(TRON_ENDPOINTS)
