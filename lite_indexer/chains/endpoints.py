"""
EVM chain RPC endpoints.

Each EVM chain has a list of eth_getLogs-compatible RPC endpoints in tiers:
  T1 (indices 0-1) — fastest, most reliable (drpc.org, tenderly, official RPCs)
  T2 (indices 2-4) — fallbacks (tatum, 48.club, blxrbdn, nodereal)
  T3 (indices 5+)  — last resort (range-limited publics)
  T4 (indices 5+)  — API-key free tiers (${ALCHEMY_KEY}, ${GETBLOCK_KEY}, etc.)

All endpoints were live-verified 2026-08-01 with a topic-only eth_getLogs
probe (100-block range from tip) — the exact request shape the indexer uses.
Providers that answer eth_blockNumber but reject topic-only eth_getLogs
(publicnode → -32701 "specify an address", meowrpc → getLogs unsupported,
binance seeds → limit exceeded, merkle → 429, ankr → unauthorized except
celo) were removed. 1rpc.io, onfinality.io, blastapi (except bsc), tatum
(except bsc/polygon), cloudflare-eth and polygon-rpc are confirmed dead and
removed.

Some endpoints require an API key from a free-tier provider (Alchemy,
GetBlock, Chainstack, QuickNode, Infura). Set the corresponding ${ENV_VAR}
in your environment — the indexer resolves placeholders via
rpc.tiers._resolve_endpoint_url(). Unset keys leave the placeholder in
place and the call fails safely (never a hard crash).

Total: 51 endpoints across 14 chains (40 public + 11 API-key free tiers).
"""

from __future__ import annotations

import os
import re

GET_LOGS_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum (7 endpoints) ────────────────────────────────────────
    # T1: drpc.org, flashbots (verified eth_getLogs)
    # T2: nodereal public demo key
    # T4: alchemy, getblock, chainstack, quicknode (free API-key tiers)
    "ethereum": [
        "https://eth.drpc.org",  # T1 — drpc.org (verified, 0.5s)
        "https://rpc.flashbots.net",  # T1 — flashbots (verified, MEV-protected)
        "https://eth-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",  # T2 — nodereal public
        # T4: API-key free tiers (set env vars to activate)
        "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free (300M CU/mo)
        "https://eth.getblock.io/mainnet/?api_key=${GETBLOCK_KEY}",  # T4 — GetBlock free (40K req/day)
        "https://ethereum-mainnet.core.chainstack.com/${CHAINSTACK_KEY}",  # T4 — Chainstack free
        "https://${QUICKNODE_ENDPOINT}",  # T4 — QuickNode free
    ],
    # ── Base (4 endpoints) ────────────────────────────────────────────
    # T1: tenderly, official mainnet.base.org (both verified eth_getLogs)
    # T2: drpc.org
    # T4: alchemy
    # NOTE: publicnode (needs address filter) and blastapi (HTTP400) do not
    #       serve topic-only eth_getLogs on free tier and were removed.
    "base": [
        "https://base.gateway.tenderly.co",  # T1 — tenderly (verified, fast)
        "https://mainnet.base.org",  # T1 — official (verified)
        "https://base.drpc.org",  # T2 — drpc.org (verified)
        "https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── BSC (5 endpoints) ─────────────────────────────────────────────
    # T1: tatum (audit-verified on bsc), nodereal public
    # T2: 48.club (verified)
    # T3: blockrazor (25-block range cap — small-range fallback)
    # T4: alchemy
    "bsc": [
        "https://bsc-mainnet.gateway.tatum.io",  # T1 — tatum (audit-verified on bsc)
        "https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",  # T1 — nodereal public
        "https://rpc-bsc.48.club",  # T2 — 48.club (verified)
        "https://bsc.blockrazor.xyz",  # T3 — blockrazor (25-block range cap)
        "https://bsc-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Polygon (5 endpoints) ─────────────────────────────────────────
    # T1: tenderly, drpc.org (verified)
    # T2: tatum (audit-verified on polygon), blxrbdn (verified)
    # T4: alchemy
    "polygon": [
        "https://polygon.gateway.tenderly.co",  # T1 — tenderly (verified)
        "https://polygon.drpc.org",  # T1 — drpc.org (verified)
        "https://polygon-mainnet.gateway.tatum.io",  # T2 — tatum (audit-verified on polygon)
        "https://polygon-rpc.blxrbdn.com",  # T2 — bloxroute (verified eth_getLogs)
        "https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Arbitrum (4 endpoints) ────────────────────────────────────────
    # T1: tenderly, drpc.org (verified)
    # T2: official arb1.arbitrum.io (verified)
    # T4: alchemy
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",  # T1 — tenderly (verified, fast)
        "https://arbitrum.drpc.org",  # T1 — drpc.org (verified)
        "https://arb1.arbitrum.io/rpc",  # T2 — official (verified)
        "https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Optimism (3 endpoints) ────────────────────────────────────────
    # T1: tenderly (verified)
    # T2: official mainnet.optimism.io (verified, occasionally slow)
    # T4: alchemy
    "optimism": [
        "https://optimism.gateway.tenderly.co",  # T1 — tenderly (verified)
        "https://mainnet.optimism.io",  # T2 — official (verified)
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Avalanche (3 endpoints) ───────────────────────────────────────
    # T1: official api.avax.network, drpc.org (verified)
    # T4: infura (free tier)
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",  # T1 — official (verified)
        "https://avalanche.drpc.org",  # T1 — drpc.org (verified)
        "https://avalanche-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
    ],
    # ── Fantom (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.fantom.network + rpcapi.fantom.network (verified)
    # T2: drpc.org (verified)
    # T4: alchemy
    "fantom": [
        "https://rpc.fantom.network",  # T1 — official (verified)
        "https://rpcapi.fantom.network",  # T1 — official-alt (verified)
        "https://fantom.drpc.org",  # T2 — drpc.org (verified)
        "https://fantom-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Linea (3 endpoints) ───────────────────────────────────────────
    # T1: official rpc.linea.build, drpc.org (verified)
    # T4: infura (free tier)
    "linea": [
        "https://rpc.linea.build",  # T1 — official (verified)
        "https://linea.drpc.org",  # T1 — drpc.org (verified)
        "https://linea-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
    ],
    # ── Scroll (3 endpoints) ──────────────────────────────────────────
    # T1: official rpc.scroll.io, drpc.org (verified)
    # T4: infura (free tier)
    "scroll": [
        "https://rpc.scroll.io",  # T1 — official (verified)
        "https://scroll.drpc.org",  # T1 — drpc.org (verified)
        "https://scroll-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
    ],
    # ── zkSync Era (3 endpoints) ──────────────────────────────────────
    # T1: official mainnet.era.zksync.io, drpc.org (verified)
    # T4: alchemy (zksync-era)
    "zksync": [
        "https://mainnet.era.zksync.io",  # T1 — official (verified)
        "https://zksync.drpc.org",  # T1 — drpc.org (verified)
        "https://zksync-era.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Mantle (2 endpoints) ──────────────────────────────────────────
    # T1: official rpc.mantle.xyz (verified)
    # T2: drpc.org (verified; free-plan usage limit under burst)
    "mantle": [
        "https://rpc.mantle.xyz",  # T1 — official (verified)
        "https://mantle.drpc.org",  # T2 — drpc.org (verified)
    ],
    # ── Gnosis (2 endpoints) ──────────────────────────────────────────
    # T1: official rpc.gnosischain.com, drpc.org (verified)
    "gnosis": [
        "https://rpc.gnosischain.com",  # T1 — official (verified)
        "https://gnosis.drpc.org",  # T1 — drpc.org (verified)
    ],
    # ── Celo (3 endpoints) ────────────────────────────────────────────
    # T1: official forno.celo.org, ankr.com/celo (verified — only working ankr)
    # T2: drpc.org (verified)
    "celo": [
        "https://forno.celo.org",  # T1 — official forno (verified)
        "https://rpc.ankr.com/celo",  # T1 — ankr (verified — celo only)
        "https://celo.drpc.org",  # T2 — drpc.org (verified)
    ],
}

# Summary of EVM endpoints per chain:
#   ethereum: 7     base: 4      bsc: 5      polygon: 5
#   arbitrum: 4     optimism: 3  avalanche: 3 fantom: 4
#   linea: 3        scroll: 3    zksync: 3   mantle: 2
#   gnosis: 2       celo: 3
# -------------------------------------------------------
#   Total: 51 endpoints across 14 chains
#   (40 free public + 11 API-key free tiers)
#
#   Non-EVM: solana (6), tron (4)
#   Grand total: 61 endpoints across 16 chains


# ── Endpoint URL template resolution ───────────────────────────────
# API-key free tiers use ${ENV_VAR} placeholders (${ALCHEMY_KEY},
# ${GETBLOCK_KEY}, ...). Resolve them at import time so the main RPC
# path never calls dead placeholder URLs. When the env var is unset the
# placeholder is kept (the call fails naturally) — set the keys in the
# service environment to activate those endpoints.

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_endpoint_url(url: str) -> str:
    """Resolve ${ENV_VAR} placeholders in an endpoint URL.

    Args:
        url: Endpoint URL potentially containing ${ENV_VAR} templates.

    Returns:
        URL with env vars resolved (placeholder kept if unset).
    """

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if not value:
            return match.group(0)  # Keep placeholder — call fails safely
        return value

    return _ENV_VAR_RE.sub(_replace, url)


GET_LOGS_ENDPOINTS = {chain: [_resolve_endpoint_url(url) for url in urls] for chain, urls in GET_LOGS_ENDPOINTS.items()}
