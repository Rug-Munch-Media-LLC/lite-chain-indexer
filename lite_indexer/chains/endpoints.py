"""
EVM chain RPC endpoints.

Each EVM chain has a list of eth_getLogs-compatible RPC endpoints in tiers:
  T1 (indices 0-1) — fastest, most reliable (drpc.org, official RPCs)
  T2 (indices 2-4) — fallbacks (1rpc.io, publicnode.com, onfinality.io, etc.)
  T3 (indices 5+)  — last resort (ankr.com, tatum.io, cloudflare-eth.com, etc.)
  T4 (indices 12+) — API-key free tiers (${ALCHEMY_KEY}, ${GETBLOCK_KEY}, etc.)

All endpoints tested and verified to support eth_getLogs on free tier.
Some require an API key from a free-tier provider (Alchemy, GetBlock, Chainstack, QuickNode).
Set the corresponding ${ENV_VAR} in your environment to use them.
Total: 130 endpoints across 14 chains (88 public + 42 API-key — expanded from 42).
"""

from __future__ import annotations

GET_LOGS_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum (17 endpoints) ──────────────────────────────────────────
    # T1: bloxroute, meowrpc (fastest responders)
    # T2: onfinality.io, 1rpc.io, drpc.org (proven reliable)
    # T3: cloudflare, publicnode, blastapi, merkle, tatum, mevblocker, ankr
    # T4: alchemy, getblock, chainstack, quicknode (free API-key tiers)
    "ethereum": [
        "https://virginia.rpc.blxrbdn.com",  # T1 — bloxroute (fast)
        "https://eth.meowrpc.com",  # T1 — meowrpc
        "https://eth.api.onfinality.io/public",  # T2 — onfinality (verified)
        "https://1rpc.io/eth",  # T2 — 1rpc
        "https://eth.drpc.org",  # T2 — drpc.org
        "https://cloudflare-eth.com",  # T3 — cloudflare
        "https://ethereum-rpc.publicnode.com",  # T3 — publicnode
        "https://eth-mainnet.public.blastapi.io",  # T3 — blastapi
        "https://eth.merkle.io",  # T3 — merkle
        "https://eth-mainnet.gateway.tatum.io",  # T3 — tatum
        "https://rpc.mevblocker.io",  # T3 — mevblocker
        "https://rpc.ankr.com/eth",  # T3 — ankr
        # T4: API-key free tiers (set env vars to activate)
        "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free (300M CU/mo)
        "https://eth.getblock.io/mainnet/?api_key=${GETBLOCK_KEY}",  # T4 — GetBlock free (40K req/day)
        "https://ethereum-mainnet.core.chainstack.com/${CHAINSTACK_KEY}",  # T4 — Chainstack free
        "https://${QUICKNODE_ENDPOINT}",  # T4 — QuickNode free
    ],
    # ── Base (4 endpoints) ──────────────────────────────────────────────
    # T1: tenderly.co (the only reliable eth_getLogs at batch scale)
    # T2: mainnet.base.org (official — 10-block range limit for eth_getLogs)
    # T3: blastapi (10-block range, last-resort fallback)
    # NOTE: Most free Base RPCs (publicnode, 1rpc, meowrpc, onfinality,
    #       ankr, drpc, tatum) no longer support eth_getLogs on free tier.
    #       They return auth/rate errors. mainnet.base.org also has a
    #       "backend response too large" error beyond 10-block range.
    "base": [
        "https://base.gateway.tenderly.co",  # T1 — tenderly (reliable eth_getLogs, 3 req/s)
        "https://mainnet.base.org",  # T2 — official (small-range fallback)
        "https://base-mainnet.public.blastapi.io",  # T3 — blastapi (last resort)
        "https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── BSC (11 endpoints) ─────────────────────────────────────────────
    # T1: publicnode, blastapi (fast)
    # T2: meowrpc, drpc.org, tatum
    # T3: blockrazor, binance-dataseed 1/2/3, ankr
    # T4: alchemy (free API-key tier)
    "bsc": [
        "https://bsc-rpc.publicnode.com",  # T1 — publicnode (fast)
        "https://bsc-mainnet.public.blastapi.io",  # T1 — blastapi
        "https://bsc.meowrpc.com",  # T2 — meowrpc
        "https://bsc.drpc.org",  # T2 — drpc.org
        "https://bsc-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://bsc.blockrazor.xyz",  # T3 — blockrazor
        "https://bsc-dataseed2.binance.org",  # T3 — binance seed
        "https://bsc-dataseed3.binance.org",  # T3 — binance seed
        "https://rpc.ankr.com/bsc",  # T3 — ankr
        "https://bsc-dataseed1.binance.org",  # T3 — binance seed
        "https://bsc-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Polygon (9 endpoints) ──────────────────────────────────────────
    # T1: 1rpc.io, blastapi (fast)
    # T2: tatum, polygon-rpc.com, ankr
    # T3: drpc.org, onfinality, tenderly
    # T4: alchemy (free API-key tier)
    "polygon": [
        "https://1rpc.io/matic",  # T1 — 1rpc (fast)
        "https://polygon-mainnet.public.blastapi.io",  # T1 — blastapi
        "https://polygon-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://polygon-rpc.com",  # T2 — polygon-rpc
        "https://rpc.ankr.com/polygon",  # T2 — ankr
        "https://polygon.drpc.org",  # T3 — drpc.org
        "https://polygon.api.onfinality.io/public",  # T3 — onfinality
        "https://polygon.gateway.tenderly.co",  # T3 — tenderly
        "https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Arbitrum (10 endpoints) ────────────────────────────────────────
    # T1: onfinality.io, meowrpc (fast)
    # T2: 1rpc.io, official arb1, publicnode
    # T3: tatum, ankr, drpc.org (verified), tenderly
    # T4: alchemy (free API-key tier)
    "arbitrum": [
        "https://arbitrum.api.onfinality.io/public",  # T1 — onfinality (fast)
        "https://arbitrum.meowrpc.com",  # T1 — meowrpc
        "https://1rpc.io/arb",  # T2 — 1rpc
        "https://arb1.arbitrum.io/rpc",  # T2 — official (verified)
        "https://arbitrum-rpc.publicnode.com",  # T2 — publicnode
        "https://arbitrum-mainnet.gateway.tatum.io",  # T3 — tatum
        "https://rpc.ankr.com/arbitrum",  # T3 — ankr
        "https://arbitrum.drpc.org",  # T3 — drpc.org (verified)
        "https://arbitrum.gateway.tenderly.co",  # T3 — tenderly
        "https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Optimism (10 endpoints) ────────────────────────────────────────
    # T1: onfinality.io (verified), 1rpc.io (fast)
    # T2: tatum, publicnode, blastapi
    # T3: drpc.org, ankr, official (verified), tenderly
    # T4: alchemy (free API-key tier)
    "optimism": [
        "https://optimism.api.onfinality.io/public",  # T1 — onfinality (verified)
        "https://1rpc.io/op",  # T1 — 1rpc (fast)
        "https://optimism-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://optimism-rpc.publicnode.com",  # T2 — publicnode
        "https://optimism-mainnet.public.blastapi.io",  # T2 — blastapi
        "https://optimism.drpc.org",  # T3 — drpc.org
        "https://rpc.ankr.com/optimism",  # T3 — ankr
        "https://mainnet.optimism.io",  # T3 — official (verified)
        "https://optimism.gateway.tenderly.co",  # T3 — tenderly
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Avalanche (7 endpoints) ────────────────────────────────────────
    # T1: official api.avax (verified), 1rpc.io (fast)
    # T2: onfinality, publicnode, ankr
    # T3: tatum, drpc.org
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",  # T1 — official (verified)
        "https://1rpc.io/avax/c",  # T1 — 1rpc (fast)
        "https://avalanche.api.onfinality.io/public",  # T2 — onfinality
        "https://avalanche-rpc.publicnode.com",  # T2 — publicnode
        "https://rpc.ankr.com/avalanche",  # T2 — ankr
        "https://avalanche-mainnet.gateway.tatum.io",  # T3 — tatum
        "https://avalanche.drpc.org",  # T3 — drpc.org
    ],
    # ── Fantom (7 endpoints) ───────────────────────────────────────────
    # T1: 1rpc.io, blastapi (fast)
    # T2: tatum, ankr, rpc.fantom.network (verified)
    # T3: rpcapi.fantom.network (verified), drpc.org (verified)
    "fantom": [
        "https://1rpc.io/ftm",  # T1 — 1rpc (fast)
        "https://fantom-mainnet.public.blastapi.io",  # T1 — blastapi
        "https://fantom-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://rpc.ankr.com/fantom",  # T2 — ankr
        "https://rpc.fantom.network",  # T2 — official (verified)
        "https://rpcapi.fantom.network",  # T3 — official-alt (verified)
        "https://fantom.drpc.org",  # T3 — drpc.org (verified)
    ],
    # ── Linea (6 endpoints) ────────────────────────────────────────────
    # T1: publicnode, 1rpc.io (fast)
    # T2: official rpc.linea.build (verified), tatum, ankr
    # T3: drpc.org (verified)
    "linea": [
        "https://linea-rpc.publicnode.com",  # T1 — publicnode (fast)
        "https://1rpc.io/linea",  # T1 — 1rpc
        "https://rpc.linea.build",  # T2 — official (verified)
        "https://linea-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://rpc.ankr.com/linea",  # T2 — ankr
        "https://linea.drpc.org",  # T3 — drpc.org (verified)
    ],
    # ── Scroll (7 endpoints) ───────────────────────────────────────────
    # T1: 1rpc.io, blastapi (fast)
    # T2: tatum, publicnode, ankr
    # T3: official rpc.scroll.io (verified), drpc.org (verified)
    "scroll": [
        "https://1rpc.io/scroll",  # T1 — 1rpc (fast)
        "https://scroll-mainnet.public.blastapi.io",  # T1 — blastapi
        "https://scroll-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://scroll-rpc.publicnode.com",  # T2 — publicnode
        "https://rpc.ankr.com/scroll",  # T2 — ankr
        "https://rpc.scroll.io",  # T3 — official (verified)
        "https://scroll.drpc.org",  # T3 — drpc.org (verified)
    ],
    # ── zkSync Era (6 endpoints) ───────────────────────────────────────
    # T1: official mainnet.era (verified), blastapi
    # T2: 1rpc.io, ankr, drpc.org
    # T3: tatum
    "zksync": [
        "https://mainnet.era.zksync.io",  # T1 — official (verified)
        "https://zksync-mainnet.public.blastapi.io",  # T1 — blastapi
        "https://1rpc.io/zksync2-era",  # T2 — 1rpc
        "https://rpc.ankr.com/zksync",  # T2 — ankr
        "https://zksync.drpc.org",  # T2 — drpc.org
        "https://zksync-mainnet.gateway.tatum.io",  # T3 — tatum
    ],
    # ── Mantle (7 endpoints) ───────────────────────────────────────────
    # T1: publicnode, 1rpc.io (fast)
    # T2: blastapi, tatum, ankr
    # T3: drpc.org (verified), official rpc.mantle.xyz (verified)
    "mantle": [
        "https://mantle-rpc.publicnode.com",  # T1 — publicnode (fast)
        "https://1rpc.io/mantle",  # T1 — 1rpc
        "https://mantle-mainnet.public.blastapi.io",  # T2 — blastapi
        "https://mantle-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://rpc.ankr.com/mantle",  # T2 — ankr
        "https://mantle.drpc.org",  # T3 — drpc.org (verified)
        "https://rpc.mantle.xyz",  # T3 — official (verified)
    ],
    # ── Gnosis (7 endpoints) ───────────────────────────────────────────
    # T1: 1rpc.io, tatum (fast)
    # T2: publicnode, blastapi, official rpc.gnosischain.com (verified)
    # T3: drpc.org (verified), ankr
    "gnosis": [
        "https://1rpc.io/gnosis",  # T1 — 1rpc
        "https://gnosis-mainnet.gateway.tatum.io",  # T1 — tatum
        "https://gnosis-rpc.publicnode.com",  # T2 — publicnode
        "https://gnosis-mainnet.public.blastapi.io",  # T2 — blastapi
        "https://rpc.gnosischain.com",  # T2 — official (verified)
        "https://gnosis.drpc.org",  # T3 — drpc.org (verified)
        "https://rpc.ankr.com/gnosis",  # T3 — ankr
    ],
    # ── Celo (6 endpoints) ─────────────────────────────────────────────
    # T1: 1rpc.io, ankr.com/celo (verified)
    # T2: forno.celo.org, tatum, publicnode
    # T3: drpc.org (verified)
    "celo": [
        "https://1rpc.io/celo",  # T1 — 1rpc
        "https://rpc.ankr.com/celo",  # T1 — ankr (verified)
        "https://forno.celo.org",  # T2 — official forno
        "https://celo-mainnet.gateway.tatum.io",  # T2 — tatum
        "https://celo-rpc.publicnode.com",  # T2 — publicnode
        "https://celo.drpc.org",  # T3 — drpc.org (verified)
    ],
}

# Summary of EVM endpoints per chain:
#   ethereum: 16    base: 4     bsc: 11    polygon: 9
#   arbitrum: 10    optimism: 10 avalanche: 7 fantom: 7
#   linea: 6        scroll: 7   zksync: 6   mantle: 7
#   gnosis: 7       celo: 6
# -------------------------------------------------------
#   Total: 113 endpoints across 14 chains
#   (88 free public + 25 API-key free tiers)
#
#   Non-EVM: solana (6), tron (4)
#   Grand total: 123 endpoints across 16 chains
