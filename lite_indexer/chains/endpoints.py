"""
EVM chain RPC endpoints — PUBLIC free/keyless pool + private overlay hook.

This module ships only FREE, KEYLESS public RPC endpoints (official chain
RPCs, publicnode, and similar public gateways). Curated providers that need
an API key or a provisioned URL live in a PRIVATE runtime overlay supplied
via ``LITE_INDEXER_ENDPOINT_OVERLAY`` — see
:mod:`lite_indexer.chains._overlay` and ``docs/PROVIDER_OVERLAY.md``. The
overlay is merged in AFTER the builtin pool (free-first order) and is never
committed to this repository.

Each EVM chain has an ordered list of eth_getLogs-compatible endpoints.
Endpoints were live-verified with an eth_blockNumber probe plus a
topic-only eth_getLogs probe (the exact request shape the indexer uses).
Providers that answer eth_blockNumber but reject topic-only eth_getLogs
(publicnode → archive token required, merkle → -32601 "Method not found")
are kept as blockNumber/timestamp fallbacks in lower tiers; the pool
scorer's eth_getLogs verification deprioritizes them for log queries (see
lite_indexer/rpc/health.py).

Some endpoints require an API key from a free-tier provider; set the
corresponding ${ENV_VAR} in your environment — the indexer resolves
placeholders at import time via os.path.expandvars
(chains.endpoints._resolved_endpoints()). Endpoints whose key is NOT set are
excluded from the active pool (a literal ``${...}`` in a URL can never be
dialed). Restart the service after setting a key to activate the endpoint.
"""

from __future__ import annotations

import os

from lite_indexer.env import load_env_file
from lite_indexer.logging import get_logger

from ._overlay import load_endpoint_overlay, merge_endpoints

# Load the shared env file BEFORE module-level placeholder resolution below,
# so every import path sees the same environment. Idempotent + non-destructive.
load_env_file()

logger = get_logger(__name__)

GET_LOGS_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum ──────────────────────────────────────────────────────
    # T1: onfinality public (verified 2026-08-05 — eth_getLogs OK at
    #     10-block chunks from tip; 429-banned on 50-block catch-up,
    #     keep chunks small via ETH_BATCH_SIZE=10)
    #     nodies (eth-pokt.nodies.app — verified 2026-08-05; 50-block
    #     getLogs cap matches CHUNK_SIZE_EVM exactly; 20k result cap)
    #     flashbots (MEV-protected; ranged topic-only getLogs errors),
    #     mevblocker (verified 2026-08-05 — 10k-result getLogs cap, blocks
    #     MEV txs; fine for blockNumber + small getLogs ranges),
    #     publicnode (verified 2026-08-05 blockNumber; eth_getLogs needs an
    #     archive token — blockNumber/timestamp fallback only)
    # T5: blastapi (10-block range cap — OK with ETH_BATCH_SIZE=10),
    #     merkle (verified 2026-08-05 blockNumber; eth_getLogs returns
    #     -32601 Method not found — tip/blockNumber fallback only)
    #       1rpc.io/eth (usage limit) were tested 2026-08-05 and failed
    #       added 2026-08-23 (freemium depth — activate via env).
    "ethereum": [
        "https://eth.api.onfinality.io/public",
        "https://eth.api.pocket.network",  # T1 — Pocket Network (keyless, 2000 req/IP/min, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
        "https://eth-pokt.nodies.app",
        "https://rpc.mevblocker.io",
        "https://rpc.flashbots.net",
        "https://virginia.rpc.blxrbdn.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://eth-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://ethereum.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── Base (5 endpoints) ────────────────────────────────────────────
    # T1: tenderly, official mainnet.base.org (both verified eth_getLogs)
    # NOTE: publicnode (needs address filter) and blastapi (HTTP400) do not
    #       serve topic-only eth_getLogs on free tier and were removed.
    "base": [
        "https://base.gateway.tenderly.co",
        "https://base.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mainnet.base.org",
        "https://base-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://base.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        "https://base-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
    ],
    # ── BSC (8 endpoints) ─────────────────────────────────────────────
    #     exact indexer request shape: topic-only eth_getLogs over a
    #     100-block range from tip — both returned full result sets)
    # T2: blockrazor (25-block range cap — small-range fallback,
    #     registered in limits.py so the indexer caps chunks for it),
    #     official Binance public seeds (blockNumber verified 2026-08-07;
    #     bsc-dataseed1.defibit.io / ninicoin.io are mirrors)
    # NOTE: tatum (range-limit + 429), binance seeds (limit exceeded),
    #       bsc-rpc.publicnode.com (http_403 on ranged topic-only —
    #       4.7k rejections during 08/04 outage), merkle (HTTP400),
    #       limit) probed and rejected — official seeds added instead.
    "bsc": [
        "https://bsc.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
        "https://rpc-bsc.48.club",  # T1 — 48.club (verified)
        "https://bsc.blockrazor.xyz",  # T2 — blockrazor (25-block range cap)
        "https://bsc-dataseed.binance.org",  # T2 — official Binance seed (blockNumber verified 2026-08-07)
        "https://bsc-dataseed1.binance.org",  # T2 — official Binance seed mirror
        "https://bsc-dataseed1.defibit.io",  # T2 — official Binance seed mirror
        "https://bsc-dataseed1.ninicoin.io",  # T2 — official Binance seed mirror
        "https://bsc-mainnet.public.blastapi.io",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://bsc-rpc.publicnode.com",  # T3 — re-added 2026-09-02 during rate-limit crisis (was http_403 on 2026-08-04 — re-probe)
        "https://bsc.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── Polygon (6 endpoints) ─────────────────────────────────────────
    # T2: tatum (audit-verified on polygon), quiknode public
    #     (rpc-mainnet.matic.quiknode.pro — verified 2026-08-05:
    #     blockNumber + topic-only eth_getLogs from tip both OK)
    # T3: polygon-bor.publicnode.com (blockNumber verified 2026-08-07;
    #     topic-only getLogs needs an address filter — fallback only)
    # NOTE: polygon-rpc.blxrbdn.com removed 2026-08-05 — DNS failure
    #       (000) during the 08/05 outage; polygon-rpc.com returns 401
    #       (tenant disabled) and polygon-bor-rpc.publicnode.com requires
    #       an address filter for getLogs, so neither was added.
    #       rejected — publicnode added as T3 fallback instead.
    "polygon": [
        "https://polygon.gateway.tenderly.co",
        "https://poly.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://polygon-mainnet.gateway.tatum.io",
        "https://polygon-bor-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://polygon.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        # rpc-mainnet.matic.network removed 2026-09-13 — DNS dead (Polygon
        # with the real key: eth_chainId 0x89, 2026-09-13) so the
        # rate-limit-critical polygon pool keeps its depth.
        "https://polygon-rpc.com",  # T3 — re-added 2026-09-02 during rate-limit crisis (was 401 on 2026-08-05 — re-probe)
        "https://rpc-mainnet.matic.quiknode.pro",
    ],
    # ── Arbitrum (6 endpoints) ────────────────────────────────────────
    # T2: official arb1.arbitrum.io (verified)
    # T3: arbitrum-one.publicnode.com (blockNumber verified 2026-08-07;
    #     getLogs needs archive token — fallback only), arbitrum.meowrpc
    #     (blockNumber verified 2026-08-07; meowrpc getLogs unsupported —
    #     blockNumber/timestamp fallback only)
    #       rejected — publicnode + meowrpc added as T3 fallbacks.
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",
        "https://arb-one.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://arb1.arbitrum.io/rpc",
        "https://arbitrum-one-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://arbitrum.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        "https://arbitrum.meowrpc.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
    ],
    # ── Optimism (4 endpoints) ────────────────────────────────────────
    # T1: tenderly (verified)
    # T2: official mainnet.optimism.io (verified, occasionally slow),
    "optimism": [
        "https://optimism.gateway.tenderly.co",
        "https://op.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mainnet.optimism.io",
        "https://optimism-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://optimism.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── Avalanche (4 endpoints) ───────────────────────────────────────
    # T2: tenderly gateway (verified 2026-08-03)
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",  # T1 — official (verified)
        "https://avax.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://avalanche.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://avalanche-c-chain-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://avalanche.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── Fantom (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.fantom.network + rpcapi.fantom.network (verified)
    "fantom": [
        "https://rpc.fantom.network",  # T1 — official (verified)
        "https://fantom.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://rpcapi.fantom.network",  # T1 — official-alt (verified)
    ],
    # ── Linea (5 endpoints) ───────────────────────────────────────────
    # T2: tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap —
    #     matches EVM chunk size, verified 2026-08-03)
    "linea": [
        "https://rpc.linea.build",  # T1 — official (verified)
        "https://linea.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://linea.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://linea-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://linea.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        "https://1rpc.io/linea",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
    ],
    # ── Scroll (4 endpoints) ──────────────────────────────────────────
    # T2: 1rpc.io (50-block cap — matches EVM chunk size, verified 2026-08-03)
    "scroll": [
        "https://rpc.scroll.io",  # T1 — official (verified)
        "https://scroll.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://1rpc.io/scroll",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://scroll-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://scroll.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── zkSync Era (4 endpoints) ──────────────────────────────────────
    # T2: tenderly gateway (verified 2026-08-03)
    "zksync": [
        "https://mainnet.era.zksync.io",  # T1 — official (verified)
        "https://zksync-era.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://zksync.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
    ],
    # ── Mantle (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.mantle.xyz (verified)
    #     tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap,
    #     verified 2026-08-03)
    "mantle": [
        "https://rpc.mantle.xyz",  # T1 — official (verified)
        "https://mantle.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://mantle.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://1rpc.io/mantle",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://mantle-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
    ],
    # ── Gnosis (3 endpoints) ──────────────────────────────────────────
    # T2: tenderly gateway (verified 2026-08-03)
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://gnosis.gateway.tenderly.co",
        "https://gnosis-rpc.publicnode.com",  # T3 — probe-verified 2026-08-23 (fallback-only: no topic-only getLogs)
        "https://gnosis.publicnode.com",  # T3 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        # eth_getLogs blocks 1..2 → real logs, 2026-09-13). 3 accounts.
    ],
    # ── Celo (3 endpoints) ────────────────────────────────────────────
    "celo": [
        "https://forno.celo.org",  # T1 — official forno (verified)
        "https://celo.api.pocket.network",  # T1 — Pocket Network (keyless, archive-capable; live-verified eth_chainId 2026-09-13)
        "https://celo.publicnode.com",  # T2 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
    ],
    # ── Blast (verified 2026-09-01) ──────────────────────────────────
    # L2, chainId 81457 (0x13e31). publicnode: chainId ✓, getLogs ✓.
    "blast": [
        "https://blast-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        "https://blast.publicnode.com",  # T1 — keyless publicnode canonical host — live-verified eth_chainId 2026-09-13
        #     keyed fallback so the chain is no longer single-endpoint.
        "https://blast.api.pocket.network",  # T3 — Pocket Network (keyless; live-verified eth_chainId 2026-09-13; getLogs returns [] which is valid on this chain)
    ],
    # ── Fraxtal (verified 2026-09-01) ────────────────────────────────
    # L2, chainId 252. publicnode: chainId ✓, getLogs ✓.
    "fraxtal": [
        "https://fraxtal-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        #     keyed fallback so the chain is no longer single-endpoint.
        "https://fraxtal.api.pocket.network",  # T3 — Pocket Network (keyless, archive-capable; live-verified eth_chainId + eth_getLogs 2026-09-13)
        # 0xfc + archive eth_getLogs blocks 1..2 → real logs, 2026-09-13).
    ],
    # ── Metis (verified 2026-09-01) ──────────────────────────────────
    # L2, chainId 1088. publicnode: chainId ✓, getLogs ✓.
    "metis": [
        "https://metis-rpc.publicnode.com",  # T1 — verified live (getLogs ✓)
        #     keyed fallback so the chain is no longer single-endpoint.
        # T3: Pocket keyless fallback, NON-ARCHIVE for eth_getLogs ("no
        #     source; Pocket is the non-archive tip/blockNumber fallback.
        "https://metis.api.pocket.network",  # T3 — Pocket Network (keyless NON-archive fallback; live-verified eth_chainId 2026-09-13)
        # eth_getLogs blocks 1..2 → real logs, 2026-09-13) — fixes metis's
        # archive gap (Pocket here is non-archive). 3 accounts.
    ],
    # ── Robinhood Chain (2 endpoints, verified 2026-09-01) ────────────
    # Arbitrum Orbit L2, chainId 4663 (0x1237), native ETH. The old spec
    # chainId 78600 was wrong — chainlist mapped it to "Vanguard" (VANRY).
    # LIVE-VERIFIED 2026-09-01:
    #   - robinhood-rpc.publicnode.com: eth_chainId ✓, eth_blockNumber ✓
    #     (block 52M, ~0.5s blocks), eth_getBlockByNumber ✓ — but getLogs
    #     is ARCHIVE-GATED on the free tier (allnodes.com personal token).
    #     eth_getBlockByNumber are "method not available" — limited surface.
    # STATUS: registered but NOT indexed yet — the indexer needs eth_getLogs
    # for ERC-20 Transfer events, which neither free endpoint provides.
    # ACTION: add to the daemon's chain list when a getLogs-capable endpoint
    # ── Robinhood Chain (5 endpoints, verified 2026-09-01) ────────────
    # Arbitrum Orbit L2, chainId 4663 (0x1237), native ETH, ~0.5s blocks.
    # LIVE-VERIFIED 2026-09-01:
    #   - rpc.mainnet.chain.robinhood.com (OFFICIAL): chainId ✓,
    #     blockNumber ✓ (block 52M), getBlockByNumber ✓, getLogs ✓ —
    #     FULLY INDEXABLE.
    #   - robinhood-rpc.publicnode.com: chainId ✓, blockNumber ✓ —
    #     getLogs archive-gated on free tier.
    #   - rpc.nodeflare.app/robinhood/public: chainId ✓ (getLogs untested).
    #   - robinhood.rpc.blxrbdn.com: chainId ✓ but getLogs 403 Forbidden.
    #     "method not available" — limited surface.
    "robinhood": [
        "https://rpc.mainnet.chain.robinhood.com",  # T1 — OFFICIAL, fully indexable
        # eth_getLogs blocks 1..2 → REAL logs, 2026-09-13). Sits beside the
        # only non-official archive source for this chain; kept high-priority.
        "https://robinhood.api.pocket.network",  # T3 — Pocket Network (keyless, probed live 2026-09-13: eth_chainId 0x1237 == 4663)
        "https://robinhood-rpc.publicnode.com",  # T2 — verified (getLogs archive-gated)
        "https://rpc.nodeflare.app/robinhood/public",  # T3 — verified chainId
        "https://robinhood.rpc.blxrbdn.com",  # T3 — chainId only (getLogs 403)
    ],
}

# Summary of PUBLIC (free/keyless) EVM endpoints per chain:
#   ethereum: 8   base: 6   bsc: 10   polygon: 7
#   arbitrum: 6   optimism: 5   avalanche: 5   fantom: 3
#   linea: 6   scroll: 5   zksync: 3   mantle: 5
#   gnosis: 5   celo: 3   blast: 3   fraxtal: 2
#   metis: 2   robinhood: 5
# -------------------------------------------------------
#   Total builtin public templates: 89 across 18 chains
#   Curated/keyed providers are supplied by the PRIVATE runtime overlay
#   (see lite_indexer/chains/_overlay.py + docs/PROVIDER_OVERLAY.md).
#   With no overlay set, only these free/keyless endpoints are used.
#
# 2026-09-13: PublicNode canonical keyless hosts added for all indexed
#   chains where they exist: https://<chain>.publicnode.com (ethereum, base,
#   bsc, polygon, arbitrum, optimism, avalanche, linea, scroll, gnosis, celo,
#   blast) — each live-verified eth_chainId here. fraxtal/metis/robinhood
#   have no canonical <name> host (403 "unsupported platform") so they keep
#   their working <name>-rpc.publicnode.com entries. Solana/Tron canonical
#   hosts added in non_evm.py.

# ── Candidate Endpoints (awaiting live verification) ────────────────
# Unverified additions from chainlist cross-pollination + operator tips.
# NOT part of the active pool — scripts/probe-rpc-endpoints.py probes
# these with eth_chainId + eth_blockNumber (+ optional topic-only
# eth_getLogs range check) and emits ready-to-paste promoted snippets.
# Promotion criteria: chainId matches, blockNumber OK, latency < 1500ms,
# and (for this indexer) topic-only eth_getLogs over a small tip range.
# Rejected candidates stay documented in the probe report, never silently.
CANDIDATE_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth.llamarpc.com",  # re-probe: 521 on 2026-08-05, provider since stabilized?
        "https://1rpc.io/eth",  # re-probe: usage-limited 2026-08-05
        "https://eth.meowrpc.com",  # getLogs unsupported — blockNumber fallback only
        "https://cloudflare-eth.com",  # reported dead 2026-08-05 — confirm before cull
    ],
    "base": [
        "https://base.llamarpc.com",
        "https://base-meow.rpc.thirdweb.com",
    ],
    "bsc": [
        "https://bsc.meowrpc.com",
        "https://bsc-blockchain.rpc.thirdweb.com",
        # binance.llamarpc.com removed 2026-09-13 — DNS dead.
    ],
    "polygon": [
        # polygon.llamarpc.com + polygon.meowrpc.com removed 2026-09-13 — DNS dead.
        "https://polygon-mainnet.public.blastapi.io",
        # as flaky (HTTP 521 on 2026-08-07); do not promote to the active pool.
    ],
    "arbitrum": [
        # arbitrum.llamarpc.com removed 2026-09-13 — DNS dead.
        # (live-verified eth_chainId 0xa4b1 with the real key).
    ],
    "optimism": [
        # optimism.llamarpc.com + optimism.meowrpc.com removed 2026-09-13 — DNS dead.
        # opt-mainnet.public.blastapi.io removed 2026-09-13 — DNS resolves but
        # serves no RPC (HTTP 000), i.e. non-functional, not dead-DNS.
    ],
    "avalanche": [
        # avalanche.mainnet.nodies.app removed 2026-09-13 — DNS dead
        # (active avalanche pool already covers the chain).
    ],
    "fantom": [
        "https://fantom-mainnet.public.blastapi.io",
    ],
    "linea": [],
    "scroll": [
        "https://scroll-mainnet.public.blastapi.io",
    ],
    "zksync": [
        # mainnet.era.zksync.network removed 2026-09-13 — DNS dead
        # (mainnet.era.zksync.io remains in the active pool).
    ],
    "mantle": [],
    "gnosis": [
        "https://gnosis.blockscout.com",  # likely blockNumber-only
    ],
    "celo": [
        # celo-mainnet.public.blastapi.io removed 2026-09-13 — DNS resolves
        # but serves no RPC (HTTP 000); active celo pool already covers it.
    ],
}


# ── Endpoint URL template resolution ───────────────────────────────
# ${GETBLOCK_KEY}, ...). They are resolved at import time so the main
# RPC path never dials dead placeholder URLs. Endpoints whose env var is
# NOT set are excluded from the active pool entirely — a URL containing a
# literal ``${...}`` can never be dialed (DNS/JSON errors) and only
# produces verification retry noise (verify_endpoint_failed storms). Set
# the corresponding env var in the service environment and restart to
# activate those endpoints.


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
}


def _resolve_endpoint_url(url: str) -> str:
    """Resolve ${ENV_VAR} placeholders in an endpoint URL.

    Uses :func:`os.path.expandvars` so both ``${VAR}`` and ``$VAR`` forms
    are handled. An unset variable leaves the placeholder in the returned
    URL so callers can detect and skip it.

    Args:
        url: Endpoint URL potentially containing ${ENV_VAR} templates.

    Returns:
        URL with env vars resolved (placeholder kept if unset).
    """
    return os.path.expandvars(url)


def _resolved_endpoints(urls: list[str]) -> list[str]:
    """Resolve env-var placeholders and drop endpoints that could not expand.

    Endpoints whose ``${ENV_VAR}`` placeholder is not set in the
    environment are excluded from the active pool: a URL containing a
    literal ``${...}`` can never be dialed (DNS/JSON errors) and only
    produces verification retry noise. Set the env var (and restart) to
    activate the endpoint.

    Args:
        urls: Raw endpoint URL templates.

    Returns:
        URLs with env vars expanded, minus unresolved placeholders.
    """
    resolved: list[str] = []
    for url in urls:
        expanded = os.path.expandvars(url)
        if "${" in expanded:
            logger.debug("endpoint_unresolved_env_skipped", url=url)
            continue
        resolved.append(expanded)
    return resolved


# Capture the builtin (public, free/keyless) templates for introspection,
# then append the private runtime overlay (no-op when absent).
BUILTIN_ENDPOINTS: dict[str, list[str]] = dict(GET_LOGS_ENDPOINTS)
_ENDPOINT_OVERLAY = load_endpoint_overlay()
# Only EVM chains belong in this dict — the overlay also carries solana/tron
# entries for the non_evm module, which must not leak into GET_LOGS_ENDPOINTS.
_OVERLAY_EXTRA = {chain: entry for chain, entry in _ENDPOINT_OVERLAY.items() if chain in BUILTIN_ENDPOINTS}
GET_LOGS_ENDPOINTS = merge_endpoints(BUILTIN_ENDPOINTS, _OVERLAY_EXTRA)
GET_LOGS_ENDPOINTS = {chain: _resolved_endpoints(urls) for chain, urls in GET_LOGS_ENDPOINTS.items()}

# ── Last-Resort Endpoints ───────────────────────────────────────────────
# Known-good endpoints tried when every endpoint in the active pool fails
# for a chain. These are the most battle-tested public providers from the
# fallback only — the regular pool's health scoring / bans / circuit
# breakers still apply; the last-resort list exists so a total pool wipe
# (e.g. an outage taking out every tier at once) still has a shot at
# serving the range instead of raising RpcFetchFailedError immediately.
LAST_RESORT_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth.api.onfinality.io/public",
        "https://eth-pokt.nodies.app",
        "https://rpc.mevblocker.io",
        "https://rpc.flashbots.net",
    ],
    "base": [
        "https://base.gateway.tenderly.co",
        "https://mainnet.base.org",
    ],
    "bsc": [
        "https://rpc-bsc.48.club",
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.defibit.io",
    ],
    "polygon": [
        "https://polygon.gateway.tenderly.co",
        "https://polygon-mainnet.gateway.tatum.io",
        "https://rpc-mainnet.matic.quiknode.pro",
    ],
    "arbitrum": [
        "https://arbitrum.gateway.tenderly.co",
        "https://arb1.arbitrum.io/rpc",
    ],
    "optimism": [
        "https://optimism.gateway.tenderly.co",
        "https://mainnet.optimism.io",
    ],
    "avalanche": ["https://api.avax.network/ext/bc/C/rpc"],
    "fantom": ["https://rpc.fantom.network"],
    "zksync": ["https://mainnet.era.zksync.io"],
    "mantle": ["https://rpc.mantle.xyz"],
    "gnosis": [
        "https://rpc.gnosischain.com",
        "https://gnosis.gateway.tenderly.co",
    ],
    "celo": ["https://forno.celo.org"],
    "robinhood": ["https://rpc.mainnet.chain.robinhood.com"],
}


def last_resort_endpoints(chain: str) -> list[str]:
    """Return the known-good last-resort endpoints for a chain.

    Resolves ``${ENV_VAR}`` placeholders and drops unresolved ones, same
    as the main pool, so a last-resort URL that needs a key that isn't
    set is never dialed.

    Args:
        chain: Chain name.

    Returns:
        List of dialable fallback endpoint URLs (may be empty).
    """
    merged = merge_endpoints({chain: list(LAST_RESORT_ENDPOINTS.get(chain, []))}, _ENDPOINT_OVERLAY)
    return _resolved_endpoints(merged.get(chain, []))
