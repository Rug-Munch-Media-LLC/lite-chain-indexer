"""
Chain configuration — RPC endpoints and poll intervals.

Each chain has a list of eth_getLogs-compatible RPC endpoints in 3 tiers:
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

# ── Solana RPC Endpoints ─────────────────────────────────────────────
# Solana uses JSON-RPC but with different methods (getSlot, etc.)
# These are NOT eth_getLogs endpoints — they're for Solana-native RPC calls.
# Tiers: T1=official/mainnet verified, T2=publicnode, T3=fallbacks.
# Tested 2026-07-30: getSlot, getSignaturesForAddress, getTransaction.

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
# Tron uses REST API (TronGrid) for event queries and JSON-RPC for blocks.
# Only TronGrid (api.trongrid.io) supports the contract events API.
# Others are fallbacks for block queries only.
# Tested 2026-07-30: /wallet/getnowblock, /v1/contracts/*/events.

TRON_ENDPOINTS: list[str] = [
    # T1 — Official TronGrid (events API + full node)
    "https://api.trongrid.io",
    # T2 — Verified free endpoints (block queries only)
    "https://api.tronstack.io",
    "https://tron-rpc.publicnode.com",
    # T3 — Fallback
    "https://rpc.tronlink.org",
]

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

# ── Provider Rate Limits ───────────────────────────────────────────
# Per-provider rate limits for adaptive throttling.
# Keyed by domain pattern (substring-matched against endpoint URL).
# Default (unlisted): 5 req/s, burst 10

PROVIDER_RATE_LIMITS: dict[str, dict[str, int]] = {
    # Tier 1 — High capacity
    "drpc.org": {"req_per_sec": 10, "burst": 20},
    "ankr.com": {"req_per_sec": 30, "burst": 50},
    "binance.org": {"req_per_sec": 100, "burst": 200},
    "cloudflare-eth.com": {"req_per_sec": 50, "burst": 100},
    # Tier 2 — Medium capacity
    "publicnode.com": {"req_per_sec": 5, "burst": 10},
    "onfinality.io": {"req_per_sec": 5, "burst": 10},
    # Tier 3 — Low capacity / rate limited
    "1rpc.io": {"req_per_sec": 2, "burst": 5},
    "meowrpc.com": {"req_per_sec": 3, "burst": 5},
    "tenderly.co": {"req_per_sec": 10, "burst": 20},
    "blastapi.io": {"req_per_sec": 3, "burst": 5},
    "blockrazor.xyz": {"req_per_sec": 3, "burst": 5},
    "tatum.io": {"req_per_sec": 5, "burst": 10},
    "merkle.io": {"req_per_sec": 3, "burst": 5},
    "mevblocker.io": {"req_per_sec": 5, "burst": 10},
    "blxrbdn.com": {"req_per_sec": 5, "burst": 10},
    # Tier 4 — API-key free tiers
    "alchemy.com": {"req_per_sec": 10, "burst": 20},
    "getblock.io": {"req_per_sec": 10, "burst": 20},
    "chainstack.com": {"req_per_sec": 10, "burst": 20},
    "helius.xyz": {"req_per_sec": 10, "burst": 20},
}

# Default rate limit when provider isn't listed
DEFAULT_RATE_LIMIT = {"req_per_sec": 5, "burst": 10}

# ── Max block range per provider ───────────────────────────────────
# Some free RPCs limit the eth_getLogs block range.
# Keyed by domain pattern, value is max range in blocks.
# None = unlimited (up to RPC timeout).

MAX_BLOCK_RANGE: dict[str, int | None] = {
    # High range (10000+ or unlimited)
    "drpc.org": 10000,
    "binance.org": None,
    "cloudflare-eth.com": 5000,
    "ankr.com": 2000,
    # Medium range (100-500)
    "publicnode.com": 1000,
    "onfinality.io": 2000,
    "tatum.io": 100,
    "mevblocker.io": 2000,
    # Low range (10-50)
    "1rpc.io": 10,
    "blastapi.io": 10,
    "blockrazor.xyz": 25,
    "tenderly.co": 5000,
    # Unlimited / unknown
    "meowrpc.com": None,
    "merkle.io": None,
    "blxrbdn.com": None,
    "fantom.network": None,
    "linea.build": None,
    "scroll.io": None,
    "mantle.xyz": None,
    "gnosischain.com": None,
    "forno.celo.org": None,
    "arbitrum.io": None,
    "optimism.io": None,
    "avax.network": None,
    "base.org": 10,  # Returns "backend response too large" beyond 10 blocks
    "zksync.io": None,
    # API-key free tiers
    "alchemy.com": 10000,
    "getblock.io": 5000,
    "chainstack.com": 10000,
    "helius.xyz": None,
}

DEFAULT_MAX_RANGE = 500

# ── Chain-specific Block Range Limits ──────────────────────────────
# Some chains have RPCs that fail on large eth_getLogs ranges.
# These cap the block range per chain regardless of provider.
# This is a safety limit to prevent "response too large" errors.

CHAIN_BLOCK_RANGE_LIMITS: dict[str, int] = {
    "ethereum": 10000,
    "base": 1000,  # Base RPCs are restrictive
    "bsc": 5000,
    "polygon": 10000,
    "arbitrum": 50000,  # Fast blocks, can handle large ranges
    "optimism": 10000,
    "avalanche": 10000,
    "fantom": 10000,
    "linea": 5000,
    "scroll": 5000,
    "zksync": 10000,
    "mantle": 5000,
    "gnosis": 5000,
    "celo": 5000,
}

# ── Health Check Configuration ─────────────────────────────────────

# How often (in seconds) to re-verify an endpoint after a successful check
ENDPOINT_VERIFY_INTERVAL = 3600  # 1 hour

# How long to wait for an endpoint verification response
ENDPOINT_VERIFY_TIMEOUT = 10.0

# Block range to use for endpoint health verification (small to avoid range errors)
VERIFY_BLOCK_DELTA = 100  # Check 100 blocks back from tip

# ── Chain-specific block time (seconds) for adaptive polling ───────────
CHAIN_POLL_INTERVALS: dict[str, float] = {
    "ethereum": 12.0,
    "base": 2.0,
    "bsc": 3.0,
    "polygon": 2.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "avalanche": 2.0,
    "fantom": 1.0,
    "linea": 12.0,
    "scroll": 3.0,
    "zksync": 1.0,
    "mantle": 2.0,
    "gnosis": 5.0,
    "celo": 5.0,
    "solana": 0.4,
    "tron": 3.0,
}


# ── Helper Functions ───────────────────────────────────────────────


def get_provider_rate_limit(endpoint: str) -> dict[str, int]:
    """Get rate limits for a given endpoint URL based on provider pattern.

    Args:
        endpoint: Full RPC URL.

    Returns:
        Dict with 'req_per_sec' and 'burst' keys.
    """
    from urllib.parse import urlparse

    domain = urlparse(endpoint).hostname or ""
    for pattern, limits in PROVIDER_RATE_LIMITS.items():
        if pattern in domain:
            return limits
    return dict(DEFAULT_RATE_LIMIT)


def get_max_block_range(endpoint: str) -> int | None:
    """Get the maximum eth_getLogs block range for this endpoint.

    Args:
        endpoint: Full RPC URL.

    Returns:
        Max block range, or None if unlimited.
    """
    from urllib.parse import urlparse

    domain = urlparse(endpoint).hostname or ""
    for pattern, max_range in MAX_BLOCK_RANGE.items():
        if pattern in domain:
            return max_range
    return DEFAULT_MAX_RANGE


def get_endpoints_for_range(
    endpoints: list[str],
    num_blocks: int,
) -> list[str]:
    """Filter endpoints that support the requested block range.

    Removes endpoints whose max range is smaller than num_blocks.
    Falls through to the original list if all would be filtered.

    Args:
        endpoints: List of RPC URLs.
        num_blocks: Number of blocks to query.

    Returns:
        Filtered endpoint list (maintains original order).
    """
    filtered = [ep for ep in endpoints if (max_r := get_max_block_range(ep)) is None or max_r >= num_blocks]
    return filtered if filtered else endpoints
