"""
Chain rate limits and block range caps.

Per-provider rate limits for adaptive throttling and chain-specific
eth_getLogs block range caps.
"""

from __future__ import annotations

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
    "nodies.app": {"req_per_sec": 5, "burst": 10},
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
# NOTE: These limits are enforced by enforce_block_range + the
# eth_getLogs handlers — do not remove them as "dead" config.

MAX_BLOCK_RANGE: dict[str, int | None] = {
    # High range (10000+ or unlimited)
    "drpc.org": 10000,
    "binance.org": None,
    "cloudflare-eth.com": 5000,
    "ankr.com": 2000,
    # Medium range (100-500)
    "publicnode.com": 1000,
    "onfinality.io": 2000,
    "nodies.app": 50,  # 50-block getLogs cap (verified 2026-08-05) — matches CHUNK_SIZE_EVM
    "tatum.io": 100,
    "mevblocker.io": 2000,
    # Low range (10-50)
    "1rpc.io": 10,
    "blastapi.io": 10,
    "blockrazor.xyz": 25,
    "tenderly.co": 5000,
    # Unlimited / unknown
    "meowrpc.com": None,
    "merkle.io": None,  # getLogs unsupported (-32601) — blockNumber/tip fallback only
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
    "robinhood": 1000,  # New Orbit L2 (~0.25s blocks) — conservative until RPC live-verified
}
