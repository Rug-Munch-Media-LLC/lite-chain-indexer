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
celo) were removed. 1rpc.io, tatum (except bsc/polygon), cloudflare-eth
and polygon-rpc are confirmed dead and removed.

2026-08-05: onfinality (eth.api.onfinality.io/public) re-added as Ethereum
T1 after 429-ban during 50-block catch-up; blastapi re-added as T5 —
both verified against 10-block eth_getLogs chunks from tip. Keep
Ethereum chunks small (ETH_BATCH_SIZE=10) so free-tier range caps
(Alchemy/blastapi = 10 blocks) and the ~20K result cap are respected.

Some endpoints require an API key from a free-tier provider (Alchemy,
GetBlock, Chainstack, QuickNode, Infura). Set the corresponding ${ENV_VAR}
in your environment — the indexer resolves placeholders at import time via
os.path.expandvars (chains.endpoints._resolved_endpoints()). Endpoints
whose key is NOT set are excluded from the active pool (they can never be
dialed — a literal ``${...}`` in a URL only produces DNS/JSON verification
retries). Restart the service after setting a key to activate the endpoint.

Total: 60 endpoints across 14 chains (48 public + 12 API-key free tiers).
"""

from __future__ import annotations

import os

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

GET_LOGS_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum (9 endpoints) ────────────────────────────────────────
    # T1: onfinality public (verified 2026-08-05 — eth_getLogs OK at
    #     10-block chunks from tip; 429-banned on 50-block catch-up,
    #     keep chunks small via ETH_BATCH_SIZE=10)
    # T2: nodereal public demo key (works, but 429-bans under parallel load)
    # T3: drpc.org (degraded — ~75% 408 free-plan timeout, keep as fallback),
    #     flashbots (MEV-protected; ranged topic-only getLogs errors)
    # T4: alchemy, getblock, chainstack, quicknode (free API-key tiers)
    # T5: blastapi (10-block range cap — OK with ETH_BATCH_SIZE=10)
    # NOTE: mevblocker (10k-result cap) does not serve the 10-block chunk
    #       shape and was not added. publicnode requires an address filter.
    "ethereum": [
        "https://eth.api.onfinality.io/public",  # T1 — onfinality (verified, fast)
        "https://eth-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",  # T2 — nodereal public
        "https://eth.drpc.org",  # T3 — drpc.org (degraded, fallback)
        "https://rpc.flashbots.net",  # T3 — flashbots (degraded, fallback)
        # T4: API-key free tiers (set env vars to activate)
        "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free (300M CU/mo, 10-block getLogs cap)
        "https://eth.getblock.io/mainnet/?api_key=${GETBLOCK_KEY}",  # T4 — GetBlock free (40K req/day)
        "https://ethereum-mainnet.core.chainstack.com/${CHAINSTACK_KEY}",  # T4 — Chainstack free
        "https://${QUICKNODE_ENDPOINT}",  # T4 — QuickNode free
        "https://eth-mainnet.public.blastapi.io",  # T5 — blastapi (10-block cap, verified 2026-08-05)
    ],
    # ── Base (5 endpoints) ────────────────────────────────────────────
    # T1: tenderly, official mainnet.base.org (both verified eth_getLogs)
    # T2: drpc.org, 1rpc.io (50-block cap — matches EVM chunk size, verified 2026-08-03)
    # T4: alchemy
    # NOTE: publicnode (needs address filter) and blastapi (HTTP400) do not
    #       serve topic-only eth_getLogs on free tier and were removed.
    "base": [
        "https://base.gateway.tenderly.co",  # T1 — tenderly (verified, fast)
        "https://mainnet.base.org",  # T1 — official (verified)
        "https://base.drpc.org",  # T2 — drpc.org (verified)
        "https://1rpc.io/base",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── BSC (4 endpoints) ─────────────────────────────────────────────
    # T1: nodereal public, 48.club (live-verified 2026-08-04 with the
    #     exact indexer request shape: topic-only eth_getLogs over a
    #     100-block range from tip — both returned full result sets)
    # T2: blockrazor (25-block range cap — small-range fallback,
    #     registered in limits.py so the indexer caps chunks for it)
    # T4: alchemy
    # NOTE: tatum (range-limit + 429), binance seeds (limit exceeded),
    #       bsc-rpc.publicnode.com (http_403 on ranged topic-only —
    #       4.7k rejections during 08/04 outage), merkle (HTTP400),
    #       ankr (unauthorized), 1rpc.io (usage limit) removed 2026-08-04.
    "bsc": [
        "https://bsc-mainnet.nodereal.io/v1/64a9df0874fb4a93b9d0a3849de012d3",  # T1 — nodereal public (verified, 25.8k logs/100-blk)
        "https://rpc-bsc.48.club",  # T1 — 48.club (verified)
        "https://bsc.blockrazor.xyz",  # T2 — blockrazor (25-block range cap)
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
    # ── Optimism (4 endpoints) ────────────────────────────────────────
    # T1: tenderly (verified)
    # T2: official mainnet.optimism.io (verified, occasionally slow),
    #     drpc.org (verified 2026-08-03)
    # T4: alchemy
    "optimism": [
        "https://optimism.gateway.tenderly.co",  # T1 — tenderly (verified)
        "https://mainnet.optimism.io",  # T2 — official (verified)
        "https://optimism.drpc.org",  # T2 — drpc.org (verified 2026-08-03)
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Avalanche (4 endpoints) ───────────────────────────────────────
    # T1: official api.avax.network, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    # T4: infura (free tier)
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",  # T1 — official (verified)
        "https://avalanche.drpc.org",  # T1 — drpc.org (verified)
        "https://avalanche.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
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
    # ── Linea (5 endpoints) ───────────────────────────────────────────
    # T1: official rpc.linea.build, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap —
    #     matches EVM chunk size, verified 2026-08-03)
    # T4: infura (free tier)
    "linea": [
        "https://rpc.linea.build",  # T1 — official (verified)
        "https://linea.drpc.org",  # T1 — drpc.org (verified)
        "https://linea.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://1rpc.io/linea",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://linea-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
    ],
    # ── Scroll (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.scroll.io, drpc.org (verified)
    # T2: 1rpc.io (50-block cap — matches EVM chunk size, verified 2026-08-03)
    # T4: infura (free tier)
    "scroll": [
        "https://rpc.scroll.io",  # T1 — official (verified)
        "https://scroll.drpc.org",  # T1 — drpc.org (verified)
        "https://1rpc.io/scroll",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
        "https://scroll-mainnet.infura.io/v3/${INFURA_KEY}",  # T4 — Infura free
    ],
    # ── zkSync Era (4 endpoints) ──────────────────────────────────────
    # T1: official mainnet.era.zksync.io, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    # T4: alchemy (zksync-era)
    "zksync": [
        "https://mainnet.era.zksync.io",  # T1 — official (verified)
        "https://zksync.drpc.org",  # T1 — drpc.org (verified)
        "https://zksync.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://zksync-era.g.alchemy.com/v2/${ALCHEMY_KEY}",  # T4 — Alchemy free
    ],
    # ── Mantle (4 endpoints) ──────────────────────────────────────────
    # T1: official rpc.mantle.xyz (verified)
    # T2: drpc.org (verified; free-plan usage limit under burst),
    #     tenderly gateway (verified 2026-08-03), 1rpc.io (50-block cap,
    #     verified 2026-08-03)
    "mantle": [
        "https://rpc.mantle.xyz",  # T1 — official (verified)
        "https://mantle.drpc.org",  # T2 — drpc.org (verified)
        "https://mantle.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
        "https://1rpc.io/mantle",  # T2 — 1rpc.io (50-block cap, verified 2026-08-03)
    ],
    # ── Gnosis (3 endpoints) ──────────────────────────────────────────
    # T1: official rpc.gnosischain.com, drpc.org (verified)
    # T2: tenderly gateway (verified 2026-08-03)
    "gnosis": [
        "https://rpc.gnosischain.com",  # T1 — official (verified)
        "https://gnosis.drpc.org",  # T1 — drpc.org (verified)
        "https://gnosis.gateway.tenderly.co",  # T2 — tenderly (verified 2026-08-03)
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
#   ethereum: 7     base: 5      bsc: 4      polygon: 5
#   arbitrum: 4     optimism: 4  avalanche: 4 fantom: 4
#   linea: 5        scroll: 4    zksync: 4   mantle: 4
#   gnosis: 3       celo: 3
# -------------------------------------------------------
#   Total: 60 endpoints across 14 chains
#   (48 free public + 12 API-key free tiers)
#
#   Non-EVM: solana (4), tron (3)
#   Grand total: 67 endpoints across 16 chains


# ── Endpoint URL template resolution ───────────────────────────────
# API-key free tiers use ${ENV_VAR} placeholders (${ALCHEMY_KEY},
# ${GETBLOCK_KEY}, ...). They are resolved at import time so the main
# RPC path never dials dead placeholder URLs. Endpoints whose env var is
# NOT set are excluded from the active pool entirely — a URL containing a
# literal ``${...}`` can never be dialed (DNS/JSON errors) and only
# produces verification retry noise (verify_endpoint_failed storms). Set
# the corresponding env var in the service environment and restart to
# activate those endpoints.


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


GET_LOGS_ENDPOINTS = {chain: _resolved_endpoints(urls) for chain, urls in GET_LOGS_ENDPOINTS.items()}

# ── Last-Resort Endpoints ───────────────────────────────────────────────
# Known-good endpoints tried when every endpoint in the active pool fails
# for a chain. These are the most battle-tested public providers from the
# live-verified sets above (official RPCs + drpc.org). They are a final
# fallback only — the regular pool's health scoring / bans / circuit
# breakers still apply; the last-resort list exists so a total pool wipe
# (e.g. an outage taking out every tier at once) still has a shot at
# serving the range instead of raising RpcFetchFailedError immediately.
LAST_RESORT_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": ["https://eth.api.onfinality.io/public", "https://eth.drpc.org", "https://rpc.flashbots.net"],
    "base": ["https://mainnet.base.org", "https://base.drpc.org"],
    "bsc": ["https://rpc-bsc.48.club"],
    "polygon": ["https://polygon.drpc.org", "https://polygon-rpc.blxrbdn.com"],
    "arbitrum": ["https://arb1.arbitrum.io/rpc", "https://arbitrum.drpc.org"],
    "optimism": ["https://mainnet.optimism.io", "https://optimism.drpc.org"],
    "avalanche": ["https://api.avax.network/ext/bc/C/rpc"],
    "fantom": ["https://rpc.fantom.network"],
    "linea": ["https://rpc.linea.build", "https://linea.drpc.org"],
    "scroll": ["https://rpc.scroll.io", "https://scroll.drpc.org"],
    "zksync": ["https://mainnet.era.zksync.io"],
    "mantle": ["https://rpc.mantle.xyz"],
    "gnosis": ["https://rpc.gnosischain.com", "https://gnosis.drpc.org"],
    "celo": ["https://forno.celo.org"],
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
    return _resolved_endpoints(LAST_RESORT_ENDPOINTS.get(chain, []))
