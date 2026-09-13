"""Freemium RPC provider registry — free-first fallbacks with referral metadata.

Design (2026-08-23):

- The indexer ALWAYS works on the keyless public pool
  (:data:`lite_indexer.chains.endpoints.GET_LOGS_ENDPOINTS` T1-T3).
- Keyed freemium providers are the *recommended* upgrade path: better rate
  limits and getLogs depth for free, and — when a self-hoster signs up via
  our referral links — they fund continued OSS development at zero cost to
  them.
- **Key hygiene**: this registry contains NO secrets. Endpoints reference
  keys exclusively through ``${ENV_VAR}`` placeholders (resolved by
  :func:`os.path.expandvars` in ``chains.endpoints``); unresolved
  placeholders are skipped by the endpoint verifier. Signup URLs ship as
  ``SET_ME`` placeholders until the project publishes its referral links —
  see docs/RPC_PROVIDERS.md.

Referral-program facts below were verified against public program pages
2026-08; percentages are the providers' published terms and may change.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FreemiumProvider:
    """A keyed freemium RPC provider with optional referral monetization."""

    name: str
    env_key: str  # ${...} placeholder used in endpoint URLs
    url_example: str  # template WITH the ${ENV_VAR} placeholder — never a real key
    chains_note: str
    free_tier: str
    referral_terms: str
    signup_url: str = "SET_ME"  # replaced with the project's referral link
    has_referral_program: bool = True


FREEMIUM_PROVIDERS: dict[str, FreemiumProvider] = {
    p.name: p
    for p in [
        FreemiumProvider(
            name="QuickNode",
            env_key="QUICKNODE_KEY",
            url_example="https://{endpoint-host}.quiknode.pro/${QUICKNODE_KEY}/",
            chains_note=(
                "83+ chains incl. Solana + Bitcoin. The bare QUICKNODE_KEY is "
                "NOT URL-embeddable (verified 2026-09-13: no endpoint host to "
                "pair it with) — QuickNode provisions per-endpoint URLs of the "
                "form https://<id>.quiknode.pro/<key>/, so a full endpoint URL "
                "env var is required. No QUICKNODE_*_URL is provisioned yet."
            ),
            free_tier="10M credits/mo",
            referral_terms="50% of first month (credits); 15% recurring on dev subs; 90d cookie",
        ),
        FreemiumProvider(
            name="Chainstack",
            # Chainstack provisions PER-NODE full-URL endpoints, not a single
            # account-wide key. Separate accounts per chain: the env vars are
            # CHAINSTACK_SOLANA_URL and CHAINSTACK_TRON_URL (live-verified
            # 2026-09-13). Node endpoints may require HTTP Basic auth, which
            # Chainstack embeds as userinfo in the URL
            # (https://<user>:<pass>@<node>.core.chainstack.com/<path>).
            env_key="CHAINSTACK_<CHAIN>_URL",
            url_example="https://<node>.core.chainstack.com/<path>",
            chains_note=(
                "Major EVMs + Solana + Tron; per-node full URLs via "
                "CHAINSTACK_SOLANA_URL / CHAINSTACK_TRON_URL (separate "
                "accounts). May require HTTP Basic auth (userinfo in URL). "
                "CHAINSTACK_API_KEY (Management API Bearer) verified "
                "2026-09-13: GET api.chainstack.com/v1/nodes/ → 200 (1 shared "
                "node); the provisioned per-node URLs answer live."
            ),
            free_tier="3M requests/mo (Developer)",
            referral_terms="10% of referred purchases for 1 year (credits)",
        ),
        FreemiumProvider(
            name="GetBlock",
            env_key="GETBLOCK_KEY",
            url_example="https://go.getblock.io/${GETBLOCK_KEY}/",
            chains_note="50+ chains, dedicated-node options",
            free_tier="40k requests (trial)",
            referral_terms="1-15% cash (deal-size dependent)",
        ),
        FreemiumProvider(
            name="NOWNodes",
            env_key="NOWNODES_KEY",
            url_example="https://btc.nownodes.io/api/v2/...?api-key=${NOWNODES_KEY}",
            chains_note="100+ networks, explorers included",
            free_tier="20k requests/mo",
            referral_terms="5% cash or 10% credits (partner's choice)",
        ),
        FreemiumProvider(
            name="Tatum",
            env_key="TATUM_KEY",
            url_example="https://api.tatum.io/v3/... (x-api-key: ${TATUM_KEY})",
            chains_note="130+ chains via unified API",
            free_tier="500k credits/mo",
            referral_terms="up to 15% cash",
        ),
        FreemiumProvider(
            name="Alchemy",
            env_key="ALCHEMY_KEY",
            url_example="https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
            chains_note="90+ networks, archive + debug traces",
            free_tier="30M CU/mo",
            referral_terms="developer referral program (credits)",
        ),
        FreemiumProvider(
            name="dRPC",
            env_key="DRPC_KEY",
            url_example="https://lb.drpc.org/ogrpc?network=ethereum&dkey=${DRPC_KEY}",
            chains_note="115 chains / 203 networks, load-balanced across 60+ providers",
            free_tier="210M CU / 30 days",
            referral_terms="partnership program (contact dRPC)",
        ),
        FreemiumProvider(
            name="Ankr",
            env_key="ANKR_KEY",
            url_example="https://rpc.ankr.com/eth/${ANKR_KEY}",
            chains_note=(
                "80+ networks (Premium); the provisioned free key works ONLY "
                "for eth/bsc/polygon/arbitrum/avalanche/base/celo/gnosis — "
                "HTTP 403 elsewhere (optimism/fantom/linea/blast/fraxtal/metis)"
            ),
            free_tier="200M credits/mo (Freemium)",
            referral_terms="no public affiliate program — recommend on merit",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="Pocket Network",
            env_key="",
            url_example="https://eth.api.pocket.network",
            chains_note=(
                "KEYLESS public RPC — 60+ chains (EVM + solana/tron), mostly "
                "archive-capable; api.pocket.network portal; no key, no signup"
            ),
            free_tier="2000 req/IP/min",
            referral_terms="no public affiliate program — recommend on merit",
            signup_url="https://api.pocket.network",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="Infura",
            env_key="INFURA_KEY",
            url_example="https://mainnet.infura.io/v3/${INFURA_KEY}",
            chains_note=(
                "13 of our chains (eth/bsc/polygon/arbitrum/optimism/base/"
                "linea/scroll/avalanche/zksync/blast/mantle/celo); archive-capable"
            ),
            free_tier="3M credits/day (free tier)",
            referral_terms="no public affiliate program — recommend on merit",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="Helius",
            env_key="HELIUS_KEY",
            url_example="https://mainnet.helius-rpc.com/?api-key=${HELIUS_KEY}",
            chains_note=(
                "Solana specialist (LaserStream gRPC, DAS API); 3 INDEPENDENT "
                "free accounts wired for capacity + failover: ${HELIUS_KEY}, "
                "${HELIUS_KEY_2}, ${HELIUS_KEY_3} — all getHealth/getSlot "
                "live-verified on mainnet.helius-rpc.com 2026-09-13 "
                "(legacy rpc.helius.xyz now 403s)."
            ),
            free_tier="1M credits/mo (10 RPS) per account",
            referral_terms="creator/partner program (contact Helius)",
        ),
        FreemiumProvider(
            name="TronGrid",
            # TronGrid's key is accepted as the TRON-PRO-API-KEY header OR a
            # path-scoped ?api_key= query param. It is NOT embeddable as a
            # base URL (https://api.trongrid.io?api_key=… → 405) and the Tron
            # transport does f"{base_url}{path}", so no pool template is
            # registered — needs strategy-level query-param support.
            env_key="TRONGRID_API_KEY",
            url_example="https://api.trongrid.io/wallet/getnowblock?api_key=${TRONGRID_API_KEY}",
            chains_note=(
                "Tron official (contract events API). Key accepted as header "
                "TRON-PRO-API-KEY or path-scoped ?api_key= (both 200, "
                "2026-09-13); the keyless host already serves 200. Base-URL "
                "template is non-functional (405) — document, not wired."
            ),
            free_tier="free tier with API key (higher rate limits than keyless)",
            referral_terms="no public affiliate program — recommend on merit",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="BlockPI",
            # BlockPI issues a PER-CHAIN key, not one account-wide key: the
            # registry documents the env var as ${BLOCKPI_<CHAIN>_KEY}
            # (e.g. BLOCKPI_ETH_KEY, BLOCKPI_BSC_KEY). Endpoint host is
            # per-chain as well (incl. zksync-era).
            env_key="BLOCKPI_<CHAIN>_KEY",
            url_example="https://<chain>.blockpi.network/v1/rpc/${BLOCKPI_<CHAIN>_KEY}",
            chains_note=(
                "40+ chains; per-chain keys. Indexed keyed+archive (verified "
                "2026-09-13): ethereum/bsc/arbitrum/optimism/avalanche/linea/"
                "scroll/zksync/fantom. Also keyed but NOT indexed: taiko/"
                "cronos/bitcoin."
            ),
            free_tier="50M RU/mo — deepest multi-chain free tier after Ankr/dRPC",
            referral_terms="no public affiliate program — recommend on merit",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="Uniblock",
            env_key="UNIBLOCK_KEY",
            url_example=("https://api.uniblock.dev/uni/v1/json-rpc?chainId=1&apiKey=${UNIBLOCK_KEY}"),
            chains_note=(
                "Unified JSON-RPC across all 20 indexed chains incl. robinhood "
                "archive; auth via apiKey query param or x-api-key header"
            ),
            free_tier=("~40-60 CU/call metered; free-tier CU quota UNKNOWN — check dashboard.uniblock.dev"),
            referral_terms="no public affiliate program — recommend on merit",
            signup_url="https://dashboard.uniblock.dev",
            has_referral_program=False,
        ),
        FreemiumProvider(
            name="BlockVision",
            # BlockVision issues a PER-CHAIN key, like BlockPI: the registry
            # documents the env var as ${BLOCKVISION_<CHAIN>_KEY}
            # (e.g. BLOCKVISION_ETH_KEY, BLOCKVISION_BSC_KEY). Hosts are
            # per-chain too (eth-mainnet / bsc-mainnet / monad-mainnet /
            # sui-mainnet).
            env_key="BLOCKVISION_<CHAIN>_KEY",
            url_example="https://<chain>-mainnet.blockvision.org/v1/${BLOCKVISION_<CHAIN>_KEY}",
            chains_note=(
                "Archive-capable EVM + Move/Sui. Indexed (verified 2026-09-13): "
                "ethereum (eth_chainId 0x1, archive eth_getLogs blocks 1..2 → []), "
                "bsc (0x38, archive eth_getLogs → real logs). Also serves but NOT "
                "indexed: monad (monad-mainnet.blockvision.org), sui "
                "(sui-mainnet.blockvision.org, sui-mainnet-grpc.blockvision.org:443)."
            ),
            free_tier="free tier UNKNOWN — check blockvision.org dashboard/pricing",
            referral_terms="no public affiliate program — recommend on merit",
            signup_url="https://blockvision.org",
            has_referral_program=False,
        ),
    ]
}

# Providers evaluated and deliberately NOT registered. Kept here so a
# future audit does not re-probe a dead end (the probe report alone is
# easy to lose; see docs/RPC_PROVIDERS.md).
SKIPPED_PROVIDERS: dict[str, str] = {
    # routemesh.org did not resolve (NXDOMAIN) from the fleet, 2026-09-13 —
    # there is no endpoint to probe. Re-check only if the domain resolves.
    "RouteMesh": "skipped 2026-09-13 — routemesh.org has no DNS (unresolved)",
    # 1RPC keyed-upgrade format UNRESOLVED (tested 2026-09-13): the provided
    # key was rejected as ?apiKey=, x-api-key header, and Authorization:
    # Bearer (all still -32001 usage-limit), and the <chain>.1rpc.io/<key>
    # path variant does not resolve. Keyless https://1rpc.io/<chain> stays
    # wired where already registered — no keyed 1RPC template added.
    "1RPC keyed upgrade": "unresolved 2026-09-13 — key format not accepted; keyless 1rpc.io remains in use",
    # Moralis: provisioned keys are 324-char REST API keys, not JSON-RPC keys.
    # A Moralis node URL is per-app (siteN.moralis-nodes.com/<chain>/<key>) and
    # none is provisioned; the free plan is currently paused (REST returns 401
    # "Your Moralis Free usage is paused"). Not URL-embeddable — no template.
    "Moralis RPC": "unusable 2026-09-13 — REST keys only (free plan paused, 401); no per-app node URL provisioned",
}


def providers_with_referrals() -> list[FreemiumProvider]:
    """Providers that pay rev-share — the ones we publish referral links for."""
    return [p for p in FREEMIUM_PROVIDERS.values() if p.has_referral_program]


def signup_table() -> list[tuple[str, str, str]]:
    """(provider, env_key, signup_url) rows for docs/CLI rendering.

    ``SET_ME`` slots are intentional: the project owner drops in referral
    links without touching code (see docs/RPC_PROVIDERS.md).
    """
    return [(p.name, p.env_key, p.signup_url) for p in FREEMIUM_PROVIDERS.values()]


__all__ = [
    "FreemiumProvider",
    "FREEMIUM_PROVIDERS",
    "SKIPPED_PROVIDERS",
    "providers_with_referrals",
    "signup_table",
]
