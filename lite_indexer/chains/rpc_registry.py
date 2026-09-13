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
            chains_note="83+ chains incl. Solana + Bitcoin",
            free_tier="10M credits/mo",
            referral_terms="50% of first month (credits); 15% recurring on dev subs; 90d cookie",
        ),
        FreemiumProvider(
            name="Chainstack",
            env_key="CHAINSTACK_KEY",
            url_example="https://nd-{id}.{region}.chainstacklabs.com/${CHAINSTACK_KEY}",
            chains_note="Major EVMs + Solana; flat request pricing",
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
            chains_note="Solana specialist (LaserStream gRPC, DAS API)",
            free_tier="1M credits/mo (10 RPS)",
            referral_terms="creator/partner program (contact Helius)",
        ),
        FreemiumProvider(
            name="BlockPI",
            env_key="BLOCKPI_KEY",
            url_example="https://rpc.blockpi.io/v1/rpc/${BLOCKPI_KEY}",
            chains_note="40+ chains",
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
    ]
}

# Providers evaluated and deliberately NOT registered. Kept here so a
# future audit does not re-probe a dead end (the probe report alone is
# easy to lose; see docs/RPC_PROVIDERS.md).
SKIPPED_PROVIDERS: dict[str, str] = {
    # routemesh.org did not resolve (NXDOMAIN) from the fleet, 2026-09-13 —
    # there is no endpoint to probe. Re-check only if the domain resolves.
    "RouteMesh": "skipped 2026-09-13 — routemesh.org has no DNS (unresolved)",
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
