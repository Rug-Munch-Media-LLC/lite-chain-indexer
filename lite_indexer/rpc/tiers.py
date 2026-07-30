"""
Tiered RPC provider system — FREE, FREEMIUM, and PAID endpoint tiers.

Provides:
  - RpcTier enum (FREE / FREEMIUM / PAID)
  - RPC_ENDPOINTS_BY_TIER dict: tier → chain → [endpoint URLs]
  - ${ENV_VAR} placeholder resolution in endpoint URLs
  - get_chain_endpoints(): merge all enabled tiers for a chain
  - get_best_chain_endpoint(): convenience wrapper with best-endpoint selection

Architecture:
  FREE      — Public endpoints, rate-limited (mirrors GET_LOGS_ENDPOINTS)
  FREEMIUM  — Free with API key (Infura free, Alchemy free tier)
  PAID      — Paid API keys with higher rate limits

  When ALLOW_PAID_RPC=true (env var) or --use-paid-rpc (CLI flag),
  the indexer appends FREEMIUM then PAID tier endpoints after the
  FREE tier, preferring free endpoints as primary with paid as fallback.
"""

from __future__ import annotations

import os
import re
from enum import Enum
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class RpcTier(str, Enum):
    """RPC provider tier — controls when endpoints are used.

    FREE: Public endpoints with no API key required. Rate-limited.
    FREEMIUM: Free tier with API key (Infura free, Alchemy free).
    PAID: Paid tier with higher rate limits.
    """

    FREE = "free"
    FREEMIUM = "freemium"
    PAID = "paid"


# ── Endpoint URL Template Resolution ────────────────────────────────

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_endpoint_url(url: str) -> str:
    """Resolve ${ENV_VAR} placeholders in an endpoint URL.

    If the env var is not set, logs a warning and returns the URL with
    the placeholder still in place (so the RPC call will naturally fail).

    Args:
        url: Endpoint URL potentially containing ${ENV_VAR} templates.

    Returns:
        URL with env vars resolved.
    """

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if not value:
            logger.warning("rpc_env_var_missing", var=var_name, url_snippet=url[:60])
            return match.group(0)  # Keep placeholder — call will fail safely
        return value

    return _ENV_VAR_RE.sub(_replace, url)


# ── Tiered RPC Endpoints ────────────────────────────────────────────
# Organized as: tier → chain → list of endpoint URLs.
# The FREE tier is bootstrapped from GET_LOGS_ENDPOINTS at import time.
# FREEMIUM and PAID tiers add API-key-gated endpoints.

RPC_ENDPOINTS_BY_TIER: dict[RpcTier, dict[str, list[str]]] = {}

# ── FREEMIUM endpoints (free with API key) ─────────────────────────
# These require an API key but are free to use.
# Endpoint URLs may contain ${ENV_VAR} placeholders resolved at runtime.

FREEMIUM_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://mainnet.infura.io/v3/${INFURA_KEY}",
        "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "base": [
        "https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "bsc": [
        "https://bsc-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "polygon": [
        "https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "arbitrum": [
        "https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "optimism": [
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "avalanche": [
        "https://avalanche-mainnet.infura.io/v3/${INFURA_KEY}",
    ],
    "fantom": [
        "https://fantom-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "linea": [
        "https://linea-mainnet.infura.io/v3/${INFURA_KEY}",
    ],
    "scroll": [
        "https://scroll-mainnet.infura.io/v3/${INFURA_KEY}",
    ],
    "zksync": [
        "https://zksync-era.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "solana": [
        "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        "https://rpc.helius.xyz/?api-key=${HELIUS_KEY}",
    ],
    "tron": [
        "https://api.trongrid.io",  # TronGrid base needs no key for basic queries
    ],
}

# ── PAID endpoints (higher rate limits) ────────────────────────────
# These require a paid API key with higher rate limits.

PAID_ENDPOINTS: dict[str, list[str]] = {
    "ethereum": [
        "https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        "https://mainnet.infura.io/v3/${INFURA_KEY}",
    ],
    "base": [
        "https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "bsc": [
        "https://bsc-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "polygon": [
        "https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "arbitrum": [
        "https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "optimism": [
        "https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
    ],
    "avalanche": [
        "https://avalanche-mainnet.infura.io/v3/${INFURA_KEY}",
    ],
    "solana": [
        "https://solana-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}",
        "https://rpc.helius.xyz/?api-key=${HELIUS_KEY}",
    ],
    "tron": [
        "https://api.trongrid.io",
        "https://rpc.tronlink.org",
    ],
}


def _build_tier_registry() -> dict[RpcTier, dict[str, list[str]]]:
    """Build the full RPC_ENDPOINTS_BY_TIER registry.

    FREE tier is bootstrapped from the module-level GET_LOGS_ENDPOINTS
    at call time so that runtime modifications (e.g., endpoint verification
    in indexer.py) are respected.
    """
    from lite_indexer.chains import GET_LOGS_ENDPOINTS

    return {
        RpcTier.FREE: {chain: list(eps) for chain, eps in GET_LOGS_ENDPOINTS.items()},
        RpcTier.FREEMIUM: {chain: list(eps) for chain, eps in FREEMIUM_ENDPOINTS.items()},
        RpcTier.PAID: {chain: list(eps) for chain, eps in PAID_ENDPOINTS.items()},
    }


def get_chain_endpoints(
    chain: str,
    allow_paid: bool | None = None,
    max_tier: RpcTier = RpcTier.PAID,
) -> list[str]:
    """Get RPC endpoints for a chain, merging tiers if paid RPC is allowed.

    Returns FREE tier endpoints always (from GET_LOGS_ENDPOINTS at runtime,
    respecting any modifications from endpoint verification). If
    ``allow_paid`` is True, also includes FREEMIUM and PAID tier endpoints
    (up to ``max_tier``). Resolves ${ENV_VAR} placeholders using the
    current environment.

    The returned list is ordered: FREE first, then FREEMIUM, then PAID.
    This ensures free endpoints are preferred and paid are fallback.
    Deduplicates URLs so the same resolved endpoint is not included twice.

    Args:
        chain: Chain name (e.g., 'ethereum', 'solana', 'tron').
        allow_paid: Whether to include paid tiers. Defaults to
            reading config.ALLOW_PAID_RPC. Explicit False forces free only.
        max_tier: Maximum tier to include (default: PAID).

    Returns:
        List of resolved endpoint URLs.
    """
    if allow_paid is None:
        from lite_indexer.config import ALLOW_PAID_RPC as _allow

        allow_paid = _allow

    # Build fresh registry (respects runtime GET_LOGS_ENDPOINTS modifications)
    registry = _build_tier_registry()

    result: list[str] = []

    # FREE tier: always included
    free_endpoints = list(registry.get(RpcTier.FREE, {}).get(chain, []))
    result.extend(free_endpoints)

    # Add FREEMIUM and PAID tiers if allowed
    if allow_paid:
        tier_order = [RpcTier.FREEMIUM, RpcTier.PAID]
        max_idx = tier_order.index(max_tier) if max_tier in tier_order else len(tier_order) - 1
        seen: set[str] = set(result)
        for tier in tier_order[: max_idx + 1]:
            chain_endpoints = registry.get(tier, {}).get(chain, [])
            for raw_url in chain_endpoints:
                resolved = _resolve_endpoint_url(raw_url)
                if resolved not in seen:
                    seen.add(resolved)
                    result.append(resolved)

    if not result:
        logger.warning("no_endpoints_for_chain", chain=chain)

    return result


def get_best_chain_endpoint(
    chain: str,
    allow_paid: bool | None = None,
) -> str:
    """Get the best available endpoint for a chain, considering all tiers.

    Convenience wrapper around get_chain_endpoints + get_best_endpoint.

    Args:
        chain: Chain name.
        allow_paid: Whether to include paid tier endpoints.

    Returns:
        Best endpoint URL (or empty string if none available).
    """
    endpoints = get_chain_endpoints(chain, allow_paid)
    if not endpoints:
        logger.error("no_endpoints_available", chain=chain)
        return ""

    # Use pool's get_best_endpoint (imported here to avoid circular imports on init)
    from lite_indexer.rpc.pool import get_best_endpoint as _best

    return _best(endpoints)


# ── Rebuild the module-level registry on first use ─────────────────
# This is auto-populated so that RPC_ENDPOINTS_BY_TIER is available
# at module level for introspection (e.g., listing available tiers).
RPC_ENDPOINTS_BY_TIER.update(_build_tier_registry())
