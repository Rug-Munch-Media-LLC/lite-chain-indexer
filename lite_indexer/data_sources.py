"""Data-source priority config — DATA-FIRST, not API-reliant.

Single place that defines, per chain, the priority-ordered list of data
sources the indexer dials:

  1. Our own / dedicated RPC nodes (env-provided: QUICKNODE_*, CHAINSTACK_*,
     GETBLOCK_*, own node URLs) — cheapest, most reliable, no rate limits.
  2. Verified free public RPCs (api.mainnet-beta.solana.com, publicnode, ...).
  3. Keyed free-tier APIs (Alchemy, Ankr, NOWNodes) — activate via env.
  4. Helius / other vendor APIs — LAST RESORT ONLY, never primary.

Sources are assembled from the existing tiered pools in
``lite_indexer.chains.endpoints`` (EVM) and
``lite_indexer.chains.non_evm`` (Solana/Tron) so there is one source of
truth for endpoint URLs; this module only enforces the ORDER.

The indexer must never depend on a single provider: every chain exposes
2+ independent sources, and callers rotate through the list (see
``SolanaHeliusAdapter._rpc_call`` and the EVM pool scorer).
"""

from __future__ import annotations

from lite_indexer.chains.endpoints import (
    GET_LOGS_ENDPOINTS,
    last_resort_endpoints,
)
from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS

# ── Solana ────────────────────────────────────────────────────────────
# SOLANA_ENDPOINTS is already tiered own/dedicated → public → keyed →
# Helius. Reorder defensively: any Helius URL is moved to the end so a
# future pool edit can never accidentally make Helius primary again.


def _demote_helius(urls: list[str]) -> list[str]:
    """Move Helius URLs to the end of the priority list."""
    primary = [u for u in urls if "helius" not in u.lower()]
    fallback = [u for u in urls if "helius" in u.lower()]
    return primary + fallback


def solana_rpc_endpoints() -> list[str]:
    """Priority-ordered Solana RPC sources: own/dedicated first, Helius last."""
    return _demote_helius(list(SOLANA_ENDPOINTS))


def tron_rpc_endpoints() -> list[str]:
    """Priority-ordered Tron RPC sources."""
    return list(TRON_ENDPOINTS)


# ── EVM ───────────────────────────────────────────────────────────────


def evm_rpc_endpoints(chain: str) -> list[str]:
    """Priority-ordered EVM eth_getLogs sources for ``chain``.

    GET_LOGS_ENDPOINTS is tier-ordered (own/keyed first, public fallbacks
    later); ``last_resort_endpoints`` provides the emergency tail so a
    chain always has 2+ independent sources even if the primary pool
    degrades.
    """
    primary = list(GET_LOGS_ENDPOINTS.get(chain, []))
    fallback = last_resort_endpoints(chain)
    # Deduplicate while preserving order.
    seen: set[str] = set()
    ordered: list[str] = []
    for url in [*primary, *fallback]:
        if url not in seen:
            seen.add(url)
            ordered.append(url)
    return ordered


def chain_rpc_endpoints(chain: str) -> list[str]:
    """Priority-ordered RPC sources for any chain (Solana/Tron/EVM)."""
    if chain == "solana":
        return solana_rpc_endpoints()
    if chain == "tron":
        return tron_rpc_endpoints()
    return evm_rpc_endpoints(chain)
