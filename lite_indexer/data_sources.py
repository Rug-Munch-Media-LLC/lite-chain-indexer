"""Data-source priority config — DATA-FIRST, not API-reliant.

Single place that defines, per chain, the priority-ordered list of data
sources the indexer dials:

  1. Keyless public RPCs (official chain endpoints, publicnode, ...).
  2. Keyed providers (env/overlay supplied) — activate via environment.

Sources are assembled from the existing tiered pools in
``lite_indexer.chains.endpoints`` (EVM) and
``lite_indexer.chains.non_evm`` (Solana/Tron) so there is one source of
truth for endpoint URLs; this module only enforces the ORDER.

The indexer must never depend on a single provider: every chain exposes
2+ independent sources, and callers rotate through the list (see
``SolanaTransfersAdapter._rpc_call`` and the EVM pool scorer).
"""

from __future__ import annotations

from lite_indexer.chains.endpoints import (
    GET_LOGS_ENDPOINTS,
    last_resort_endpoints,
)
from lite_indexer.chains.non_evm import SOLANA_ENDPOINTS, TRON_ENDPOINTS
from lite_indexer.rpc.ledger import key_id_for_endpoint

# ── Solana ────────────────────────────────────────────────────────────
# SOLANA_ENDPOINTS is already tiered public → keyed. Reorder defensively:
# any keyed endpoint is moved to the end so a future pool edit can never
# accidentally make a keyed provider primary.


def _is_keyed(url: str) -> bool:
    return "${" in url or bool(key_id_for_endpoint(url))


def _demote_keyed(urls: list[str]) -> list[str]:
    """Move keyed endpoints to the end of the priority list."""
    primary = [u for u in urls if not _is_keyed(u)]
    fallback = [u for u in urls if _is_keyed(u)]
    return primary + fallback


def solana_rpc_endpoints() -> list[str]:
    """Priority-ordered Solana RPC sources: keyless first, keyed last."""
    return _demote_keyed(list(SOLANA_ENDPOINTS))


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
