"""
Free trace-capable RPC endpoints per EVM chain.

Unlike :mod:`lite_indexer.chains.endpoints` (eth_getLogs-focused), this list
is for the ``trace_*`` / ``debug_traceBlockByNumber`` methods that expose
internal transfers. These are much rarer on free tiers — most public RPCs
(official chain RPCs, publicnode, flashbots) reject tracing.

Every endpoint below was live-verified with a ``trace_block`` /
``debug_traceBlockByNumber`` probe on block 0x1:

  - ``eth.merkle.io``      — trace_block OK + debug_traceBlockByNumber OK
  - ``rpc.eth.gateway.fm`` — trace_block OK (intermittent 503)
  - ``bsc.merkle.io`` / ``base.merkle.io`` — trace_block OK

Provider-specific trace endpoints and any keyed templates are supplied by
the private runtime overlay; the public tree keeps only keyless publics.
Probed and rejected (no free trace support): llamarpc (521), publicnode
(archive token required), flashbots (not whitelisted), official chain RPCs
(unsupported), tenderly gateways (rate-limited on free tier).

The collector tries ``debug_traceBlockByNumber`` with the ``callTracer``
first (nested tree, one object per tx — cheaper than parity's flat list and
it is what geth-based endpoints expose), then falls back to ``trace_block``
(parity/erigon) when the method is not whitelisted.
"""

from __future__ import annotations

import os

# Load the shared env file BEFORE the module-level placeholder resolution
# below (idempotent; see lite_indexer.env.load_env_file).
from lite_indexer.env import load_env_file

load_env_file()

TRACE_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum (2 endpoints, verified) ─────────────────────────────
    "ethereum": [
        "https://eth.merkle.io",  # trace_block + debug_traceBlockByNumber — verified
        "https://rpc.eth.gateway.fm",  # trace_block — verified (intermittent 503)
    ],
    # ── BSC (1 endpoint, verified) ───────────────────────────────────
    "bsc": [
        "https://bsc.merkle.io",  # trace_block — verified
    ],
    # ── Base (1 endpoint, verified) ──────────────────────────────────
    "base": [
        "https://base.merkle.io",  # trace_block — verified
    ],
}

# Chains with NO verified free trace endpoint — the collector logs a warning
# and skips them rather than hammering rate-limited dead ends.
_TRACE_UNSUPPORTED_CHAINS = (
    "polygon",
    "fantom",
    "linea",
    "gnosis",
    "scroll",
    "arbitrum",
    "optimism",
    "avalanche",
    "zksync",
    "mantle",
    "celo",
)

# Keep the module honest: report the unsupported set programmatically.
TRACE_UNSUPPORTED_CHAINS: tuple[str, ...] = _TRACE_UNSUPPORTED_CHAINS


def _resolve_endpoints(urls: list[str]) -> list[str]:
    """Drop endpoints whose ${ENV_VAR} placeholder could not expand.

    Matches the pattern used by ``lite_indexer.chains.endpoints`` so an
    unset API key never leaves a dead placeholder URL in the pool.
    """
    resolved: list[str] = []
    for url in urls:
        expanded = os.path.expandvars(url)
        if "${" in expanded:
            continue
        resolved.append(expanded)
    return resolved


TRACE_ENDPOINTS = {chain: _resolve_endpoints(urls) for chain, urls in TRACE_ENDPOINTS.items()}


def has_trace_endpoints(chain: str) -> bool:
    """Return True when the chain has at least one verified trace endpoint.

    Args:
        chain: Chain name (e.g. 'ethereum').

    Returns:
        True when traces can be collected for the chain.
    """
    return bool(TRACE_ENDPOINTS.get(chain))
