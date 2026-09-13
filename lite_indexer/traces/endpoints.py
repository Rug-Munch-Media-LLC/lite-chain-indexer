"""
Free trace-capable RPC endpoints per EVM chain.

Unlike :mod:`lite_indexer.chains.endpoints` (eth_getLogs-focused), this
list is for the ``trace_*`` / ``debug_traceBlockByNumber`` methods that
expose internal transfers. These are much rarer on free tiers — most
public RPCs (drpc routing defaults, official chain RPCs, publicnode,
flashbots) reject tracing.

Every endpoint below was live-verified 2026-08-04 with a ``trace_block``
/ ``debug_traceBlockByNumber`` probe on block 0x1:

  - ``eth.drpc.org``     — debug_traceBlockByNumber (callTracer) OK
  - ``eth.merkle.io``    — trace_block OK + debug_traceBlockByNumber OK
  - ``rpc.eth.gateway.fm`` — trace_block OK (intermittent 503)
  - ``bsc-mainnet.nodereal.io`` — trace_block OK (parity format)
  - ``bsc.merkle.io`` / ``base.merkle.io`` — trace_block OK
  - ``polygon.drpc.org``, ``fantom.drpc.org``, ``linea.drpc.org``,
    ``gnosis.drpc.org``, ``scroll.drpc.org`` — debug_traceBlockByNumber OK

Probed and rejected (no free trace support): rpc.ankr.com/* (rate-limited
to the point of unusability), llamarpc (521), publicnode (archive token
required), flashbots (not whitelisted), official chain RPCs (unsupported),
tenderly gateways (rate-limited on free tier), blastapi ("only core evm
requests allowed").

The collector tries ``debug_traceBlockByNumber`` with the ``callTracer``
first (nested tree, one object per tx — cheaper than parity's flat list
and it is what geth-based endpoints expose), then falls back to
``trace_block`` (parity/erigon) when the method is not whitelisted.
"""

from __future__ import annotations

import os

# Load the shared env file (NODEREAL_KEY / ALCHEMY_KEY / ...) BEFORE the
# module-level placeholder resolution below, so every import path — daemon,
# cron, manual — resolves the same keys as the daemon's systemd unit.
# Idempotent and non-destructive (see lite_indexer.env.load_env_file).
from lite_indexer.env import load_env_file

load_env_file()

# Trace endpoints with a ${VAR} placeholder are resolved below; an unset key
# simply drops that endpoint from the trace pool (no startup failure).

TRACE_ENDPOINTS: dict[str, list[str]] = {
    # ── Ethereum (3 endpoints, all verified) ─────────────────────────
    "ethereum": [
        "https://eth.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
        "https://eth.merkle.io",  # trace_block + debug_traceBlockByNumber — verified
        "https://rpc.eth.gateway.fm",  # trace_block — verified (intermittent 503)
    ],
    # ── BSC (2 endpoints, verified) ──────────────────────────────────
    "bsc": [
        "https://bsc-mainnet.nodereal.io/v1/${NODEREAL_KEY}",  # trace_block (parity) — verified; key from env
        "https://bsc.merkle.io",  # trace_block — verified
    ],
    # ── Base (1 endpoint, verified) ──────────────────────────────────
    "base": [
        "https://base.merkle.io",  # trace_block — verified
    ],
    # ── Polygon (1 endpoint, verified) ───────────────────────────────
    "polygon": [
        "https://polygon.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
    ],
    # ── Fantom (1 endpoint, verified) ────────────────────────────────
    "fantom": [
        "https://fantom.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
    ],
    # ── Linea (1 endpoint, verified) ─────────────────────────────────
    "linea": [
        "https://linea.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
    ],
    # ── Gnosis (1 endpoint, verified) ────────────────────────────────
    "gnosis": [
        "https://gnosis.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
    ],
    # ── Scroll (1 endpoint, verified) ────────────────────────────────
    "scroll": [
        "https://scroll.drpc.org",  # debug_traceBlockByNumber (callTracer) — verified
    ],
}

# Chains with NO verified free trace endpoint — the collector logs a
# warning and skips them rather than hammering rate-limited dead ends.
# Candidates that were probed and failed (2026-08-04): arbitrum (drpc
# unsupported, official unsupported), optimism (drpc unsupported, official
# unsupported), avalanche (drpc internal error), zksync (drpc unsupported),
# mantle (drpc internal error), celo (drpc internal error). Alchemy /
# Infura / GetBlock free tiers expose trace_* when an API key is set, but
# none is configured in this environment.
_TRACE_UNSUPPORTED_CHAINS = (
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
