"""Single source of truth for reorg depth and the unfinalized horizon.

Three call sites used to hardcode their own numbers: the live EVM
staging path (``strategies/evm/reorg.py``), the on-demand query path
(``query/rpc_fetcher.py``) and the Solana staging path
(``strategies/solana/reorg.py``). They disagreed — a row the live path
declared final at ``+12`` was still considered reorganizable at ``+12`` by
the on-demand path — so the same row could be served as canonical and
refused as provisional depending on which code path answered.

The invariant now enforced here: **``UNFINALIZED_HORIZON >= REORG_DEPTH``
for every chain.** A row is only refused persistence once it is deep
enough that the staging path has actually verified it, so a staging path
can never certify a block the query path still considers reorganizable.

Both are env-overridable per chain via ``LICHAIN_<CHAIN>_REORG_DEPTH`` and
``LICHAIN_<CHAIN>_UNFINALIZED_HORIZON`` (e.g. ``LICHAIN_ETHEREUM_REORG_DEPTH=16``).
"""

from __future__ import annotations

import os

#: Blocks (EVM) / slots (Solana) a checkpoint must be confirmed against the
#: current chain before its rows are trusted. Staging compares the stored
#: hash at this depth; deeper is safer, costlier.
REORG_DEPTH: dict[str, int] = {
    "ethereum": 12,
    "polygon": 32,
    "bsc": 12,
    "arbitrum": 12,
    "optimism": 12,
    "base": 12,
    "avalanche": 12,
    "gnosis": 12,
    "solana": 32,
    "tron": 19,
}

#: Blocks (EVM) / slots (Solana) inside which a row is still treated as
#: provisional: served live, never persisted to the lake.
UNFINALIZED_HORIZON: dict[str, int] = {
    "ethereum": 128,
    "polygon": 128,
    "bsc": 128,
    "arbitrum": 128,
    "optimism": 128,
    "base": 128,
    "avalanche": 128,
    "gnosis": 128,
    "solana": 32,
    "tron": 19,
}

#: Fallback for chains not listed above. EVM-like block time until proven
#: otherwise; Solana's slot time is fast, hence its explicit entry.
_DEFAULT_DEPTH = 12
_DEFAULT_HORIZON = 128


def _env_override(chain: str, knob: str) -> int | None:
    """Read ``LICHAIN_<CHAIN>_<knob>`` as a positive int, or None."""
    raw = os.getenv(f"LICHAIN_{chain.upper()}_{knob}")
    if raw is None or not raw.strip().lstrip("-").isdigit():
        return None
    value = int(raw)
    return value if value > 0 else None


def reorg_depth(chain: str) -> int:
    """Blocks/slots a checkpoint must age before it is trusted."""
    override = _env_override(chain, "REORG_DEPTH")
    if override is not None:
        return override
    return REORG_DEPTH.get(chain, _DEFAULT_DEPTH)


def unfinalized_horizon(chain: str) -> int:
    """Blocks/slots inside which rows are provisional and not persisted.

    Never returns less than :func:`reorg_depth` for the same chain: a row
    may not be refused persistence before the staging path has verified
    it, or the query path would permanently withhold verifiable data.
    """
    override = _env_override(chain, "UNFINALIZED_HORIZON")
    horizon = override if override is not None else UNFINALIZED_HORIZON.get(chain, _DEFAULT_HORIZON)
    return max(int(horizon), reorg_depth(chain))


__all__ = ["REORG_DEPTH", "UNFINALIZED_HORIZON", "reorg_depth", "unfinalized_horizon"]
