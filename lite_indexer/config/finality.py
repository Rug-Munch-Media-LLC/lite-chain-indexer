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

import os  # noqa: F401  — read by the *_resolve default path

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


def _positive_int(raw: str | None) -> int | None:
    """Return ``raw`` as a positive int, or None when unusable.

    Pure: takes the raw string, reads no environment. A value that is absent,
    non-numeric, or non-positive is not an override — the configured table
    value stands.
    """
    if raw is None or not raw.strip().lstrip("-").isdigit():
        return None
    value = int(raw)
    return value if value > 0 else None


def _env_override(chain: str, knob: str) -> int | None:
    """Read ``LICHAIN_<CHAIN>_<knob>`` as a positive int, or None."""
    return _positive_int(os.getenv(f"LICHAIN_{chain.upper()}_{knob}"))


def _resolve(chain: str, knob: str, table: dict[str, int], default: int, env: dict[str, str] | None) -> int:
    """Resolve one bound from ``table``, an explicit env mapping, or ``default``.

    Pure: ``env`` is supplied by the caller. ``resolve_env`` is the only
    function that reads :data:`os.environ`, which is what lets the override
    and clamp rules be tested with a dict instead of a mutated environment.
    """
    raw = (
        os.environ.get(f"LICHAIN_{chain.upper()}_{knob}") if env is None else env.get(f"LICHAIN_{chain.upper()}_{knob}")
    )
    override = _positive_int(raw)
    if override is not None:
        return override
    return table.get(chain, default)


def reorg_depth(chain: str, env: dict[str, str] | None = None) -> int:
    """Blocks/slots a checkpoint must age before it is trusted.

    Args:
        chain: Chain name, matched case-insensitively for the env key.
        env: Optional explicit env mapping. ``None`` reads
            :data:`os.environ` (the production path); a test passes a dict.
    """
    return _resolve(chain, "REORG_DEPTH", REORG_DEPTH, _DEFAULT_DEPTH, env)


def unfinalized_horizon(chain: str, env: dict[str, str] | None = None) -> int:
    """Blocks/slots inside which rows are provisional and not persisted.

    Never returns less than :func:`reorg_depth` for the same chain: a row
    may not be refused persistence before the staging path has verified
    it, or the query path would permanently withhold verifiable data.

    Args:
        chain: Chain name, matched case-insensitively for the env key.
        env: Optional explicit env mapping. ``None`` reads
            :data:`os.environ` (the production path); a test passes a dict.
    """
    horizon = _resolve(chain, "UNFINALIZED_HORIZON", UNFINALIZED_HORIZON, _DEFAULT_HORIZON, env)
    return max(horizon, reorg_depth(chain, env))


__all__ = ["REORG_DEPTH", "UNFINALIZED_HORIZON", "reorg_depth", "unfinalized_horizon"]
