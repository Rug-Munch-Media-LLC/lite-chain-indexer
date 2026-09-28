"""Tests for the shared finality config (``config/finality.py``).

The regression this locks down: ``strategies/evm/reorg.py`` re-exports
``reorg_depth`` so ``tests/test_reorg.py`` and the strategy docstrings
keep a name they already used. A wrapper that accidentally called the
module-level name instead of the imported one would recurse forever
and surface as a ``RecursionError`` at import time — i.e. on every
indexer start, before any test could run. These tests import the real
modules and call the function, which is the only way that class of bug
can never reach production unnoticed.
"""

from __future__ import annotations

import pytest

from lite_indexer.config import finality
from lite_indexer.strategies.evm import reorg as evm_reorg
from lite_indexer.strategies.solana import reorg as solana_reorg


def test_evm_wrapper_is_not_self_referential() -> None:
    """The EVM re-export must delegate, not recurse into itself."""
    assert evm_reorg.reorg_depth is not finality.reorg_depth
    assert evm_reorg.reorg_depth("ethereum") == finality.reorg_depth("ethereum")


@pytest.mark.parametrize("chain", ["ethereum", "polygon", "bsc", "base", "solana", "tron"])
def test_reorg_depth_resolves_every_configured_chain(chain: str) -> None:
    """Every chain the staging paths name returns its configured depth."""
    assert evm_reorg.reorg_depth(chain) == finality.REORG_DEPTH[chain]


def test_reorg_depth_unknown_chain_uses_documented_fallback() -> None:
    assert evm_reorg.reorg_depth("not-a-real-chain") == finality._DEFAULT_DEPTH


def test_solana_staging_reads_the_same_source() -> None:
    """Solana and EVM staging must not disagree on a shared chain."""
    assert finality.reorg_depth("solana") == solana_reorg.SOLANA_REORG_DEPTH


def test_max_reorg_depth_reexport_matches_canonical_table() -> None:
    assert evm_reorg.MAX_REORG_DEPTH == finality.REORG_DEPTH


def test_unfinalized_horizon_is_never_below_reorg_depth() -> None:
    """The invariant the refactor exists to enforce."""
    for chain in finality.REORG_DEPTH:
        assert finality.unfinalized_horizon(chain) >= finality.reorg_depth(chain)
    assert finality.unfinalized_horizon("not-a-real-chain") >= finality.reorg_depth("not-a-real-chain")


def test_env_override_is_applied_and_positive_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LICHAIN_ETHEREUM_REORG_DEPTH", "24")
    assert finality.reorg_depth("ethereum") == 24
    monkeypatch.setenv("LICHAIN_ETHEREUM_REORG_DEPTH", "0")
    assert finality.reorg_depth("ethereum") == finality.REORG_DEPTH["ethereum"]
    monkeypatch.setenv("LICHAIN_ETHEREUM_REORG_DEPTH", "not-a-number")
    assert finality.reorg_depth("ethereum") == finality.REORG_DEPTH["ethereum"]


def test_horizon_override_cannot_undercut_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    """A too-small horizon override is clamped back up to the depth."""
    monkeypatch.setenv("LICHAIN_ETHEREUM_UNFINALIZED_HORIZON", "4")
    monkeypatch.setenv("LICHAIN_ETHEREUM_REORG_DEPTH", "12")
    assert finality.unfinalized_horizon("ethereum") == 12
