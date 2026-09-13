"""Solana strategy package — split from the former 1109-line strategy.py.

Sub-modules (mixin classes composed into SolanaStrategy):
  sync        — discovery, launchpad watcher, deployer capture, RPC
                plumbing, reorg staging, ClickHouse write path
  transfers   — SPL token transfer extraction (signature-walk + blockwise)
  traces      — trace-row buffering/draining (SOL transfers, CPIs)
  dex_events  — DEX swap row buffering (wallet_memory.dex_events)
"""

from __future__ import annotations

from typing import Any

import httpx

from lite_indexer.config import LAUNCHPAD_SEEN_MINTS_MAX
from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.strategies.solana.models import SOLANA_TRACKED_TOKENS
from lite_indexer.traces.solana import SolanaTraceCollector
from lite_indexer.util.bounded import BoundedSet

from .address_activity import AddressActivityMixin
from .dex_events import DexEventsMixin
from .sync import SyncMixin
from .traces import TracesMixin
from .transfers import TransfersMixin

__all__ = [
    "AddressActivityMixin",
    "DexEventsMixin",
    "SolanaStrategy",
    "SyncMixin",
    "TracesMixin",
    "TransfersMixin",
]


class SolanaStrategy(
    AddressActivityMixin, SyncMixin, TransfersMixin, TracesMixin, DexEventsMixin, AbstractChainStrategy
):
    """Strategy for Solana — indexes SPL token balance changes.

    Uses getSignaturesForAddress on tracked token mints to find recent
    transactions, then parses pre/post token balances to detect transfers.
    """

    def __init__(
        self,
        chain: str = "solana",
        tracked_tokens: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(chain)
        self.tracked_tokens = tracked_tokens or SOLANA_TRACKED_TOKENS
        self._mint_set: set[str] = {t["mint"] for t in self.tracked_tokens}
        self._client: httpx.AsyncClient | None = None
        # Deployer events: the fee payer of each tracked mint's creation
        # transaction (the oldest signature the strategy has seen for the
        # mint). Captured at most once per mint per process via
        # ``_deployer_captured_mints``.
        self._last_deployer_events: list[dict[str, Any]] = []
        self._deployer_captured_mints: set[str] = set()
        # Mints whose deployer capture was ATTEMPTED (success or not) —
        # bounds the fallback's RPC spend (a hot mint whose history exceeds
        # the fetch window can never yield a creation tx; retrying it every
        # cycle would burn getSignaturesForAddress calls forever).
        self._deployer_attempted_mints: set[str] = set()
        # Trace-level collection: SOL transfers, CPI inner instructions and
        # token account state changes, parsed from the same getTransaction
        # result that already feeds SPL balance changes (no extra RPC
        # call). Rows are buffered here and written in write_clickhouse.
        self._trace_collector = SolanaTraceCollector()
        self._trace_rows: list[dict[str, Any]] = []
        # Address-activity keystone rows (wallet_memory.address_activity):
        # one row per involved account of every parsed transaction — the
        # Helius getSignaturesForAddress parity table. Buffered here and
        # drained in write_clickhouse (see AddressActivityMixin).
        self._address_activity_rows: list[dict[str, Any]] = []
        # Launch detection (Pump.fun/PumpSwap/Raydium/LaunchLab/LetsBonk) and
        # Jito bundle detection: parsed from the same getTransaction results
        # that feed SPL balance changes (no extra RPC). Buffered here and
        # flushed in indexer.py's sync cycle. Best-effort — failures are
        # contained and never abort the SPL write.
        self._last_launch_events: list[Any] = []
        self._last_bundle_events: list[Any] = []
        # DEX swap rows (wallet_memory.dex_events, chain='solana'): parsed
        # from the same getTransaction results as SPL balance changes (no
        # extra RPC). Buffered here and drained by the sync loop, which
        # writes them via the shared write_dex_events path.
        self._last_swap_events: list[dict[str, Any]] = []
        # Dynamic token discovery: mints picked up from
        # wallet_memory.solana_launches (launch detection writes there every
        # cycle). Cached ClickHouse client, lazily connected.
        self._ch_client: Any = None
        self._base_mints: frozenset[str] = frozenset(self._mint_set)
        # Launchpad block watcher: newest slot already scanned for token
        # creations, plus mints already buffered (dedup across cycles).
        self._launchpad_last_slot: int = 0
        # Bounded (2026-09-13): dedup set would grow without limit over a
        # long-running daemon; oldest mints are evicted past the cap.
        self._launchpad_seen_mints: BoundedSet[str] = BoundedSet(LAUNCHPAD_SEEN_MINTS_MAX)
