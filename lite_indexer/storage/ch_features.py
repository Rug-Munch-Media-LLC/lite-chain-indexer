"""ClickHouse wallet-access features — projections, token meta, balances.

Split out of :mod:`lite_indexer.storage.clickhouse` (2026-09-22) so the
writer stays under the per-file size ratchet. Config flags that tests
patch on the facade module are read through it at call time.
"""

from __future__ import annotations

from typing import Any

import lite_indexer.storage.clickhouse as _facade
from lite_indexer.logging import get_logger
from lite_indexer.storage.balance_cache import (
    ensure_balance_cache_table,
    ensure_holder_balances_mv,
    update_holder_balances_bulk,
)
from lite_indexer.storage.ch_errors import require_client
from lite_indexer.storage.projections import ensure_wallet_projection
from lite_indexer.storage.token_meta import ensure_token_meta_table, upsert_token_meta
from lite_indexer.storage.writer_api import WriterApi

logger = get_logger(__name__)


async def _wallet_token_meta(client: Any, chain: str, events: list[dict[str, Any]]) -> int:
    """Upsert one token_meta_ref row per unique token in the batch.

    ReplacingMergeTree makes re-upserts across batches a harmless no-op,
    so no existence check is needed. Returns the unique-token count.
    """
    seen_tokens: dict[str, dict[str, Any]] = {}
    for event in events:
        token_address = event.get("tokenAddress") or ""
        if token_address and token_address not in seen_tokens:
            seen_tokens[token_address] = {
                "symbol": event.get("tokenSymbol"),
                "name": event.get("tokenName"),
                "block": event.get("blockNumber") or 0,
            }
    for token_address, meta in seen_tokens.items():
        await upsert_token_meta(
            client,
            chain,
            token_address,
            meta["symbol"],
            meta["name"],
            first_seen_block=meta["block"],
        )
    return len(seen_tokens)


async def _wallet_balance_deltas(client: Any, chain: str, events: list[dict[str, Any]]) -> int:
    """Incremental balance cache — accumulate deltas per holder.

    Applied in ONE bulk read + ONE bulk insert (the old per-holder
    read-modify-write needed 2 round-trips per holder — 100K+ holders in
    a catch-up batch deadlocked every EVM sync cycle for 15+ min, zero
    sync_completes). Returns the number of balance updates applied.
    """
    deltas: dict[tuple[str, str, str], int] = {}
    for event in events:
        try:
            amount = int(event.get("value") or "0")
        except (ValueError, TypeError):
            continue
        token_address = event.get("tokenAddress") or ""
        to_address = event.get("to") or ""
        from_address = event.get("from") or ""
        if to_address:
            key = (chain, token_address, to_address)
            deltas[key] = deltas.get(key, 0) + amount
        if from_address:
            key = (chain, token_address, from_address)
            deltas[key] = deltas.get(key, 0) - amount
    return await update_holder_balances_bulk(client, deltas)


class FeaturesMixin:
    """Wallet-access path: projections, token metadata, balance caches."""

    async def write_internal_transfers(self: WriterApi, chain: str, rows: list[dict[str, Any]]) -> int:
        """Insert internal-transfer rows into ClickHouse.

        Delegates to :func:`lite_indexer.storage.trace_store.write_internal_transfers`,
        ensuring the table exists first. Returns rows inserted (0 when
        ClickHouse is unavailable or the insert fails).

        Args:
            chain: Chain name.
            rows: Standardized internal-transfer row dicts from
                ``TraceCollector``.

        Returns:
            Number of rows inserted (0 on failure/unavailable).
        """
        if not rows:
            return 0
        client = require_client(self._connect(), chain)

        from lite_indexer.storage.trace_store import (
            ensure_internal_transfers_table,
        )
        from lite_indexer.storage.trace_store import (
            write_internal_transfers as _write,
        )

        if not self._trace_table_ready and await ensure_internal_transfers_table(client):
            self._trace_table_ready = True
        if not self._trace_table_ready:
            return 0
        return _write(client, chain, rows)

    async def write_wallet_features(self: WriterApi, chain: str, events: list[dict[str, Any]]) -> dict[str, int]:
        """Apply wallet-access optimizations for a batch of transfers.

        Runs only when the corresponding config flag is enabled:

        1. ``WALLET_PROJECTION_ENABLED`` — address-first projections on
           evm_transfers (electrs key-as-record pattern).
        2. Token metadata reference table — always enabled
           (``TOKEN_META_ENABLED``), upserts one row per unique token into
           ``wallet_memory.token_meta_ref`` (cryo reference table).
        3. ``BALANCE_CACHE_ENABLED`` — incrementally update holder balances
           (electrs lazy cache); deltas are accumulated per holder so each
           holder gets at most one read-modify-write per batch.
        4. ``HOLDER_BALANCES_MV_ENABLED`` — auto-maintained SummingMergeTree
           materialized view over evm_transfers.

        Best-effort: when ClickHouse is unreachable or a DDL fails, the
        method degrades gracefully and returns whatever stats were
        collected (callers surface them in the sync summary).

        Args:
            chain: Chain name.
            events: Parsed transfer event dicts (must match the events
                already inserted into evm_transfers).

        Returns:
            Stats dict, e.g. ``{"token_meta_upserted": 3,
            "holder_balance_updates": 5}``. Empty when ClickHouse is down.
        """
        client = self._connect()
        if not client:
            return {}

        stats: dict[str, int] = {}

        # 1. Address-first projections (once per process, on success).
        if (
            _facade.WALLET_PROJECTION_ENABLED
            and not self._wallet_projection_ready
            and await ensure_wallet_projection(client)
        ):
            self._wallet_projection_ready = True
            stats["wallet_projection_ready"] = 1

        # 2. Token metadata reference table — once per unique token per
        #    batch (see _wallet_token_meta).
        if _facade.TOKEN_META_ENABLED:
            if not self._token_meta_ready and await ensure_token_meta_table(client):
                self._token_meta_ready = True
            upserted = await _wallet_token_meta(client, chain, events)
            if upserted:
                stats["token_meta_upserted"] = upserted

        # 3. Manual incremental balance cache (see _wallet_balance_deltas).
        if _facade.BALANCE_CACHE_ENABLED:
            if not self._balance_cache_ready and await ensure_balance_cache_table(client):
                self._balance_cache_ready = True
            balance_updates = await _wallet_balance_deltas(client, chain, events)
            if balance_updates:
                stats["holder_balance_updates"] = balance_updates

        # 4. Auto-maintained materialized view (once per process).
        if _facade.HOLDER_BALANCES_MV_ENABLED and not self._holder_mv_ready and await ensure_holder_balances_mv(client):
            self._holder_mv_ready = True
            stats["holder_balances_mv_ready"] = 1

        return stats
