"""Blockwise SPL transfer extraction (deep/catch-up windows).

Split out of transfers.py so each extraction path owns one module. Used by
``SolanaStrategy`` via mixin composition; the signature-walk path lives in
``transfers.py``.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.models import (
    SOLANA_BLOCK_MAX_PER_SYNC,
    SOLANA_TX_CONCURRENCY,
)
from lite_indexer.strategies.solana.parsing import is_vote_transaction
from lite_indexer.strategies.solana.stride import base_stride, next_sample_stride
from lite_indexer.strategies.solana.swaps import parse_solana_swap_events
from lite_indexer.strategies.solana.timestamps import iso_from_epoch

from ._base import SolanaMixinBase

logger = get_logger(__name__)

class TransfersBlockwiseMixin(SolanaMixinBase):
    """getBlock-per-slot transfer extraction for deep catch-up windows."""

    def _balance_events(
        self,
        tx: dict[str, Any],
        slot: int,
        tracked: dict[str, dict[str, Any]],
        block_ts: str | None,
    ) -> list[dict[str, Any]]:
        """SPL balance deltas for tracked mints within one block transaction."""
        meta = tx.get("meta") or {}
        out: list[dict[str, Any]] = []
        pre = {
            (b.get("mint"), b.get("owner") or "", b.get("accountIndex")): b
            for b in (meta.get("preTokenBalances") or [])
        }
        for b in meta.get("postTokenBalances") or []:
            mint = b.get("mint") or ""
            if mint not in tracked:
                continue
            key = (mint, b.get("owner") or "", b.get("accountIndex"))
            prev = pre.get(key) or {}
            pre_amt = float(prev.get("uiTokenAmount", {}).get("uiAmount") or 0)
            post_amt = float(b.get("uiTokenAmount", {}).get("uiAmount") or 0)
            delta = post_amt - pre_amt
            if delta == 0:
                continue
            tok = tracked[mint]
            # Emit the solana_spl_balance_changes shape that
            # write_clickhouse expects (owner/amount_pre/amount_post/
            # change/symbol/token_name) — NOT the solana_transfers
            # shape (the old blockwise path emitted the wrong schema
            # and the daemon silently wrote 0 rows).
            out.append(
                {
                    "chain": "solana",
                    "slot": slot,
                    "signature": tx.get("transaction", {}).get("signatures", [""])[0],
                    "mint": mint,
                    "owner": b.get("owner") or "",
                    "token_account": b.get("address")
                    or str(key[2]),  # accountIndex is an int — CH String column
                    "amount_pre": str(pre_amt),
                    "amount_post": str(post_amt),
                    "change": str(delta),
                    "decimals": tok.get("decimals", 0),
                    "symbol": tok.get("symbol", ""),
                    "token_name": tok.get("name", ""),
                    "blockTimestamp": block_ts,
                }
            )
        return out

    async def _fetch_transfers_blockwise(
        self,
        from_slot: int,
        to_slot: int,
    ) -> list[dict[str, Any]]:
        """Block-based extraction for deep catch-up windows.

        getBlock(jsonParsed) per slot → parse pre/post token balances for
        tracked mints → emit transfer events in the same shape the
        signature-based path produces. Bounded by SOLANA_BLOCK_MAX_PER_SYNC
        so a 2M-slot catch-up progresses in bounded chunks (the daemon
        advances the highwater per window; the next window continues).
        """
        tracked = {t["mint"]: t for t in self.tracked_tokens}
        if not tracked:
            return []

        all_events: list[dict[str, Any]] = []
        vote_skipped = 0
        n_txs = 0
        fetch_times: list[float] = []
        parse_times: list[float] = []
        # 2026-09-01: process the FULL window in SOLANA_BLOCK_MAX_PER_SYNC
        # chunks — the old code capped at 200 slots total, silently dropping
        # the rest of a 1000-slot daemon window.
        slots = list(range(from_slot, to_slot + 1))
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)

        async def _fetch_one_block(slot: int) -> list[dict[str, Any]]:
            nonlocal vote_skipped, n_txs
            async with semaphore:
                _t_fetch = time.perf_counter()
                result = await self._rpc_call_versioned(
                    "getBlock",
                    [slot, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1, "rewards": False}],
                )
                fetch_times.append(time.perf_counter() - _t_fetch)
            if not result or "result" not in result or not result["result"]:
                return []
            block = result["result"]
            block_ts = iso_from_epoch(block.get("blockTime"))
            events: list[dict[str, Any]] = []
            _t_parse = time.perf_counter()
            for tx in block.get("transactions") or []:
                meta = tx.get("meta") or {}
                if meta.get("err") is not None:
                    continue
                if is_vote_transaction(tx):
                    vote_skipped += 1
                    continue
                n_txs += 1
                events.extend(self._balance_events(tx, slot, tracked, block_ts))
                # DEX swap detection in catch-up mode too (best-effort,
                # zero extra RPC — same block payload).
                try:
                    swap_rows = parse_solana_swap_events(tx, slot)
                    if swap_rows:
                        self._buffer_swap_rows(swap_rows)
                except Exception as exc:  # noqa: BLE001 — best-effort swaps
                    logger.warning("solana_swap_parse_failed", slot=slot, error=str(exc)[:60])
                # Address-activity keystone in catch-up mode too: expand
                # every successful block transaction into one row per
                # involved account (zero extra RPC — same block payload).
                # getBlock transaction dicts carry no top-level ``slot``,
                # so pass the block's slot explicitly — otherwise every
                # row is written with the sentinel 0.
                try:
                    self._ingest_address_activity(tx, slot)
                except Exception as exc:  # noqa: BLE001 — best-effort
                    logger.warning(
                        "solana_address_activity_blockwise_failed",
                        slot=slot,
                        error=str(exc)[:60],
                    )
            parse_times.append(time.perf_counter() - _t_parse)
            return events

        # Sampled coverage (adaptive stride: escalates when per-block RPC
        # latency is high so a window still fits the chain's rate).
        base = base_stride()
        stride = next_sample_stride(
            getattr(self, "_sample_stride", None),
            float(getattr(self, "_sample_mean_fetch_s", 0.0) or 0.0),
            base,
        )
        tasks = [_fetch_one_block(slot) for slot in slots if slot % stride == 0]
        # Process in bounded batches so a 1000-slot window doesn't blow the
        # daemon timeout — 200-slot chunks with a gather per chunk.
        chunk_size = SOLANA_BLOCK_MAX_PER_SYNC
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i : i + chunk_size]
            results = await asyncio.gather(*chunk, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_events.extend(r)
        mean_fetch = (sum(fetch_times) / len(fetch_times)) if fetch_times else 0.0
        self._sample_mean_fetch_s = mean_fetch
        self._sample_stride = next_sample_stride(stride, mean_fetch, base)
        if os.getenv("SOLANA_TIMING"):
            logger.info(
                "solana_sample_stride",
                stride=stride,
                next_stride=self._sample_stride,
                mean_fetch_s=round(mean_fetch, 2),
            )
            logger.info(
                "solana_blockwise_timing",
                blocks=len(fetch_times),
                fetch_s=round(sum(fetch_times), 1),
                parse_s=round(sum(parse_times), 1),
                txs=n_txs,
                votes_skipped=vote_skipped,
                events=len(all_events),
                activity_rows=len(self._address_activity_rows),
            )
        return all_events
