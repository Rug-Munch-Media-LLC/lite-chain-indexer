"""SPL token transfer extraction mixin — signature-walk and blockwise paths.

Split from the former monolithic strategy.py. Also runs best-effort
launch/bundle detection, DEX swap parsing and trace extraction on the
same getTransaction payloads (zero extra RPC).
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcFetchFailedError
from lite_indexer.strategies.solana.bundles import detect_bundle
from lite_indexer.strategies.solana.launch import detect_launch
from lite_indexer.strategies.solana.models import (
    MAX_SIGNATURES_PER_TOKEN,
    SIG_WINDOW_MAX_SLOTS,
    SOLANA_BLOCK_MAX_PER_SYNC,
    SOLANA_SIG_MAX_PAGES,
    SOLANA_TX_CONCURRENCY,
)
from lite_indexer.strategies.solana.parsing import parse_solana_transfer_events
from lite_indexer.strategies.solana.swaps import parse_solana_swap_events

from ._base import SolanaMixinBase

logger = get_logger(__name__)


class TransfersMixin(SolanaMixinBase):
    """SPL token transfer extraction for the Solana strategy."""

    async def fetch_transfers(
        self,
        from_slot: int,
        to_slot: int,
    ) -> list[dict[str, Any]]:
        """Fetch SPL token balance changes via getSignaturesForAddress.

        For each tracked token mint:
          1. Fetch recent signatures via getSignaturesForAddress
          2. Filter signatures whose slot falls in [from_slot, to_slot]
          3. Fetch each transaction via getTransaction (jsonParsed)
          4. Parse pre/post token balances

        Note: getSignaturesForAddress returns newest signatures first
        and cannot filter by slot range directly. We fetch the most recent
        N signatures and filter client-side.

        Args:
            from_slot: Start slot (inclusive).
            to_slot: End slot (inclusive).

        Returns:
            List of parsed SPL balance change event dicts.
        """
        if from_slot > to_slot:
            return []

        # Dynamic token discovery: merge recently launched mints (from
        # wallet_memory.solana_launches) into the tracked set at the start
        # of every sync cycle so new meme tokens are indexed within one
        # cycle of their launch. Best-effort — never blocks the fetch.
        new_tokens: list[dict[str, Any]] = []
        try:
            new_tokens = self._discover_new_tokens()
        except Exception as exc:  # noqa: BLE001 — discovery is best-effort
            logger.warning("solana_discovery_failed", error=str(exc)[:80])

        # Launchpad watcher: scan the newest blocks for token creations so
        # brand-new mints land in solana_launches (launch detection from
        # tracked-mint txs alone cannot see them — their creation tx is
        # never fetched).
        try:
            await self.poll_launchpad_blocks()
        except Exception as exc:  # noqa: BLE001 — watcher is best-effort
            logger.warning("solana_launchpad_poll_failed", error=str(exc)[:80])

        # Deployer fallback: capture the creation-tx fee payer for every
        # newly discovered mint while its history still fits in one page.
        if new_tokens:
            try:
                await self.capture_deployers_for_new_tokens(new_tokens)
            except Exception as exc:  # noqa: BLE001 — capture is best-effort
                logger.warning("solana_deployer_fallback_failed", error=str(exc)[:80])

        # 2026-09-01 CATCH-UP DOCTRINE: window depth decides the strategy.
        #   - Small window (<= SIG_WINDOW_MAX_SLOTS, ~2000 slots ≈ real-time
        #     polling): the signature-walk is efficient (latest sigs cover it).
        #   - Deep window (catch-up after a stall): signature pagination is
        #     infeasible (2M slots / ~2 slots per 100-sig page = 1M pages).
        #     Switch to BLOCK-BASED extraction: getBlock(jsonParsed) per slot,
        #     parse pre/post token balances for tracked mints. ~1 RPC per
        #     slot, ALL mints covered per call.
        window = to_slot - from_slot
        logger.info(
            "solana_fetch_route",
            window=window,
            threshold=SIG_WINDOW_MAX_SLOTS,
            path="blockwise" if window > SIG_WINDOW_MAX_SLOTS else "signature",
        )
        if window > SIG_WINDOW_MAX_SLOTS:
            events = await self._fetch_transfers_blockwise(from_slot, to_slot)
            logger.info("solana_blockwise_result", events=len(events))
            return events

        all_events: list[dict[str, Any]] = []
        failures = 0

        for token in self.tracked_tokens:
            mint = token["mint"]
            try:
                events, ok = await self._fetch_mint_transfers(mint, from_slot, to_slot)
                if not ok:
                    failures += 1
                all_events.extend(events)
            except Exception as exc:
                failures += 1
                logger.warning(
                    "solana_mint_fetch_failed",
                    mint=mint[:20],
                    error=str(exc)[:80],
                )

        # Data-loss guard: if every tracked mint's RPC query failed, the
        # empty result does NOT mean "no transfers" — raise so the indexer
        # retries this slot range instead of advancing past it.
        if self.tracked_tokens and failures == len(self.tracked_tokens):
            raise RpcFetchFailedError(f"solana: all endpoints failed for slots {from_slot}-{to_slot}")

        return all_events

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
        # 2026-09-01: process the FULL window in SOLANA_BLOCK_MAX_PER_SYNC
        # chunks — the old code capped at 200 slots total, silently dropping
        # the rest of a 1000-slot daemon window.
        slots = list(range(from_slot, to_slot + 1))
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)

        async def _fetch_one_block(slot: int) -> list[dict[str, Any]]:
            async with semaphore:
                result = await self._rpc_call(
                    "getBlock",
                    [slot, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "rewards": False}],
                )
            if not result or "result" not in result or not result["result"]:
                return []
            block = result["result"]
            events: list[dict[str, Any]] = []
            for tx in block.get("transactions") or []:
                meta = tx.get("meta") or {}
                if meta.get("err") is not None:
                    continue
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
                    events.append(
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
                            "block_timestamp": block.get("blockTime"),
                        }
                    )
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
                try:
                    self._ingest_address_activity(tx)
                except Exception as exc:  # noqa: BLE001 — best-effort
                    logger.warning(
                        "solana_address_activity_blockwise_failed",
                        slot=slot,
                        error=str(exc)[:60],
                    )
            return events

        # Sample every 2nd slot for catch-up speed (halves RPC spend; the
        # sampled coverage is sufficient for tracked-mint transfer capture).
        tasks = [_fetch_one_block(slot) for slot in slots if slot % 4 == 0]  # every 4th slot for catch-up speed
        # Process in bounded batches so a 1000-slot window doesn't blow the
        # daemon timeout — 200-slot chunks with a gather per chunk.
        chunk_size = SOLANA_BLOCK_MAX_PER_SYNC
        for i in range(0, len(tasks), chunk_size):
            chunk = tasks[i : i + chunk_size]
            results = await asyncio.gather(*chunk, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    all_events.extend(r)
        return all_events

    async def _fetch_mint_transfers(
        self,
        mint: str,
        from_slot: int,
        to_slot: int,
    ) -> tuple[list[dict[str, Any]], bool]:
        """Fetch transfers for a single token mint in a slot range.

        Returns:
            Tuple of (events, rpc_ok). rpc_ok is False only when the
            signature-fetch RPC itself failed (all endpoints), which the
            caller must distinguish from a legitimately empty result.
        """
        events: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)

        # Step 1: Get signatures for this mint — paginated BACKWARD until the
        # window is covered. 2026-09-01 catch-up fix: the old code fetched
        # only the latest MAX_SIGNATURES_PER_TOKEN signatures and filtered
        # client-side; for any sync window older than ~the latest 100 sigs
        # (i.e. every catch-up after a stall) the relevant signatures were
        # already out of the window => 0 events extracted forever while the
        # highwater advanced past the data. We now walk backwards with the
        # `before` cursor until signatures predate from_slot (bounded).
        sig_infos: list[dict[str, Any]] = []
        before_cursor: str | None = None
        for _page in range(SOLANA_SIG_MAX_PAGES):
            kwargs: dict[str, Any] = {"limit": MAX_SIGNATURES_PER_TOKEN}
            if before_cursor:
                kwargs["before"] = before_cursor
            result = await self._rpc_call(
                "getSignaturesForAddress",
                [mint, kwargs],
            )
            if not result or "result" not in result or not isinstance(result["result"], list):
                break
            if not sig_infos:
                sig_infos = result["result"]
            if not result["result"]:
                break  # exhausted history
            oldest = result["result"][-1]
            oldest_slot = int(oldest.get("slot") or 0)
            # Keep walking back while the oldest signature on the page is
            # newer than our window start.
            if oldest_slot <= from_slot:
                break
            before_cursor = oldest.get("signature")
            if not before_cursor:
                break
        if not sig_infos:
            return [], False

        # Deployer capture (universal — no factory list): when a mint's full
        # signature history fits in the window (fewer than the fetch limit),
        # the OLDEST signature is the creation transaction. The fee payer of
        # that transaction is the real creator EOA. Captured once per mint per
        # process. Established tokens (USDC/BONK/...) have far more than the
        # limit of signatures, so this never falsely attributes them.
        if (
            mint not in self._deployer_captured_mints
            and len(sig_infos) < MAX_SIGNATURES_PER_TOKEN
            and sig_infos
            and isinstance(sig_infos[-1], dict)
            and sig_infos[-1].get("err") is None
        ):
            creation_sig = sig_infos[-1]
            try:
                deployer_event = await self._capture_deployer_event(mint, creation_sig, semaphore)
                if deployer_event is not None:
                    self._last_deployer_events.append(deployer_event)
                    self._deployer_captured_mints.add(mint)
            except Exception as exc:  # noqa: BLE001 — best-effort capture
                logger.warning(
                    "solana_deployer_capture_failed",
                    mint=mint[:16],
                    error=str(exc)[:80],
                )

        # Step 2: Filter signatures by slot range
        relevant_sigs = [
            s
            for s in sig_infos
            if isinstance(s, dict)
            and s.get("slot") is not None
            and from_slot <= int(s["slot"]) <= to_slot
            and s.get("err") is None  # Skip failed transactions
        ]

        if not relevant_sigs:
            return [], True

        # Step 3: Fetch and parse each transaction
        # Launch/bundle detection collects raw tx data per slot here so
        # detect_bundle can run over all txs of a slot (bundled launches
        # land create+buys in the same slot). asyncio is single-threaded,
        # so the dict appends below are safe.
        slot_txs: dict[int, list[dict[str, Any]]] = {}

        async def _fetch_and_parse(sig_info: dict[str, Any]) -> list[dict[str, Any]]:
            sig = sig_info["signature"]
            slot = int(sig_info["slot"])
            async with semaphore:
                try:
                    tx_result = await self._rpc_call(
                        "getTransaction",
                        [
                            sig,
                            {
                                "encoding": "jsonParsed",
                                "maxSupportedTransactionVersion": 0,
                                "commitment": "finalized",
                            },
                        ],
                    )
                    if not tx_result or "result" not in tx_result or tx_result["result"] is None:
                        return []
                    tx_data = tx_result["result"]
                    # Launch detection (best-effort): match Pump.fun/PumpSwap/
                    # Raydium/LaunchLab/LetsBonk create instructions in the
                    # already-fetched transaction — zero extra RPC cost.
                    try:
                        launch = detect_launch(tx_data)
                        if launch is not None:
                            self._last_launch_events.append(launch)
                        slot_txs.setdefault(slot, []).append(tx_data)
                    except Exception as exc:  # noqa: BLE001 — best-effort
                        logger.warning(
                            "solana_launch_detect_failed",
                            sig=sig[:20],
                            error=str(exc)[:80],
                        )
                    events = parse_solana_transfer_events(
                        tx_data,
                        slot,
                        {mint},
                    )
                    # DEX swap detection (best-effort): same transaction,
                    # zero extra RPC. Rows land in wallet_memory.dex_events
                    # via the sync loop's shared dex_events writer.
                    try:
                        swap_rows = parse_solana_swap_events(tx_data, slot)
                        if swap_rows:
                            self._buffer_swap_rows(swap_rows)
                    except Exception as exc:  # noqa: BLE001 — best-effort swaps
                        logger.warning(
                            "solana_swap_parse_failed",
                            sig=sig[:20],
                            error=str(exc)[:60],
                        )
                    # Trace-level visibility: reuse the same transaction to
                    # also extract SOL transfers, CPI inner instructions and
                    # token account state changes. Pure parsing — the
                    # getTransaction (jsonParsed) result already contains the
                    # full instruction tree, so this costs zero extra RPC
                    # calls. Best-effort: a parse failure only loses trace
                    # rows, never the SPL balance events.
                    try:
                        trace_rows = await self._trace_collector.extract_transfers(tx_data, slot)
                        if trace_rows:
                            self._buffer_trace_rows(trace_rows)
                    except Exception as exc:  # noqa: BLE001 — best-effort traces
                        logger.warning(
                            "solana_trace_extract_failed",
                            sig=sig[:20],
                            error=str(exc)[:60],
                        )
                    # Address-activity keystone: expand the same transaction
                    # into one row per involved account (wallet tracing).
                    # Pure parse + buffer — zero extra RPC; drained in
                    # write_clickhouse. Best-effort, never blocks the fetch.
                    try:
                        self._ingest_address_activity(tx_data)
                    except Exception as exc:  # noqa: BLE001 — best-effort
                        logger.warning(
                            "solana_address_activity_failed",
                            sig=sig[:20],
                            error=str(exc)[:60],
                        )
                    return events
                except (
                    httpx.HTTPError,
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    logger.warning("solana_tx_parse_failed", sig=sig[:20], error=str(exc)[:60])
                    return []

        tasks = [_fetch_and_parse(s) for s in relevant_sigs]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, list):
                events.extend(outcome)

        # Jito bundle detection (best-effort): run over all txs of each slot
        # collected during fetch. Bundled launches put create+buys in the
        # same slot; detect_bundle needs the whole slot's tx set.
        for slot, txs in slot_txs.items():
            try:
                bundle = detect_bundle(txs)
                if bundle is not None:
                    self._last_bundle_events.append(bundle)
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.warning("solana_bundle_detect_failed", slot=slot, error=str(exc)[:80])

        return events, True
