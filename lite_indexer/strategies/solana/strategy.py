"""
Solana Strategy — SPL token transfer indexing via getSignaturesForAddress.

Solana indexing is fundamentally different from EVM:
  - No event logs (no eth_getLogs equivalent)
  - Token transfers are detected via pre/post token balance changes
  - getSignaturesForAddress returns newest signatures first (no range filter)
  - Each signature requires a separate getTransaction call
  - Block numbers are called "slots"

Strategy:
  1. Get latest finalized slot (getSlot)
  2. For each tracked token mint, fetch recent signatures (getSignaturesForAddress)
  3. For each signature, fetch the transaction (getTransaction, jsonParsed)
  4. Parse preTokenBalances / postTokenBalances to detect transfers
  5. Filter by slot range (since getSignaturesForAddress can't filter by slot)
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.logging import get_logger
from lite_indexer.rpc import RpcFetchFailedError
from lite_indexer.strategies.base import AbstractChainStrategy
from lite_indexer.traces.solana import (
    SolanaTraceCollector,
    ensure_solana_transfers_table,
    write_solana_transfers,
)

from .bundles import detect_bundle
from .launch import detect_launch
from .models import (
    MAX_SIGNATURES_PER_TOKEN,
    SOLANA_SIG_MAX_PAGES,
    SOLANA_TRACKED_TOKENS,
    SOLANA_TX_CONCURRENCY,
)
from .parsing import parse_solana_transfer_events
from .rpc import rpc_call

logger = get_logger(__name__)

# Upper bound on buffered trace rows awaiting a ClickHouse write. Trace
# rows are collected from the same getTransaction calls as SPL events
# (zero extra RPC cost) and drained in write_clickhouse; the cap bounds
# memory when CH is down between cycles (oldest rows are dropped first).
_SOLANA_TRACE_ROW_CAP = 10_000


class SolanaStrategy(AbstractChainStrategy):
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
        # Trace-level collection: SOL transfers, CPI inner instructions and
        # token account state changes, parsed from the same getTransaction
        # result that already feeds SPL balance changes (no extra RPC
        # call). Rows are buffered here and written in write_clickhouse.
        self._trace_collector = SolanaTraceCollector()
        self._trace_rows: list[dict[str, Any]] = []
        # Launch detection (Pump.fun/PumpSwap/Raydium/LaunchLab/LetsBonk) and
        # Jito bundle detection: parsed from the same getTransaction results
        # that feed SPL balance changes (no extra RPC). Buffered here and
        # flushed in indexer.py's sync cycle. Best-effort — failures are
        # contained and never abort the SPL write.
        self._last_launch_events: list[Any] = []
        self._last_bundle_events: list[Any] = []

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            limits = httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=30,
            )
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                limits=limits,
            )
        return self._client

    async def _rpc_call(
        self,
        method: str,
        params: list[Any] | None = None,
        endpoint: str | None = None,
    ) -> dict[str, Any] | None:
        """Make a JSON-RPC call using the shared client."""
        client = await self._get_client()
        return await rpc_call(client, method, params, endpoint)

    # ── Feature Flags ─────────────────────────────────────────────────

    def supports_deployers(self) -> bool:
        """Whether this chain tracks deployer events (True for Solana).

        Solana deployer events are captured structurally: the fee payer of
        the mint's creation transaction (oldest signature when the full
        history fits in the fetch window) is the real creator EOA.
        """
        return True

    async def _capture_deployer_event(
        self,
        mint: str,
        creation_sig: dict[str, Any],
        semaphore: asyncio.Semaphore,
    ) -> dict[str, Any] | None:
        """Capture a deployer event for a mint's creation transaction.

        Fetches the creation signature (oldest in the window) via
        ``getTransaction`` (jsonParsed) and records the fee payer
        (``transaction.message.accountKeys[0]``) as the deployer EOA. The
        deployer event dict matches the ``deployer_events`` schema so the
        ClickHouse writer can insert it directly (fee payer is already
        resolved — no ``eth_getTransactionByHash`` round-trip needed).

        Args:
            mint: Token mint address.
            creation_sig: Signature info dict (with ``signature`` / ``slot``).
            semaphore: Shared concurrency limiter.

        Returns:
            Deployer event dict, or None when the tx cannot be fetched or the
            fee payer is missing.
        """
        sig = creation_sig.get("signature", "")
        if not sig:
            return None

        async with semaphore:
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
        if not tx_result or not isinstance(tx_result, dict) or not isinstance(tx_result.get("result"), dict):
            return None

        tx_data = tx_result["result"]
        message = (tx_data.get("transaction") or {}).get("message") or {}
        account_keys = message.get("accountKeys") or []
        if not account_keys:
            return None
        first = account_keys[0]
        fee_payer = first.get("pubkey") if isinstance(first, dict) else str(first)
        if not fee_payer:
            return None

        block_time = tx_data.get("blockTime")
        try:
            ts = datetime.fromtimestamp(int(block_time), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (ValueError, TypeError, OverflowError):
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "deployer": str(fee_payer),
            "deployer_eoa": str(fee_payer),
            "token_address": mint,
            "token0": mint,
            "token1": "",
            "pair_address": "",
            "chain": "solana",
            "block_number": int(creation_sig.get("slot") or 0),
            "block_timestamp": ts,
            "tx_hash": sig,
            "type": "solana_token",
            "factory": "",
            "ingested_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "lite_indexer",
        }

    # ── Block Number ─────────────────────────────────────────────────

    async def fetch_block_number(self) -> int | None:
        """Get latest finalized slot.

        Returns the most recent slot that has been finalized by the
        supermajority of validators.
        """
        result = await self._rpc_call("getSlot", [{"commitment": "finalized"}])
        if result and "result" in result:
            return int(result["result"])
        return None

    # ── Block Timestamps ─────────────────────────────────────────────

    async def fetch_block_timestamps(self, block_numbers: set[int]) -> dict[int, str]:
        """Fetch block timestamps via getBlock.

        Solana blocks include a Unix timestamp in the block header.
        """
        if not block_numbers:
            return {}

        timestamps: dict[int, str] = {}
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)

        async def _get_block_time(slot: int) -> tuple[int, str | None]:
            async with semaphore:
                try:
                    result = await self._rpc_call(
                        "getBlock",
                        [slot, {"encoding": "json", "transactionDetails": "none"}],
                    )
                    if result and "result" in result:
                        block = result["result"]
                        if block and "blockTime" in block:
                            bt = block["blockTime"]
                            if isinstance(bt, (int, float)) and bt > 0:
                                ts = datetime.fromtimestamp(bt, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                                return slot, ts
                except (
                    httpx.HTTPError,
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    logger.warning(
                        "solana_block_time_failed",
                        slot=slot,
                        error=str(exc)[:80],
                    )
                return slot, None

        tasks = [_get_block_time(s) for s in block_numbers]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, tuple) and outcome[1] is not None:
                timestamps[outcome[0]] = outcome[1]

        return timestamps

    # ── Transfer Events ──────────────────────────────────────────────

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
                            self._trace_rows.extend(trace_rows)
                            if len(self._trace_rows) > _SOLANA_TRACE_ROW_CAP:
                                self._trace_rows = self._trace_rows[-_SOLANA_TRACE_ROW_CAP:]
                    except Exception as exc:  # noqa: BLE001 — best-effort traces
                        logger.warning(
                            "solana_trace_extract_failed",
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

    # ── ClickHouse Write ─────────────────────────────────────────────

    def write_clickhouse(
        self,
        events: list[dict[str, Any]],
        ch_writer: Any,
    ) -> int:
        """Write Solana SPL balance changes to ClickHouse.

        Uses the solana_spl_balance_changes table schema:
          chain, slot, signature, mint, owner, token_account,
          amount_pre, amount_post, change, decimals, symbol,
          token_name, block_timestamp, source, ingested_at
        """
        if not events:
            return 0

        client = ch_writer._connect()
        if not client:
            logger.warning("solana_clickhouse_skipped")
            return 0

        # F1 wallet resolution: ensure the owner column exists (idempotent)
        # before inserting. Old deployments degrade gracefully — the DDL
        # helper reports success once the column is in place, historical
        # rows keep DEFAULT '' until backfilled.
        ensure_owner = getattr(ch_writer, "_ensure_solana_owner_column", None)
        if ensure_owner is not None:
            ensure_owner(client)

        now = datetime.now(UTC)
        rows: list[tuple[Any, ...]] = []

        for event in events:
            ts_str = event.get("blockTimestamp", "")
            if ts_str:
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    ts = now
            else:
                ts = now

            rows.append(
                (
                    event.get("slot", 0),
                    ts,
                    event.get("signature", ""),
                    event.get("token_account", ""),
                    event.get("mint", ""),
                    event.get("owner", ""),
                    event.get("amount_pre", "0"),
                    event.get("amount_post", "0"),
                    event.get("change", "0"),
                    event.get("decimals", 0),
                    event.get("symbol", ""),
                    event.get("token_name", ""),
                    now,
                )
            )

        solana_table = "wallet_memory.solana_spl_balance_changes"

        try:
            client.execute(
                f"INSERT INTO {solana_table} "
                "(slot, block_timestamp, tx_hash, token_account, "
                "mint, owner, pre_amount, post_amount, delta, "
                "decimals, token_symbol, token_name, "
                "ingested_at) VALUES",
                rows,
            )
            logger.info("solana_clickhouse_inserted", rows=len(rows))
            return len(rows)
        except Exception as exc:
            logger.error("solana_clickhouse_insert_failed", rows=len(rows), error=str(exc)[:150])
            return 0
        finally:
            # 4b. Trace-level rows (SOL transfers, CPIs, account state
            # changes) collected from the same getTransaction calls. Written
            # best-effort after the SPL balance events; on failure the rows
            # are kept (bounded by _SOLANA_TRACE_ROW_CAP) and retried on the
            # next write cycle. Runs in a finally so trace rows are drained
            # even when the SPL insert fails (e.g. CH hiccup).
            trace_rows = self._trace_rows
            self._trace_rows = []
            if trace_rows:
                try:
                    ensure_solana_transfers_table(client)
                    written = write_solana_transfers(client, trace_rows)
                    logger.info("solana_trace_clickhouse_inserted", rows=written)
                except Exception as trace_exc:  # noqa: BLE001 — best-effort traces
                    logger.error(
                        "solana_trace_clickhouse_insert_failed",
                        rows=len(trace_rows),
                        error=str(trace_exc)[:150],
                    )
                    self._trace_rows = trace_rows
