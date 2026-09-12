"""Sync-loop infrastructure mixin — discovery, launchpad watcher, deployer
capture, RPC plumbing, reorg staging and the ClickHouse write path.

Split from the former monolithic strategy.py.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx

from lite_indexer.config import REORG_PROTECTION_ENABLED
from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.launch import SOLANA_LAUNCHES_TABLE, detect_launch
from lite_indexer.strategies.solana.models import (
    MAX_SIGNATURES_PER_TOKEN,
    SOLANA_DISCOVERY_LOOKBACK_HOURS,
    SOLANA_DISCOVERY_MAX_PER_CYCLE,
    SOLANA_LAUNCHPAD_MAX_BLOCKS,
    SOLANA_MAX_TRACKED_TOKENS,
    SOLANA_TX_CONCURRENCY,
)
from lite_indexer.strategies.solana.reorg import (
    SOLANA_REORG_DEPTH,
    checkpoint_slot,
    detect_reorg,
    ensure_checkpoint_table,
    rollback_to_slot,
)
from lite_indexer.strategies.solana.rpc import rpc_call

logger = get_logger(__name__)

# Wrapped-SOL mint — never a "new token"; the pumpswap venue matcher
# (confidence "low") reports it on every swap, so the launchpad watcher
# filters it out before buffering.
_WSOL_MINT = "So11111111111111111111111111111111111111112"


class SyncMixin:
    """Sync-loop infrastructure for the Solana strategy."""

    # ── Dynamic Token Discovery ──────────────────────────────────────

    def _get_ch_client(self) -> Any | None:
        """Lazily connect a ClickHouse client for discovery queries.

        Mirrors ClickHouseWriter._connect env handling (CH_HOST/CH_PORT/
        CH_USER/CH_PASSWORD/CH_DATABASE). Returns None when unavailable —
        discovery is best-effort and never blocks the sync cycle.
        """
        if self._ch_client is not None:
            return self._ch_client
        try:
            import os

            from clickhouse_driver import Client as CHClient

            self._ch_client = CHClient(
                host=os.getenv("CH_HOST") or os.getenv("CLICKHOUSE_HOST", "127.0.0.1"),
                port=int(os.getenv("CH_PORT") or os.getenv("CLICKHOUSE_PORT") or "9000"),
                user=os.getenv("CH_USER") or os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CH_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD") or "",
                database=os.getenv("CH_DATABASE", "wallet_memory"),
                connect_timeout=5,
                send_receive_timeout=15,
            )
            return self._ch_client
        except Exception as exc:  # noqa: BLE001 — discovery is best-effort
            logger.warning("solana_discovery_ch_unavailable", error=str(exc)[:80])
            return None

    def _discover_new_tokens(self) -> list[dict[str, Any]]:
        """Discover recently launched tokens from wallet_memory.solana_launches.

        The launch detector (launch.py) writes every Pump.fun/PumpSwap/
        Raydium/LaunchLab/LetsBonk token creation to solana_launches. This
        reads mints launched in the last SOLANA_DISCOVERY_LOOKBACK_HOURS and
        merges them into the tracked set so their transfers get indexed via
        getSignaturesForAddress like the hardcoded base tokens.

        Bounded: at most SOLANA_DISCOVERY_MAX_PER_CYCLE new mints per cycle
        and SOLANA_MAX_TRACKED_TOKENS total tracked (oldest dynamic tokens
        are evicted first; the hardcoded base set is never evicted).

        Returns:
            The list of newly added token dicts (empty when CH is down or
            nothing new was found).
        """
        client = self._get_ch_client()
        if client is None:
            return []
        try:
            rows = client.execute(  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
                f"""
                SELECT mint
                FROM {SOLANA_LAUNCHES_TABLE}
                WHERE block_timestamp >= now() - INTERVAL {SOLANA_DISCOVERY_LOOKBACK_HOURS} HOUR
                GROUP BY mint
                ORDER BY max(slot) DESC
                LIMIT {SOLANA_DISCOVERY_MAX_PER_CYCLE}
                """
            )
        except Exception as exc:  # noqa: BLE001 — discovery is best-effort
            logger.warning("solana_discovery_query_failed", error=str(exc)[:100])
            # Drop the cached client so the next cycle retries the connection.
            self._ch_client = None
            return []

        new_tokens: list[dict[str, Any]] = []
        for (mint,) in rows:
            mint = str(mint)
            if not mint or mint in self._mint_set:
                continue
            token = {"mint": mint, "symbol": mint[:8], "decimals": 0, "name": ""}
            self.tracked_tokens.append(token)
            self._mint_set.add(mint)
            new_tokens.append(token)

        # Evict oldest dynamic tokens when over the total cap (base set is
        # protected — only tokens added by discovery are removable).
        if len(self.tracked_tokens) > SOLANA_MAX_TRACKED_TOKENS:
            evict = len(self.tracked_tokens) - SOLANA_MAX_TRACKED_TOKENS
            kept: list[dict[str, Any]] = []
            for token in reversed(self.tracked_tokens):
                if evict > 0 and token["mint"] not in self._base_mints:
                    evict -= 1
                    self._mint_set.discard(token["mint"])
                    continue
                kept.append(token)
            kept.reverse()
            self.tracked_tokens = kept

        if new_tokens:
            logger.info(
                "solana_token_discovery",
                new_count=len(new_tokens),
                total_tracked=len(self.tracked_tokens),
            )
        return new_tokens

    # ── Launchpad Block Watcher (pump.fun creates) ───────────────────

    async def poll_launchpad_blocks(self) -> int:
        """Watch recent blocks for NEW token creations (launch detection).

        Launch detection (``detect_launch``) only sees transactions that
        were fetched for TRACKED mints — a brand-new pump.fun mint's
        creation tx is never fetched, so ``wallet_memory.solana_launches``
        went stale (last row 2026-08-24) and dynamic discovery + deployer
        capture starved. This watcher fetches the newest few blocks
        (jsonParsed) and runs ``detect_launch`` on EVERY transaction, so
        pump.fun creates (and any other detectable venue) land in
        ``solana_launches`` every cycle — which feeds
        ``_discover_new_tokens`` and the deployer fallback below.

        Bounded: at most ``SOLANA_LAUNCHPAD_MAX_BLOCKS`` getBlock calls per
        cycle. Best-effort: RPC errors are contained and never abort the
        sync.

        Returns:
            Number of launch events buffered into ``_last_launch_events``.
        """
        latest = await self.fetch_block_number()
        if latest is None:
            return 0
        # First run after (re)start: only scan the newest block — the
        # historical backfill is not this watcher's job.
        start = latest - SOLANA_LAUNCHPAD_MAX_BLOCKS + 1 if self._launchpad_last_slot else latest
        start = max(start, self._launchpad_last_slot + 1)
        if start > latest:
            return 0

        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)
        buffered = 0

        async def _scan_block(slot: int) -> int:
            async with semaphore:
                result = await self._rpc_call(
                    "getBlock",
                    [slot, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "rewards": False}],
                )
            block = result.get("result") if isinstance(result, dict) else None
            if not isinstance(block, dict):
                return 0
            found = 0
            for tx in block.get("transactions") or []:
                meta = tx.get("meta") or {}
                if meta.get("err") is not None:
                    continue
                try:
                    launch = detect_launch(tx)
                    # Only true token CREATIONS feed discovery — the
                    # pumpswap venue matches every PumpSwap instruction
                    # (swaps included, confidence "low"), which would
                    # flood solana_launches with swap noise and WSOL.
                    if (
                        launch is not None
                        and launch.launch_signal
                        and launch.mint
                        and launch.mint != _WSOL_MINT
                        and launch.mint not in self._mint_set
                        and launch.mint not in self._launchpad_seen_mints
                    ):
                        self._launchpad_seen_mints.add(launch.mint)
                        self._last_launch_events.append(launch)
                        found += 1
                except Exception as exc:  # noqa: BLE001 — best-effort detection
                    logger.warning("solana_launchpad_detect_failed", slot=slot, error=str(exc)[:80])
            return found

        slots = list(range(start, latest + 1))
        results = await asyncio.gather(*(_scan_block(s) for s in slots), return_exceptions=True)
        for r in results:
            if isinstance(r, int):
                buffered += r
        self._launchpad_last_slot = latest
        if buffered:
            logger.info("solana_launchpad_creates_detected", launches=buffered, slots=f"{start}-{latest}")
        return buffered

    async def capture_deployers_for_new_tokens(self, new_tokens: list[dict[str, Any]]) -> int:
        """Deployer fallback for newly discovered mints.

        The in-sync deployer capture (``_fetch_mint_transfers``) only fires
        when a mint's FULL signature history fits in one fetch window —
        by the time a hot meme token is polled it often already exceeds
        ``MAX_SIGNATURES_PER_TOKEN`` and the creation tx is unreachable.
        This fallback runs immediately when discovery adds a mint (fresh
        launches have few signatures), so ``dev_sold`` / ``sniper_detect``
        get deployer data for every new launch, not just tracked tokens.

        For each new mint: fetch its signature history; when it fits in
        one page the OLDEST signature is the creation tx and its fee payer
        is the deployer (same structural rule as the in-sync capture).
        Buffered into ``_last_deployer_events`` for the shared write path.

        Args:
            new_tokens: Token dicts returned by ``_discover_new_tokens``.

        Returns:
            Number of deployer events buffered.
        """
        if not new_tokens:
            return 0
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)
        buffered = 0
        for token in new_tokens:
            mint = token["mint"]
            if mint in self._deployer_captured_mints or mint in self._deployer_attempted_mints:
                continue
            self._deployer_attempted_mints.add(mint)
            try:
                result = await self._rpc_call(
                    "getSignaturesForAddress",
                    [mint, {"limit": MAX_SIGNATURES_PER_TOKEN}],
                )
                sigs = result.get("result") if isinstance(result, dict) else None
                if (
                    isinstance(sigs, list)
                    and sigs
                    and len(sigs) < MAX_SIGNATURES_PER_TOKEN
                    and isinstance(sigs[-1], dict)
                    and sigs[-1].get("err") is None
                ):
                    deployer_event = await self._capture_deployer_event(mint, sigs[-1], semaphore)
                    if deployer_event is not None:
                        self._last_deployer_events.append(deployer_event)
                        self._deployer_captured_mints.add(mint)
                        buffered += 1
            except Exception as exc:  # noqa: BLE001 — best-effort capture
                logger.warning("solana_deployer_fallback_failed", mint=mint[:16], error=str(exc)[:80])
        if buffered:
            logger.info("solana_deployer_fallback_captured", deployers=buffered)
        return buffered

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

    # ── Reorg Staging (slot-finality checkpointing) ──────────────────

    async def reorg_stage(
        self,
        from_slot: int,
        to_slot: int,
        ch_client: Any | None = None,
    ) -> int | None:
        """Checkpoint slot blockhashes and roll back deep reorgs for a batch.

        Solana mirror of ``EVMChainStrategy.reorg_stage``, gated by
        ``REORG_PROTECTION_ENABLED``. Called after a batch is processed:

        1. Save the batch tip slot's blockhash to the checkpoint table
           (the last ``SOLANA_REORG_DEPTH`` slots are "unripe").
        2. For ``to_slot - SOLANA_REORG_DEPTH``, verify the current
           blockhash matches the checkpointed blockhash.
        3. On mismatch: log a warning, roll back
           ``solana_spl_balance_changes`` + ``dex_events`` (chain='solana')
           above the fork slot, and return the fork slot so the caller can
           re-fetch from it.

        Args:
            from_slot: Start slot of the processed batch (unused, kept for
                signature parity with the EVM path).
            to_slot: Tip slot of the processed batch (checkpointed).
            ch_client: ClickHouse client (raw, from ClickHouseWriter).

        Returns:
            The fork slot when a reorg was detected and rolled back,
            otherwise None.
        """
        if not REORG_PROTECTION_ENABLED or ch_client is None:
            return None

        if not await ensure_checkpoint_table(ch_client):
            return None

        client = await self._get_client()

        # 1. Checkpoint the batch tip blockhash.
        if not await checkpoint_slot(client, to_slot, ch_client):
            logger.debug("reorg_tip_blockhash_missing", chain=self.chain, slot=to_slot)
            return None

        # 2. Verify the slot that should now be deep-confirmed.
        fork_slot = await detect_reorg(client, to_slot, ch_client)
        if fork_slot is None:
            return None

        # 3. Reorg detected — log, roll back to the fork slot, re-fetch.
        logger.warning(
            "chain_reorg_deep",
            chain=self.chain,
            fork_slot=fork_slot,
            depth=SOLANA_REORG_DEPTH,
        )
        await rollback_to_slot(ch_client, fork_slot)
        return fork_slot

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
            self._drain_trace_rows(client)
            # 4c. Address-activity keystone rows (wallet_memory.address_activity)
            # buffered from the same getTransaction calls — drained alongside
            # trace rows so a CH hiccup on the SPL insert never strands them.
            self._drain_address_activity_rows(client)
