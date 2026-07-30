"""
Solana strategy — SPL token transfer indexing via getSignaturesForAddress.

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
from lite_indexer.strategies.base import AbstractChainStrategy

logger = get_logger(__name__)

# ── Solana RPC Endpoints (tiered fallback) ───────────────────────────
# T1 — Official, most reliable (mainnet-beta is Solana Foundation's public RPC)
# T2 — Proven free alternatives (publicnode, ankr-with-key)
# T3 — Best-effort fallbacks
#
# All endpoints tested 2026-07-30: getSlot, getSignaturesForAddress, getTransaction
# getBlock works on all verified endpoints.
#
SOLANA_ENDPOINTS: list[str] = [
    # T1 — Official Solana Foundation RPC (most reliable, rate-limited per IP)
    "https://api.mainnet-beta.solana.com",
    # T2 — Verified free RPC with good uptime
    "https://solana-rpc.publicnode.com",
    # T3 — Fallbacks (may have lower rate limits or require API keys)
    "https://rpc.ankr.com/solana",
    "https://solana.drpc.org",
]

# ── Tracked SPL Tokens ───────────────────────────────────────────────
# Well-known SPL tokens to track for balance changes.
# Uses getSignaturesForAddress on the mint address, then parses
# pre/post token balances from the transaction.

SOLANA_TRACKED_TOKENS: list[dict[str, Any]] = [
    {"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "symbol": "USDC", "decimals": 6, "name": "USD Coin"},
    {"mint": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "symbol": "USDT", "decimals": 6, "name": "Tether USD"},
    {"mint": "So11111111111111111111111111111111111111112", "symbol": "SOL", "decimals": 9, "name": "Wrapped SOL"},
    {"mint": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263", "symbol": "BONK", "decimals": 5, "name": "Bonk"},
    {"mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN", "symbol": "JUP", "decimals": 6, "name": "Jupiter"},
    {"mint": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr", "symbol": "POPCAT", "decimals": 9, "name": "Popcat"},
    {"mint": "ukHH6c7mMyiWCf1b9pnWe25TSpkDDt3H5pQZgZ74J82", "symbol": "BOME", "decimals": 6, "name": "BOOK OF MEME"},
    {"mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm", "symbol": "WIF", "decimals": 6, "name": "dogwifhat"},
    {"mint": "A3eME5CetyZPBoWbRUwY3tSe25S6tb18ba9ZPbWk9eFp", "symbol": "PENG", "decimals": 6, "name": "Peng"},
    {
        "mint": "MEW1gQWJ3nEXg2qgERiKu7FAFj79PHvQVREQUzScPP5",
        "symbol": "MEW",
        "decimals": 5,
        "name": "cat in a dogs world",
    },
]

# Maximum signatures to fetch per tracked token per cycle
MAX_SIGNATURES_PER_TOKEN = 100

# Max concurrent getTransaction calls
SOLANA_TX_CONCURRENCY = 10


def _parse_solana_transfer_events(
    tx_data: dict[str, Any],
    slot: int,
    mint_filter: set[str],
) -> list[dict[str, Any]]:
    """Parse SPL token balance changes from a Solana transaction.

    Scans preTokenBalances and postTokenBalances to find accounts
    whose balance changed for a tracked mint. A transfer involves
    a decrease in one account and an increase in another for the
    same mint within the same transaction.

    Args:
        tx_data: Parsed getTransaction result (jsonParsed encoding).
        slot: Slot number this transaction was confirmed in.
        mint_filter: Set of tracked mint addresses to filter for.

    Returns:
        List of parsed SPL balance change event dicts.
    """
    meta = tx_data.get("meta") or {}
    if not meta:
        return []

    pre_balances: list[dict] = meta.get("preTokenBalances") or []
    post_balances: list[dict] = meta.get("postTokenBalances") or []

    if not pre_balances and not post_balances:
        return []

    # Build a map of account_index -> pre_balance for tracked mints
    pre_map: dict[int, dict[str, Any]] = {}
    for bal in pre_balances:
        mint = bal.get("mint", "")
        if mint in mint_filter:
            pre_map[bal["accountIndex"]] = bal

    post_map: dict[int, dict[str, Any]] = {}
    for bal in post_balances:
        mint = bal.get("mint", "")
        if mint in mint_filter:
            post_map[bal["accountIndex"]] = bal

    if not pre_map and not post_map:
        return []

    events: list[dict[str, Any]] = []
    transaction = tx_data.get("transaction") or {}
    signatures = transaction.get("signatures") or []
    tx_signature = signatures[0] if signatures else ""
    block_time = tx_data.get("blockTime")

    # Process all account indices that appear in either pre or post
    all_indices = set(pre_map.keys()) | set(post_map.keys())

    for idx in all_indices:
        pre = pre_map.get(idx, {})
        post = post_map.get(idx, {})

        mint = post.get("mint") or pre.get("mint", "")
        if mint not in mint_filter:
            continue

        owner = post.get("owner") or pre.get("owner", "")

        pre_amount = pre.get("uiTokenAmount", {}).get("amount", "0")
        post_amount = post.get("uiTokenAmount", {}).get("amount", "0")

        # Parse amounts (strings of raw integer values)
        try:
            pre_val = int(pre_amount)
        except (ValueError, TypeError):
            pre_val = 0
        try:
            post_val = int(post_amount)
        except (ValueError, TypeError):
            post_val = 0

        change = post_val - pre_val

        # Skip zero-change (no transfer for this account)
        if change == 0:
            continue

        # Find token metadata
        token_info = None
        for t in SOLANA_TRACKED_TOKENS:
            if t["mint"] == mint:
                token_info = t
                break
        decimals = token_info["decimals"] if token_info else 0
        symbol = token_info["symbol"] if token_info else ""

        # Build ISO-8601 timestamp
        if block_time and isinstance(block_time, (int, float)):
            ts = datetime.fromtimestamp(block_time, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        events.append(
            {
                "type": "spl_balance_change",
                "chain": "solana",
                "slot": slot,
                "signature": tx_signature,
                "mint": mint,
                "owner": owner,
                "token_account": "",  # Would need accountKeys resolution
                "amount_pre": str(pre_val),
                "amount_post": str(post_val),
                "change": str(change),
                "decimals": decimals,
                "symbol": symbol,
                "token_name": token_info["name"] if token_info else "",
                "blockTimestamp": ts,
                "source": "lite_indexer",
            }
        )

    return events


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
        """Make a JSON-RPC call to a Solana RPC endpoint.

        Args:
            method: Solana RPC method name.
            params: Method parameters.
            endpoint: Specific endpoint to use (default: rotate through list).

        Returns:
            Parsed JSON response dict, or None on failure.
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or [],
        }
        client = await self._get_client()

        endpoints = list(SOLANA_ENDPOINTS)
        if endpoint:
            endpoints = [endpoint]

        last_error: str | None = None
        for url in endpoints:
            try:
                resp = await client.post(url, json=payload, timeout=30.0)
                if resp.status_code == 429:
                    await asyncio.sleep(1)
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, dict) and "result" in data:
                        return data
                    if isinstance(data, dict) and "error" in data:
                        last_error = str(data["error"])
                        continue
                last_error = f"http_{resp.status_code}"
            except (httpx.TimeoutException, httpx.ConnectError) as exc:
                last_error = str(exc)[:60]
                continue

        logger.debug("solana_rpc_failed", method=method, error=last_error)
        return None

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
                except Exception:
                    pass
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

        for token in self.tracked_tokens:
            mint = token["mint"]
            try:
                events = await self._fetch_mint_transfers(mint, from_slot, to_slot)
                all_events.extend(events)
            except Exception as exc:
                logger.warning(
                    "solana_mint_fetch_failed",
                    mint=mint[:20],
                    error=str(exc)[:80],
                )

        return all_events

    async def _fetch_mint_transfers(
        self,
        mint: str,
        from_slot: int,
        to_slot: int,
    ) -> list[dict[str, Any]]:
        """Fetch transfers for a single token mint in a slot range."""
        events: list[dict[str, Any]] = []
        semaphore = asyncio.Semaphore(SOLANA_TX_CONCURRENCY)

        # Step 1: Get signatures for this mint
        result = await self._rpc_call(
            "getSignaturesForAddress",
            [mint, {"limit": MAX_SIGNATURES_PER_TOKEN}],
        )
        if not result or "result" not in result:
            return []

        sig_infos = result["result"]
        if not isinstance(sig_infos, list):
            return []

        # Step 2: Filter signatures by slot range
        # Each sig_info has {signature, slot, err, ...}
        relevant_sigs = [
            s
            for s in sig_infos
            if isinstance(s, dict)
            and s.get("slot") is not None
            and from_slot <= int(s["slot"]) <= to_slot
            and s.get("err") is None  # Skip failed transactions
        ]

        if not relevant_sigs:
            return []

        # Step 3: Fetch and parse each transaction
        async def _fetch_and_parse(sig_info: dict) -> list[dict[str, Any]]:
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
                    return _parse_solana_transfer_events(
                        tx_result["result"],
                        slot,
                        {mint},
                    )
                except Exception as exc:
                    logger.debug("solana_tx_parse_failed", sig=sig[:20], error=str(exc)[:60])
                    return []

        tasks = [_fetch_and_parse(s) for s in relevant_sigs]
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for outcome in outcomes:
            if isinstance(outcome, list):
                events.extend(outcome)

        return events

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

        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        rows: list[tuple] = []

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
                    event.get("chain", "solana"),
                    event.get("slot", 0),
                    event.get("signature", ""),
                    event.get("mint", ""),
                    event.get("owner", ""),
                    event.get("token_account", ""),
                    event.get("amount_pre", "0"),
                    event.get("amount_post", "0"),
                    event.get("change", "0"),
                    event.get("decimals", 0),
                    event.get("symbol", ""),
                    event.get("token_name", ""),
                    ts,
                    event.get("source", "lite_indexer"),
                    now,
                )
            )

        solana_table = "wallet_memory.solana_spl_balance_changes"

        try:
            client.execute(
                f"INSERT INTO {solana_table} "
                "(chain, slot, signature, mint, owner, "
                "token_account, amount_pre, amount_post, "
                "change, decimals, symbol, token_name, "
                "block_timestamp, source, ingested_at) VALUES",
                rows,
            )
            logger.info("solana_clickhouse_inserted", rows=len(rows))
            return len(rows)
        except Exception as exc:
            logger.error("solana_clickhouse_insert_failed", rows=len(rows), error=str(exc)[:150])
            return 0
