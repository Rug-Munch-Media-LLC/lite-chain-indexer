"""
Solana Strategy - Transaction Parsing
======================================
Parse SPL token balance changes from Solana transactions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .models import SOLANA_TRACKED_TOKENS


def parse_solana_transfer_events(
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

    pre_balances: list[dict[str, Any]] = meta.get("preTokenBalances") or []
    post_balances: list[dict[str, Any]] = meta.get("postTokenBalances") or []

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

    # Resolve token account addresses from the transaction's account keys.
    # In jsonParsed encoding each key is a dict with a ``pubkey`` field, and
    # balance entries reference them by ``accountIndex``. The token account
    # is a PDA whose on-chain data carries the wallet ``owner`` — having the
    # address lets scanners resolve who holds each token.
    message = transaction.get("message") or {}
    account_keys = message.get("accountKeys") or []

    def _account_pubkey(account_index: int) -> str:
        try:
            key = account_keys[account_index]
        except (IndexError, TypeError):
            return ""
        if isinstance(key, dict):
            pubkey = key.get("pubkey")
            return str(pubkey) if pubkey is not None else ""
        return str(key)

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
                "token_account": _account_pubkey(idx),
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


#: Validator vote program — vote transactions carry no user/token activity.
_VOTE_PROGRAM_ID = "Vote111111111111111111111111111111111111111"


def is_vote_transaction(tx: dict[str, Any]) -> bool:
    """True for validator vote transactions (skip these early).

    Vote txs are a large share of Solana block volume and never contain
    token/DEX/launch activity. ``jsonParsed`` instructions carry
    ``programId``; fall back to scanning ``accountKeys`` for shapes
    without an instructions list. Never raises (filter must not break a
    sync): any parse error returns False.
    """
    try:
        message = ((tx or {}).get("transaction") or {}).get("message") or {}
        for ix in message.get("instructions") or []:
            if ix.get("programId") == _VOTE_PROGRAM_ID:
                return True
        for key in message.get("accountKeys") or []:
            pk = key.get("pubkey") if isinstance(key, dict) else key
            if pk == _VOTE_PROGRAM_ID:
                return True
    except Exception:  # noqa: BLE001 — filter must never raise
        return False
    return False
