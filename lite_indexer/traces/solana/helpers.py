"""Shared constants and small helpers for Solana trace parsing.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

# ClickHouse table for Solana transfer traces. Override with
# CH_SOLANA_TRANSFERS_TABLE for operators who prefer a different database.
SOLANA_TRANSFERS_TABLE: str = os.environ.get("CH_SOLANA_TRANSFERS_TABLE", "wallet_memory.solana_transfers")

# ── Program IDs ────────────────────────────────────────────────────────

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
STAKE_PROGRAM = "Stake11111111111111111111111111111111111111"
COMPUTE_BUDGET_PROGRAM = "ComputeBudget111111111111111111111111111111"
VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"

# Transfer types stored in the ``transfer_type`` column (LowCardinality).
NATIVE_SOL = "native_sol"
CPI_SOL = "cpi_sol"
SPL_TRANSFER = "spl_transfer"
SPL_TRANSFER_CHECKED = "spl_transfer_checked"
ACCOUNT_CREATE = "account_create"
ACCOUNT_CLOSE = "account_close"
STAKE_DELEGATE = "stake_delegate"

TRANSFER_TYPES: tuple[str, ...] = (
    NATIVE_SOL,
    CPI_SOL,
    SPL_TRANSFER,
    SPL_TRANSFER_CHECKED,
    ACCOUNT_CREATE,
    ACCOUNT_CLOSE,
    STAKE_DELEGATE,
)


def _to_int(value: Any, default: int = 0) -> int:
    """Safely coerce a JSON amount (str/int) into an int.

    Args:
        value: Raw amount from a parsed instruction (usually a string).
        default: Value returned when the coercion fails.

    Returns:
        Parsed int, or ``default``.
    """
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return default


def _to_timestamp(block_time: Any) -> datetime | None:
    """Coerce a Solana blockTime into a UTC datetime.

    Args:
        block_time: Unix seconds (int/float) or None.

    Returns:
        Timezone-aware UTC datetime, or None.
    """
    try:
        return datetime.fromtimestamp(int(block_time), tz=UTC)
    except (ValueError, TypeError, OverflowError):
        return None


def _account_pubkey(account_keys: list[Any], account_index: int) -> str:
    """Resolve an account index to a pubkey string.

    jsonParsed encoding renders each account key as a dict with a
    ``pubkey`` field; older/compact encodings render plain strings.

    Args:
        account_keys: The transaction's ``message.accountKeys`` list.
        account_index: Index into the list.

    Returns:
        The pubkey string, or '' when out of range.
    """
    try:
        key = account_keys[account_index]
    except (IndexError, TypeError):
        return ""
    if isinstance(key, dict):
        pubkey = key.get("pubkey")
        return str(pubkey) if pubkey is not None else ""
    return str(key)


def _balance_delta(pre_balances: list[Any], post_balances: list[Any], pubkey: str, account_keys: list[Any]) -> int:
    """Compute an account's lamport change from pre/post balances.

    Used for ``account_close`` rows where the parsed instruction does not
    carry the residual lamport amount (only the account + destination).

    Args:
        pre_balances: ``meta.preBalances`` lamport list.
        post_balances: ``meta.postBalances`` lamport list.
        pubkey: Account whose balance delta is wanted.
        account_keys: The transaction's account key list (to resolve index).

    Returns:
        Absolute lamport delta (0 when the account is not in the tx).
    """
    idx = -1
    for i, _key in enumerate(account_keys):
        if _account_pubkey(account_keys, i) == pubkey:
            idx = i
            break
    if idx < 0 or idx >= len(pre_balances) or idx >= len(post_balances):
        return 0
    try:
        delta = int(post_balances[idx]) - int(pre_balances[idx])
    except (ValueError, TypeError, IndexError):
        return 0
    return abs(delta)


def _known_decimals(mint: str) -> int:
    """Return known decimals for a tracked mint, or 0.

    Looked up lazily from the strategy's tracked-token list to avoid a
    module-level import cycle (strategy imports this module).

    Args:
        mint: SPL mint address.

    Returns:
        Decimals for known mints, else 0.
    """
    try:
        from lite_indexer.strategies.solana.models import SOLANA_TRACKED_TOKENS  # noqa: PLC0415
    except ImportError:
        return 0
    for token in SOLANA_TRACKED_TOKENS:
        if token.get("mint") == mint:
            return _to_int(token.get("decimals"), 0)
    return 0


__all__ = [
    "ACCOUNT_CLOSE",
    "ACCOUNT_CREATE",
    "ASSOCIATED_TOKEN_PROGRAM",
    "COMPUTE_BUDGET_PROGRAM",
    "CPI_SOL",
    "NATIVE_SOL",
    "SOLANA_TRANSFERS_TABLE",
    "SPL_TRANSFER",
    "SPL_TRANSFER_CHECKED",
    "STAKE_DELEGATE",
    "STAKE_PROGRAM",
    "SYSTEM_PROGRAM",
    "TOKEN_2022_PROGRAM",
    "TOKEN_PROGRAM",
    "TRANSFER_TYPES",
    "VOTE_PROGRAM",
    "_account_pubkey",
    "_balance_delta",
    "_known_decimals",
    "_to_int",
    "_to_timestamp",
]
