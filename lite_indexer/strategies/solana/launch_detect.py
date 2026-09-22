"""Solana launch detector — pure transaction → LaunchEvent matching.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the full venue/signal model and confidence semantics.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.launch_models import _VENUE_PRIORITY, VENUES, LaunchEvent
from lite_indexer.strategies.solana.launch_tx import (
    _account_pubkey,
    _iso_timestamp,
    _iter_instructions,
    _match_instruction,
    _pubkey_set,
)

logger = get_logger(__name__)


def detect_launch(tx: dict[str, Any], slot: int | None = None) -> LaunchEvent | None:
    """Detect a Solana token-launch / graduation event in a transaction.

    Pure function — no I/O. ``tx`` is a ``getTransaction`` result
    (jsonParsed encoding) with the shape the strategy already parses:
    ``transaction.message.accountKeys/instructions``,
    ``meta.postTokenBalances``, ``meta.innerInstructions``, ``slot`` and
    ``blockTime``.

    Matching is exact where the spec gives exact constants (pump.fun
    create discriminator, LetsBonk config account on LaunchLab) and
    name/program-presence based elsewhere — see the module docstring for
    the per-venue confidence model.

    Args:
        tx: Parsed Solana transaction result dict.
        slot: The transaction's slot when the caller already knows it
            (e.g. the block-based extraction pass, whose per-transaction
            dicts come from ``getBlock`` and carry no top-level
            ``slot``). Overrides ``tx["slot"]`` when provided.

    Returns:
        :class:`LaunchEvent` with the new mint, creator, venue and
        launch/graduation signals, or None when the tx contains no
        launch signal (or no post-token-balance mint to attribute).
    """
    transaction = tx.get("transaction")
    if not isinstance(transaction, dict):
        return None
    message = transaction.get("message")
    if not isinstance(message, dict):
        return None
    meta = tx.get("meta")
    if not isinstance(meta, dict):
        return None

    mint = _extract_mint(meta)
    if not mint:
        return None

    account_keys = message.get("accountKeys")
    account_keys = account_keys if isinstance(account_keys, list) else []
    message_pubkeys = _pubkey_set(account_keys)

    matches: list[Any] = []
    for ix in _iter_instructions(message, meta):
        for venue in VENUES:
            match = _match_instruction(ix, venue, message_pubkeys)
            if match is not None:
                matches.append(match)
                break

    if not matches:
        return None

    return _build_event(tx, slot, mint, account_keys, matches)


def _extract_mint(meta: dict[str, Any]) -> str:
    """Extract the new token mint (spec: ``meta.postTokenBalances[0].mint``)."""
    post_balances = meta.get("postTokenBalances")
    if not isinstance(post_balances, list) or not post_balances:
        logger.debug("solana_launch_skip_no_post_token_balances")
        return ""
    first = post_balances[0]
    if not isinstance(first, dict):
        return ""
    return str(first.get("mint") or "")


def _build_event(
    tx: dict[str, Any],
    slot: int | None,
    mint: str,
    account_keys: list[Any],
    matches: list[Any],
) -> LaunchEvent | None:
    """Assemble the LaunchEvent from prioritized venue matches."""
    matches.sort(key=lambda m: _VENUE_PRIORITY.get(m.venue, len(VENUES)))
    launch_matches = [m for m in matches if m.launch_signal]
    graduation_matches = [m for m in matches if m.graduation_signal]

    transaction = tx.get("transaction") or {}
    signatures = transaction.get("signatures")
    signature = str(signatures[0]) if isinstance(signatures, list) and signatures else ""
    try:
        launch_slot = int(slot) if slot is not None else int(tx.get("slot") or 0)
    except (ValueError, TypeError):
        launch_slot = 0

    common: dict[str, Any] = {
        "signature": signature,
        "slot": launch_slot,
        "block_timestamp": _iso_timestamp(tx.get("blockTime")),
        "mint": mint,
        "creator": _account_pubkey(account_keys, 0),
        "ingested_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if launch_matches:
        primary = launch_matches[0]
        return LaunchEvent(
            **common,
            venue=primary.venue,
            launch_signal=True,
            graduation_signal=bool(graduation_matches),
            confidence=primary.confidence,
            instruction_name=primary.instruction_name,
        )

    if graduation_matches:
        primary = graduation_matches[0]
        return LaunchEvent(
            **common,
            venue=primary.venue,
            launch_signal=False,
            graduation_signal=True,
            confidence=primary.confidence,
            instruction_name=primary.instruction_name,
        )

    return None
