"""Solana launch transaction-shape helpers — base58, instruction iteration, matching.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the full venue/signal model and confidence semantics.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.launch_models import VenueSpec, _Match

logger = get_logger(__name__)

# ── Base58 (self-contained — the RPC returns raw data base58-encoded) ──

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58decode(value: str) -> bytes:
    """Decode a base58 string (Solana addresses / raw instruction data)."""
    if not value:
        return b""
    n = 0
    for ch in value:
        idx = _B58_ALPHABET.find(ch)
        if idx < 0:
            raise ValueError(f"invalid base58 character: {ch!r}")
        n = n * 58 + idx
    pad = len(value) - len(value.lstrip("1"))
    body = n.to_bytes((n.bit_length() + 7) // 8 or 1, "big")
    return b"\x00" * pad + body


def _b58encode(data: bytes) -> str:
    """Encode bytes as base58 (bonding-curve creator pubkey)."""
    if not data:
        return ""
    pad = len(data) - len(data.lstrip(b"\x00"))
    n = int.from_bytes(data, "big")
    chars: list[str] = []
    while n:
        n, rem = divmod(n, 58)
        chars.append(_B58_ALPHABET[rem])
    return "1" * pad + "".join(reversed(chars))


# ── Transaction shape helpers ─────────────────────────────────────────


def _iter_instructions(message: dict[str, Any], meta: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield outer and CPI inner instructions from a jsonParsed tx.

    Both shapes carry ``programId`` / ``accounts`` and either ``parsed``
    (known programs) or raw base58 ``data`` (unknown programs).
    """
    for ix in message.get("instructions") or []:
        if isinstance(ix, dict):
            yield ix
    for group in meta.get("innerInstructions") or []:
        if not isinstance(group, dict):
            continue
        for ix in group.get("instructions") or []:
            if isinstance(ix, dict):
                yield ix


def _parsed_instruction_name(ix: dict[str, Any]) -> str:
    """Return the parsed instruction name (``type`` or ``name``), or ''."""
    parsed = ix.get("parsed")
    if isinstance(parsed, dict):
        name = parsed.get("type") or parsed.get("name")
        if name:
            return str(name)
    return ""


def _pubkey_set(account_keys: list[Any]) -> set[str]:
    """Resolve a list of account keys (dict or plain string) to pubkeys."""
    result: set[str] = set()
    for key in account_keys:
        if isinstance(key, dict):
            pubkey = key.get("pubkey")
            if pubkey:
                result.add(str(pubkey))
        elif key:
            result.add(str(key))
    return result


def _account_pubkey(account_keys: list[Any], index: int) -> str:
    """Resolve the pubkey at ``index`` in the transaction account keys."""
    try:
        key = account_keys[index]
    except (IndexError, TypeError):
        return ""
    if isinstance(key, dict):
        pubkey = key.get("pubkey")
        return str(pubkey) if pubkey is not None else ""
    return str(key) if key else ""


def _iso_timestamp(value: Any) -> str:
    """Format a Unix timestamp as ISO-8601 UTC, or '' when invalid."""
    try:
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(int(value), tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError, OverflowError) as exc:
        logger.debug("solana_launch_ts_parse_failed", error=str(exc)[:60])
    return ""


def _match_instruction(
    ix: dict[str, Any], venue: VenueSpec, message_pubkeys: set[str]
) -> _Match | None:
    """Match one instruction against one venue spec (see module docstring)."""
    if str(ix.get("programId") or "") != venue.program:
        return None
    accounts = ix.get("accounts") or []
    if venue.requires_config_account is not None:
        config = venue.requires_config_account
        if config not in _pubkey_set(accounts) and config not in message_pubkeys:
            return None
        return _Match(venue.venue, True, False, "high", _parsed_instruction_name(ix))
    name = _parsed_instruction_name(ix)
    if name and name in venue.launch_instruction_names:
        return _Match(venue.venue, not venue.graduation, venue.graduation, "medium", name)
    data = str(ix.get("data") or "")
    if data and venue.launch_discriminators:
        try:
            raw = _b58decode(data)
        except ValueError:
            raw = b""
        if len(raw) >= 8 and raw[:8] in venue.launch_discriminators:
            return _Match(venue.venue, not venue.graduation, venue.graduation, "high", name)
    if venue.graduation:
        return _Match(venue.venue, False, True, "low", name)
    return None
