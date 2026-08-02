"""
Tron Strategy - Event Parsing
==============================
Parse TRC20 Transfer events from TronGrid responses.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Any, cast

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Tron mainnet version byte (0x41) + 20-byte address → base58check.
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _hex_to_base58check(hex_addr: str) -> str | None:
    """Encode a 0x41-prefixed (or bare) Tron hex address as base58check.

    Args:
        hex_addr: Hex address string (41-prefixed, optionally 0x-prefixed).

    Returns:
        Base58check address, or None if the input is not valid hex.
    """
    try:
        raw = bytes.fromhex(hex_addr)
    except ValueError:
        return None

    checksum = hashlib.sha256(hashlib.sha256(raw).digest()).digest()[:4]
    payload = raw + checksum

    num = int.from_bytes(payload, "big")
    encoded = ""
    while num > 0:
        num, rem = divmod(num, 58)
        encoded = _BASE58_ALPHABET[rem] + encoded

    # Leading zero bytes → '1' characters
    for byte in payload:
        if byte == 0:
            encoded = "1" + encoded
        else:
            break
    return encoded


def _normalize_tron_address(addr: Any) -> str:
    """Normalize a Tron address to base58check format.

    TronGrid returns addresses as base58check (e.g. TR7NH...) for the
    top-level from_address/to_address fields, but the decoded event args
    in ``result`` may come back as 0x41-prefixed hex on some contracts.
    Mixed formats break downstream matching, so we normalize everything.

    Args:
        addr: Address value from TronGrid (base58, hex, or empty).

    Returns:
        Normalized base58check address, or the input unchanged when it
        cannot be recognized/converted.
    """
    if not addr:
        return ""
    addr = str(addr).strip()
    if not addr:
        return ""
    # Already base58check (34 chars, starts with 'T')
    if len(addr) == 34 and addr.startswith("T"):
        return cast(str, addr)
    # Hex form: 0x41... / 41... (version byte + 20-byte address)
    hex_addr = addr[2:] if addr.startswith("0x") else addr
    if len(hex_addr) == 42 and hex_addr.startswith("41"):
        converted = _hex_to_base58check(hex_addr)
        if converted:
            return converted
    return cast(str, addr)


def parse_tron_transfer_event(
    raw: dict[str, Any],
    token: dict[str, Any],
    chain: str,
) -> dict[str, Any] | None:
    """Parse a single TRC20 Transfer event from TronGrid response.

    TronGrid event format:
    {
      "block_number": "12345",
      "transaction_id": "0x...",
      "event_index": 0,           # real log index (NOT log_index)
      "from_address": "TR7NH...", # base58
      "to_address": "...",
      "contract_address": "...",
      "result": {
        "value": "1000000",
        "from": "41...",          # may be hex on some contracts
        "to": "41..."
      },
      "block_timestamp": 1700000000000
    }

    Args:
        raw: Raw event dict from TronGrid.
        token: Tracked token metadata dict.
        chain: Chain name.

    Returns:
        Parsed event dict compatible with evm_transfers schema,
        or None if the event cannot be parsed.
    """
    try:
        result = raw.get("result") or {}
        from_addr = raw.get("from_address") or result.get("from") or raw.get("from", "")
        to_addr = raw.get("to_address") or result.get("to") or raw.get("to", "")
        from_addr = _normalize_tron_address(from_addr)
        to_addr = _normalize_tron_address(to_addr)

        if not from_addr or not to_addr:
            return None

        value = result.get("value", "0")
        if isinstance(value, str) and value.startswith("0x"):
            try:
                value = str(int(value, 16))
            except ValueError:
                value = "0"

        block_str = raw.get("block_number", "0")
        if isinstance(block_str, str):
            block_num = int(block_str)
        else:
            block_num = int(block_str)

        tx_hash = raw.get("transaction_id", "")

        # TronGrid reports the real log index as event_index.
        log_index = raw.get("event_index", raw.get("log_index", 0))
        if isinstance(log_index, str):
            log_index = int(log_index)

        # Parse timestamp from TronGrid (milliseconds since epoch)
        ts_raw = raw.get("block_timestamp", 0)
        if ts_raw:
            try:
                ts_sec = int(ts_raw) / 1000.0
                ts = datetime.fromtimestamp(ts_sec, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError, OSError):
                ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

        return {
            "type": "transfer",
            "chain": chain,
            "blockNumber": block_num,
            "blockTimestamp": ts,
            "txHash": tx_hash,
            "txID": tx_hash,
            "logIndex": log_index,
            "from": from_addr,
            "to": to_addr,
            "tokenAddress": token["address"],
            "value": str(value),
            "tokenSymbol": token["symbol"],
            "tokenName": token.get("name"),
            "source": "lite_indexer",
        }
    except Exception as exc:
        logger.debug("tron_event_parse_failed", error=str(exc)[:80])
        return None


# Backward-compat alias for existing importers of _parse_tron_transfer_event
_parse_tron_transfer_event = parse_tron_transfer_event
