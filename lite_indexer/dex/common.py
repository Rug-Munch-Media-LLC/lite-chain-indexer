"""
Shared helpers for DEX event decoding.

Contains the address-unpadding utility and small parsing primitives
used by both V2 and V3 decoders.
"""

from __future__ import annotations


def unpad_address(padded_hex: str) -> str:
    """Unpad a 32-byte hex address to 20-byte.

    topics[1] and topics[2] are indexed params padded to 32 bytes.
    """
    if not padded_hex or padded_hex == "0x":
        return "0x0000000000000000000000000000000000000000"
    raw = padded_hex.removeprefix("0x")
    if len(raw) > 40:
        raw = raw[-40:]
    return "0x" + raw.lower()


def parse_block_number(log: dict) -> int:
    """Extract and normalize the blockNumber from a raw log entry.

    Args:
        log: Raw eth_getLogs result entry.

    Returns:
        Block number as int (0 if unparseable).
    """
    block_hex = log.get("blockNumber", "0x0")
    try:
        return int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
    except (ValueError, TypeError):
        return 0


def parse_log_index(log: dict) -> int:
    """Extract and normalize the logIndex from a raw log entry.

    Args:
        log: Raw eth_getLogs result entry.

    Returns:
        Log index as int (0 if unparseable).
    """
    log_index_hex = log.get("logIndex", "0x0")
    try:
        return int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
    except (ValueError, TypeError):
        return 0
