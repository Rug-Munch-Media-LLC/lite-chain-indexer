"""
Uniswap V3 DEX event decoders.

Handles V3 PoolCreated (factory events) and V3 Swap events:
  V3 PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, ...):
    topics[1] = token0, topics[2] = token1, topics[3] = fee
    data[32:64] = pool address (rightmost 20 bytes)

  V3 Swap:
    topics[1] = sender, topics[2] = recipient
    data = amount0 + amount1 + sqrtPriceX96 + liquidity + tick
"""

from __future__ import annotations

from typing import Any

from .common import unpad_address


def _decode_int256_magnitude(hex_str: str) -> int:
    """Decode a 32-byte two's-complement int256 and return its magnitude.

    Uniswap V3 Swap events carry amount0/amount1 as *signed* int256: one
    leg is the input (positive) and the other the output (negative). The
    trade size of a leg is the absolute value. Without this, negative
    amounts decode to ~2^256 — a value that would make every V3 swap look
    like a whale.
    """
    value = int(hex_str, 16)
    if value >= 2**255:
        value -= 2**256
    return abs(value)


def parse_pool_created_v3(
    log: dict[str, Any],
    chain: str,
) -> dict[str, Any] | None:
    """Parse a single V3 PoolCreated log.

    Args:
        log: Raw eth_getLogs result entry.
        chain: Chain name for event metadata.

    Returns:
        Dict with 'pair_tokens' and 'deployer_event' keys,
        or None if the log is not a valid V3 PoolCreated event.
    """
    topics = log.get("topics", [])
    if len(topics) < 4:
        return None

    data_hex = log.get("data", "0x")
    raw_data = data_hex.removeprefix("0x")
    if len(raw_data) < 128:
        return None

    token0 = unpad_address(topics[1])
    token1 = unpad_address(topics[2])
    pool_hex = raw_data[64:128]
    pool_bytes = pool_hex[24:64]
    pair = "0x" + pool_bytes.lower()

    block_hex = log.get("blockNumber", "0x0")
    try:
        bn = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
    except (ValueError, TypeError):
        bn = 0

    return {
        "pair": pair,
        "token0": token0,
        "token1": token1,
        "deployer_event": {
            "factory": log.get("address", ""),
            "pair_address": pair,
            "token0": token0,
            "token1": token1,
            "version": "v3",
            "blockNumber": bn,
            "transactionHash": log.get("transactionHash", ""),
            "blockTimestamp": "",
        },
    }


def parse_swap_v3(
    log: dict[str, Any],
    chain: str,
    from_block: int,
    to_block: int,
    pair_tokens: dict[str, tuple[str, str]],
) -> dict[str, Any] | None:
    """Parse a single V3 Swap log.

    Args:
        log: Raw eth_getLogs result entry.
        chain: Chain name.
        from_block: Minimum block number filter.
        to_block: Maximum block number filter.
        pair_tokens: Dict mapping pair address to (token0, token1).

    Returns:
        Parsed swap event dict, or None if the log is not a valid V3 swap.
    """
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None

    # Parse block number
    block_hex = log.get("blockNumber", "0x0")
    try:
        block_num = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
    except (ValueError, TypeError):
        return None

    if block_num < from_block or block_num > to_block:
        return None

    log_index_hex = log.get("logIndex", "0x0")
    try:
        log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
    except (ValueError, TypeError):
        log_index = 0

    tx_hash = log.get("transactionHash", "")

    data_hex = log.get("data", "0x")
    raw = data_hex.removeprefix("0x")

    sender = unpad_address(topics[1])
    recipient = unpad_address(topics[2]) if len(topics) > 2 else "0x0000000000000000000000000000000000000000"

    amount0_in = "0"
    amount1_in = "0"
    amount0_out = "0"
    amount1_out = "0"

    if len(raw) >= 256:  # amount0 + amount1 + sqrtPriceX96 + liquidity + tick
        # Signed int256: one leg is negative (the output side). Store the
        # magnitude so whale detection and USD notional see the true size.
        amount0_in = str(_decode_int256_magnitude(raw[0:64]))
        amount1_in = str(_decode_int256_magnitude(raw[64:128]))

    # Resolve token0/token1 from pair→tokens mapping
    pair = log.get("address", "").lower()
    token0, token1 = pair_tokens.get(pair, ("", ""))

    return {
        "type": "swap",
        "dex_type": "v3",
        "dex": "uniswap_v3",
        "chain": chain,
        "blockNumber": block_num,
        "blockTimestamp": "",
        "transactionHash": tx_hash,
        "logIndex": log_index,
        "sender": sender,
        "recipient": recipient,
        "token0": token0,
        "token1": token1,
        "amount0In": amount0_in,
        "amount1In": amount1_in,
        "amount0Out": amount0_out,
        "amount1Out": amount1_out,
        "pairAddress": log.get("address", ""),
        "price_usd": 0.0,
        "volume_usd": 0.0,
        "source": "lite_indexer",
    }
