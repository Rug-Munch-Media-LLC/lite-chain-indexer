"""
Uniswap V2 DEX event decoders.

Handles V2 PairCreated (factory events) and V2 Swap events:
  V2 PairCreated(address indexed token0, address indexed token1, address pair, uint256):
    topics[1] = token0, topics[2] = token1
    data[0:32] = pair address (rightmost 20 bytes)

  V2 Swap:
    topics[1] = sender, topics[2] = recipient
    data = amount0In + amount1In + amount0Out + amount1Out (4× uint256)
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

from .common import unpad_address

logger = get_logger(__name__)


def parse_pair_created_v2(
    log: dict[str, Any],
    chain: str,
) -> dict[str, Any] | None:
    """Parse a single V2 PairCreated log.

    Args:
        log: Raw eth_getLogs result entry.
        chain: Chain name for event metadata.

    Returns:
        Dict with 'pair_tokens' and 'deployer_event' keys,
        or None if the log is not a valid V2 PairCreated event.
    """
    topics = log.get("topics", [])
    if len(topics) < 3:
        return None

    data_hex = log.get("data", "0x")
    raw_data = data_hex.removeprefix("0x")
    if len(raw_data) < 64:
        return None

    token0 = unpad_address(topics[1])
    token1 = unpad_address(topics[2])
    pair_bytes = raw_data[24:64]
    pair = "0x" + pair_bytes.lower()

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
            "version": "v2",
            "blockNumber": bn,
            "transactionHash": log.get("transactionHash", ""),
            "blockTimestamp": "",
        },
    }


def parse_swap_v2(
    log: dict[str, Any],
    chain: str,
    from_block: int,
    to_block: int,
    pair_tokens: dict[str, tuple[str, str]],
) -> dict[str, Any] | None:
    """Parse a single V2 Swap log.

    Args:
        log: Raw eth_getLogs result entry.
        chain: Chain name.
        from_block: Minimum block number filter.
        to_block: Maximum block number filter.
        pair_tokens: Dict mapping pair address to (token0, token1).

    Returns:
        Parsed swap event dict, or None if the log is not a valid V2 swap.
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

    if len(raw) >= 256:  # 4 × 32 bytes
        amount0_in = str(int(raw[0:64], 16))
        amount1_in = str(int(raw[64:128], 16))
        amount0_out = str(int(raw[128:192], 16))
        amount1_out = str(int(raw[192:256], 16))

    # Resolve token0/token1 from pair→tokens mapping
    pair = log.get("address", "").lower()
    token0, token1 = pair_tokens.get(pair, ("", ""))

    return {
        "type": "swap",
        "dex_type": "v2",
        "dex": "uniswap_v2",
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
