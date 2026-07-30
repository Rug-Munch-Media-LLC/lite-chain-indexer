"""
DEX event decoders — V2/V3 swap parsing, PairCreated/PoolCreated extraction.

Decodes raw eth_getLogs results into structured swap events and
pair creation events for downstream storage.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    MAX_EVENTS_PER_BATCH,
    PAIR_CREATED_TOPIC,
    POOL_CREATED_TOPIC,
    SWAP_V2_TOPIC,
)
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


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


def parse_pair_created_logs(
    logs: list[dict[str, Any]],
    chain: str,
) -> tuple[dict[str, tuple[str, str]], list[dict[str, Any]]]:
    """Parse PairCreated (V2) and PoolCreated (V3) events from factory logs.

    V2 PairCreated(address indexed token0, address indexed token1, address pair, uint256):
      topics[1] = token0, topics[2] = token1
      data[0:32] = pair address (rightmost 20 bytes)

    V3 PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, ...):
      topics[1] = token0, topics[2] = token1, topics[3] = fee
      data[32:64] = pool address (rightmost 20 bytes)

    Args:
        logs: Raw eth_getLogs results (pre-filtered by factory address + topic).
        chain: Chain name for event metadata.

    Returns:
        Tuple of (pair_tokens dict, deployer_events list).
    """
    pair_tokens: dict[str, tuple[str, str]] = {}
    deployer_events: list[dict[str, Any]] = []

    for log in logs:
        if log.get("removed", False):
            continue

        topics = log.get("topics", [])
        data_hex = log.get("data", "0x")
        raw_data = data_hex.removeprefix("0x")

        if not topics:
            continue

        event_topic = topics[0]

        if event_topic == PAIR_CREATED_TOPIC:
            # V2 PairCreated
            if len(topics) < 3:
                continue
            token0 = unpad_address(topics[1])
            token1 = unpad_address(topics[2])
            if len(raw_data) < 64:
                continue
            pair_bytes = raw_data[24:64]
            pair = "0x" + pair_bytes.lower()

        elif event_topic == POOL_CREATED_TOPIC:
            # V3 PoolCreated
            if len(topics) < 4:
                continue
            token0 = unpad_address(topics[1])
            token1 = unpad_address(topics[2])
            if len(raw_data) < 128:
                continue
            pool_hex = raw_data[64:128]
            pool_bytes = pool_hex[24:64]
            pair = "0x" + pool_bytes.lower()
        else:
            continue

        pair_tokens[pair] = (token0, token1)

        block_hex = log.get("blockNumber", "0x0")
        try:
            bn = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        except (ValueError, TypeError):
            bn = 0

        deployer_events.append({
            "factory": log.get("address", ""),
            "pair_address": pair,
            "version": "v3" if event_topic == POOL_CREATED_TOPIC else "v2",
            "blockNumber": bn,
            "transactionHash": log.get("transactionHash", ""),
            "blockTimestamp": "",
        })

    return pair_tokens, deployer_events


def parse_swap_logs(
    logs: list[dict[str, Any]],
    chain: str,
    from_block: int,
    to_block: int,
    topic: str,
    pair_tokens: dict[str, tuple[str, str]],
) -> list[dict[str, Any]]:
    """Parse raw eth_getLogs results for swap events (V2 or V3).

    V2 (Uniswap V2 style):
      topics[1] = sender, topics[2] = recipient
      data = amount0In + amount1In + amount0Out + amount1Out (4× uint256)

    V3 (Uniswap V3 style):
      topics[1] = sender, topics[2] = recipient
      data = amount0 + amount1 + sqrtPriceX96 + liquidity + tick

    Args:
        logs: Raw eth_getLogs results.
        chain: Chain name.
        from_block: Minimum block number filter.
        to_block: Maximum block number filter.
        topic: Event topic (V2 or V3 swap).
        pair_tokens: Dict mapping pair address to (token0, token1).

    Returns:
        List of parsed swap event dicts.
    """
    is_v2 = topic == SWAP_V2_TOPIC
    events: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicates = 0

    for log in logs:
        if log.get("removed", False):
            continue

        topics = log.get("topics", [])
        if len(topics) < 3:
            continue

        # Parse block number
        block_hex = log.get("blockNumber", "0x0")
        try:
            block_num = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        except (ValueError, TypeError):
            continue

        if block_num < from_block or block_num > to_block:
            continue

        log_index_hex = log.get("logIndex", "0x0")
        try:
            log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
        except (ValueError, TypeError):
            log_index = 0

        tx_hash = log.get("transactionHash", "")
        dedup_key = f"{tx_hash}:{log_index}"
        if dedup_key in seen_keys:
            duplicates += 1
            continue
        seen_keys.add(dedup_key)

        data_hex = log.get("data", "0x")
        raw = data_hex.removeprefix("0x")

        if is_v2:
            # V2 swap decode
            sender = unpad_address(topics[1])
            recipient = (
                unpad_address(topics[2]) if len(topics) > 2
                else "0x0000000000000000000000000000000000000000"
            )

            amount0_in = "0"
            amount1_in = "0"
            amount0_out = "0"
            amount1_out = "0"

            if len(raw) >= 256:  # 4 × 32 bytes
                amount0_in = str(int(raw[0:64], 16))
                amount1_in = str(int(raw[64:128], 16))
                amount0_out = str(int(raw[128:192], 16))
                amount1_out = str(int(raw[192:256], 16))

            dex_type = "v2"
            dex_name = "uniswap_v2"
        else:
            # V3 swap decode
            sender = unpad_address(topics[1])
            recipient = (
                unpad_address(topics[2]) if len(topics) > 2
                else "0x0000000000000000000000000000000000000000"
            )

            amount0_in = "0"
            amount1_in = "0"
            amount0_out = "0"
            amount1_out = "0"

            if len(raw) >= 256:  # amount0 + amount1 + sqrtPriceX96 + liquidity + tick
                amount0_in = str(int(raw[0:64], 16))
                amount1_in = str(int(raw[64:128], 16))

            dex_type = "v3"
            dex_name = "uniswap_v3"

        # Resolve token0/token1 from pair→tokens mapping
        pair = log.get("address", "").lower()
        token0, token1 = pair_tokens.get(pair, ("", ""))

        events.append({
            "type": "swap",
            "dex_type": dex_type,
            "dex": dex_name,
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
        })

        if len(events) >= MAX_EVENTS_PER_BATCH:
            logger.warning("swap_event_cap_reached", count=MAX_EVENTS_PER_BATCH)
            break

    if duplicates:
        logger.debug("swap_dedup_removed", chain=chain, count=duplicates)

    return events


def parse_transfer_logs(
    logs: list[dict[str, Any]],
    chain: str,
    from_block: int,
    to_block: int,
) -> list[dict[str, Any]]:
    """Parse raw eth_getLogs results into NDJSON-compatible transfer events.

    Handles edge cases: removed logs, empty data, out-of-range blocks,
    and NFT transfer filtering.

    Args:
        logs: Raw eth_getLogs results.
        chain: Chain name.
        from_block: Minimum block number filter.
        to_block: Maximum block number filter.

    Returns:
        List of parsed transfer event dicts.
    """
    events: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicates = 0

    for log in logs:
        if log.get("removed", False):
            continue

        topics = log.get("topics", [])
        if len(topics) < 3:
            continue

        from_addr = unpad_address(topics[1])
        to_addr = unpad_address(topics[2])

        # Parse value from data field
        value_hex = log.get("data", "0x0")
        try:
            if isinstance(value_hex, str) and value_hex.startswith("0x"):
                value_str = str(int(value_hex, 16))
            else:
                value_str = str(value_hex)
        except (ValueError, TypeError):
            value_str = "0"

        # Skip NFT transfers (ERC721/ERC1155)
        if value_str == "0" and len(topics) >= 4:
            continue

        # Parse block number
        block_hex = log.get("blockNumber", "0x0")
        try:
            block_num = int(block_hex, 16) if isinstance(block_hex, str) else int(block_hex)
        except (ValueError, TypeError):
            continue

        if block_num < from_block or block_num > to_block:
            continue

        log_index_hex = log.get("logIndex", "0x0")
        try:
            log_index = int(log_index_hex, 16) if isinstance(log_index_hex, str) else int(log_index_hex)
        except (ValueError, TypeError):
            log_index = 0

        tx_hash = log.get("transactionHash", "")
        dedup_key = f"{tx_hash}:{log_index}"
        if dedup_key in seen_keys:
            duplicates += 1
            continue
        seen_keys.add(dedup_key)

        events.append({
            "type": "transfer",
            "chain": chain,
            "blockNumber": block_num,
            "blockTimestamp": "",
            "txHash": tx_hash,
            "logIndex": log_index,
            "from": from_addr,
            "to": to_addr,
            "tokenAddress": log.get("address", ""),
            "value": value_str,
            "tokenSymbol": None,
            "tokenName": None,
            "source": "lite_indexer",
        })

        if len(events) >= MAX_EVENTS_PER_BATCH:
            logger.warning("event_cap_reached", count=MAX_EVENTS_PER_BATCH)
            break

    if duplicates:
        logger.debug("dedup_removed", chain=chain, count=duplicates)

    return events
