"""
ERC-20 transfer event decoder.

Parses raw eth_getLogs results into NDJSON-compatible transfer events,
handling edge cases like removed logs, empty data, out-of-range blocks,
and NFT transfer filtering.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import MAX_EVENTS_PER_BATCH
from lite_indexer.logging import get_logger

from .common import unpad_address

logger = get_logger(__name__)


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

        events.append(
            {
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
            }
        )

        if len(events) >= MAX_EVENTS_PER_BATCH:
            logger.warning("event_cap_reached", count=MAX_EVENTS_PER_BATCH)
            break

    if duplicates:
        logger.debug("dedup_removed", chain=chain, count=duplicates)

    return events
