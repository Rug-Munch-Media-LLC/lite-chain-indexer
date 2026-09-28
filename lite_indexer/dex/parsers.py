"""
Top-level DEX event decoders — dispatch raw eth_getLogs results to the
V2 or V3 per-log parsers.

These are the public entry points used by the EVM strategy handlers:
  - parse_pair_created_logs: dispatches V2 PairCreated / V3 PoolCreated
  - parse_swap_logs:         dispatches V2 / V3 swap logs by topic
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

from .log_context import LogDecodeContext
from .v2 import parse_pair_created_v2, parse_swap_v2
from .v3 import parse_pool_created_v3, parse_swap_v3

logger = get_logger(__name__)


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
        if not topics:
            continue

        event_topic = topics[0]

        if event_topic == PAIR_CREATED_TOPIC:
            parsed = parse_pair_created_v2(log, chain)
        elif event_topic == POOL_CREATED_TOPIC:
            parsed = parse_pool_created_v3(log, chain)
        else:
            continue

        if parsed is None:
            continue

        pair_tokens[parsed["pair"]] = (parsed["token0"], parsed["token1"])
        deployer_events.append(parsed["deployer_event"])

    return pair_tokens, deployer_events


def parse_swap_logs(logs: list[dict[str, Any]], ctx: LogDecodeContext) -> list[dict[str, Any]]:
    """Parse raw eth_getLogs results for swap events (V2 or V3).

    V2 (Uniswap V2 style):
      topics[1] = sender, topics[2] = recipient
      data = amount0In + amount1In + amount0Out + amount1Out (4× uint256)

    V3 (Uniswap V3 style):
      topics[1] = sender, topics[2] = recipient
      data = amount0 + amount1 + sqrtPriceX96 + liquidity + tick

    Args:
        logs: Raw eth_getLogs results.
        ctx.chain: Chain name.
        ctx.from_block: Minimum block number filter.
        ctx.to_block: Maximum block number filter.
        ctx.topic: Event ctx.topic (V2 or V3 swap).
        ctx.pair_tokens: Dict mapping pair address to (token0, token1).

    Returns:
        List of parsed swap event dicts.
    """
    is_v2 = ctx.topic == SWAP_V2_TOPIC
    events: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    duplicates = 0

    for log in logs:
        if log.get("removed", False):
            continue

        topics = log.get("topics", [])
        if len(topics) < 3:
            continue

        if is_v2:
            event = parse_swap_v2(log, ctx.chain, ctx.from_block, ctx.to_block, ctx.pair_tokens)
        else:
            event = parse_swap_v3(log, ctx.chain, ctx.from_block, ctx.to_block, ctx.pair_tokens)

        if event is None:
            continue

        dedup_key = f"{event['transactionHash']}:{event['logIndex']}"
        if dedup_key in seen_keys:
            duplicates += 1
            continue
        seen_keys.add(dedup_key)

        events.append(event)

        if len(events) >= MAX_EVENTS_PER_BATCH:
            logger.warning("swap_event_cap_reached", count=MAX_EVENTS_PER_BATCH)
            break

    if duplicates:
        logger.debug("swap_dedup_removed", chain=ctx.chain, count=duplicates)

    return events
