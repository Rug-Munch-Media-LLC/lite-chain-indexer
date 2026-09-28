"""Top-level LP (Mint/Burn) log parser and the shared topic dispatcher.

Split from :mod:`lite_indexer.dex.parsers` (2026-09-28) so that file stays
at its recorded size. ``parse_dex_logs`` is the entry point every
non-factory topic fetch goes through, so one writer handles swap legs and
LP legs without the writer knowing which is which.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import MAX_EVENTS_PER_BATCH, SWAP_V2_TOPIC, SWAP_V3_TOPIC
from lite_indexer.logging import get_logger

from .log_context import LogDecodeContext
from .lp import lp_event_type, parse_lp_log
from .parsers import parse_swap_logs

logger = get_logger(__name__)

__all__ = ["parse_dex_logs", "parse_lp_logs"]


def parse_dex_logs(logs: list[dict[str, Any]], ctx: LogDecodeContext) -> list[dict[str, Any]]:
    """Dispatch a raw eth_getLogs result to the decoder for ``topic``.

    One entry point for every non-factory topic the indexer fetches: swap
    legs (V2/V3) and LP legs (Mint/Burn). ``type`` is always derived from
    the topic, never hardcoded, so a single writer handles both shapes.

    Args:
        logs: Raw eth_getLogs results for one topic.
        ctx: Chain, block range, topic and pair tokens for this fetch.

    Returns:
        List of parsed event dicts (``type`` already set).
    """
    if ctx.topic in (SWAP_V2_TOPIC, SWAP_V3_TOPIC):
        return parse_swap_logs(logs, ctx)
    if lp_event_type(ctx.topic):
        return parse_lp_logs(logs, ctx)
    return []


def parse_lp_logs(logs: list[dict[str, Any]], ctx: LogDecodeContext) -> list[dict[str, Any]]:
    """Parse raw eth_getLogs results for LP Mint/Burn events.

    Mirrors :func:`parse_swap_logs` (reorg filter, dedup on
    tx_hash:log_index, MAX_EVENTS_PER_BATCH cap) so LP legs obey the same
    bounds as swaps.

    Returns:
        List of parsed LP event dicts with type add_liquidity /
        remove_liquidity. Empty when ``topic`` is not an LP topic.
    """
    if not lp_event_type(ctx.topic):
        return []

    events: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    for log in logs:
        if log.get("removed", False):
            continue
        event = parse_lp_log(log, ctx)
        if event is None:
            continue
        dedup_key = f"{event['transactionHash']}:{event['logIndex']}"
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        events.append(event)
        if len(events) >= MAX_EVENTS_PER_BATCH:
            logger.warning("lp_event_cap_reached", count=MAX_EVENTS_PER_BATCH)
            break

    return events
