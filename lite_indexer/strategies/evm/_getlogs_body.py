"""eth_getLogs request-body construction for the EVM topic fetchers.

Extracted from ``strategies/evm/dex_topic.py`` (shrink-only size ratchet).
One place builds the JSON-RPC envelope so the v2/v3 topic fetchers cannot
drift on block-range encoding (hex, inclusive both ends).
"""

from __future__ import annotations

from typing import Any


def build_getlogs_body(topic: str, range_start: int, range_end: int) -> dict[str, Any]:
    """Return the JSON-RPC envelope for one single-topic getLogs call.

    Args:
        topic: Event topic hash to filter on.
        range_start: Start block (inclusive), encoded as hex.
        range_end: End block (inclusive), encoded as hex.

    Returns:
        The full JSON-RPC request body.
    """
    filter_params = {
        "fromBlock": hex(range_start),
        "toBlock": hex(range_end),
        "topics": [topic],
    }
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [filter_params],
    }
