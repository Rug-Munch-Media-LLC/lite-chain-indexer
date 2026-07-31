"""
Tron Strategy - Event Parsing
==============================
Parse TRC20 Transfer events from TronGrid responses.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


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
      "from": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
      "to": "...",
      "result": {
        "value": "1000000",
        "from": "...",
        "to": "..."
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
        from_addr = result.get("from", "") or raw.get("from", "")
        to_addr = result.get("to", "") or raw.get("to", "")

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

        log_index = raw.get("log_index", 0)
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
