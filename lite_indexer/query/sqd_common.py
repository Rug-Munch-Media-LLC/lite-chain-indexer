"""Shared constants and helpers for the SQD Portal readers.

Split out of :mod:`lite_indexer.query.sqd_source` so the per-chain reader
modules can share the ERC-20 ``Transfer`` topic and the epoch→datetime helper
without importing the facade (which would create an import cycle).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)
#: ERC-20 ``Transfer`` event topic (Tron logs carry it without the 0x prefix).
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def ts(value: Any) -> datetime | None:
    """Epoch → datetime; auto-detects milliseconds (Tron uses ms)."""
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        logger.debug("sqd_ts_unparseable", value=str(value)[:40])
        return None
    if seconds > 10**12:
        seconds //= 1000
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError):
        logger.debug("sqd_ts_out_of_range", value=str(value)[:40])
        return None
