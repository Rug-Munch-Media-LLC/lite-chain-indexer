"""Solana block-time coercion helpers (producer/consumer contract).

The signature path emits ``blockTimestamp`` as an ISO-8601 string; the
blockwise path historically emitted ``block_timestamp`` as an epoch int,
which the ClickHouse writer silently dropped — those rows stored the ingest
time instead of the block time (breaking freshness, TTL and analytics).
Producers and consumers share these helpers so the contract is explicit.
"""
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

_ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


def iso_from_epoch(value: Any) -> str | None:
    """Epoch seconds (int/float) -> ISO-8601 UTC string; None when unusable."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return datetime.fromtimestamp(float(value), tz=UTC).strftime(_ISO_FMT)
    except (OverflowError, OSError, ValueError):
        return None


def coerce_event_ts(raw: Any, now: datetime) -> datetime:
    """Best-effort event block timestamp: ISO string or epoch -> datetime.

    Falls back to ``now`` for None/garbage so a writer never crashes on an
    unexpected shape. Accepts both contract generations.
    """
    if isinstance(raw, str) and raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return now
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        try:
            return datetime.fromtimestamp(float(raw), tz=UTC)
        except (OverflowError, OSError, ValueError):
            return now
    return now
