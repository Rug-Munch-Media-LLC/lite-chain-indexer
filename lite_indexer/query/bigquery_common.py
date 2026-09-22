"""Shared BigQuery helpers (flag parsing + epoch conversion)."""
from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def ms_to_dt(value: Any) -> datetime | None:
    """Convert an epoch-millisecond value to an aware UTC datetime."""
    if value is None or value == "":
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)
    except (TypeError, ValueError):
        logger.debug("bigquery_ms_unparseable", value=str(value)[:40])
        return None

