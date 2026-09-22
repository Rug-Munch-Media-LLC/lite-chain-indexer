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



class BigQueryQuotaError(RuntimeError):
    """The project's free query-bytes-scanned quota is exhausted.

    Public-dataset scans are billed to OUR project: 1 TiB/month free
    (resets monthly). Fills must be rationed or pointed at a fresh
    project via ``BIGQUERY_PROJECT_ID`` + a new service-account key.
    """


_QUOTA_MARKERS = ("Quota exceeded", "free query bytes", "quotaExceeded")


def is_quota_error(text: str) -> bool:
    """True when a bq failure looks like the free-bytes-scanned quota cap."""
    lowered = text or ""
    return any(m.lower() in lowered.lower() for m in _QUOTA_MARKERS)
