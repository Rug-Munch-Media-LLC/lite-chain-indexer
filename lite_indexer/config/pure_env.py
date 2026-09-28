"""Pure parsers for operator-facing environment values.

Own module so ``config/indexing.py`` and ``config/rpc.py`` stay under their
shrink-only size ratchets, and so the bounds are testable with a plain string
argument instead of mutating ``os.environ`` in a test. Nothing here reads the
environment: every function takes the raw value and returns the resolved one.
"""

from __future__ import annotations

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Documented disk cap when ``DISK_CAP_GB`` is unset or unparsable.
DEFAULT_DISK_CAP_GB = 10.0

_NUMERIC = set("0123456789.+-")


def disk_cap_gb(raw: str | None) -> float:
    """Disk cap in GB from the raw ``DISK_CAP_GB`` value.

    An unset, blank or unparsable value keeps the documented default rather
    than crashing the daemon at import time on a typo.
    """
    text = (raw or "").strip()
    if not text or not set(text) <= _NUMERIC:
        return DEFAULT_DISK_CAP_GB
    try:
        return float(text)
    except ValueError as exc:
        logger.warning("disk_cap_gb_unparsable", error=str(exc)[:60], raw=text[:24])
        return DEFAULT_DISK_CAP_GB


def env_flag(raw: str | None) -> bool:
    """Resolve a default-ON operator switch from its raw value.

    Only the exact string ``"0"`` turns it off, so an unset or unparsable
    value keeps the default instead of silently disabling a feature.
    """
    return (raw if raw is not None else "1") != "0"


__all__ = ["DEFAULT_DISK_CAP_GB", "disk_cap_gb", "env_flag"]
