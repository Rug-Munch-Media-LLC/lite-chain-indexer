"""Numeric conversions shared by the lake row mappers.

The AWS public-blockchain lakes store token amounts as ``DOUBLE``. That has two
consequences the mappers must not paper over:

* A missing value is ``NULL``, meaning "the lake does not know". It must stay
  ``None`` (ClickHouse ``NULL``), never ``"0"`` — a fabricated zero is
  indistinguishable from a real zero-value transfer and silently corrupts
  balances and volume.
* ``DOUBLE`` is exact only up to ``2**53``. An 18-decimal token amount above
  that is already rounded inside the lake; no formatting recovers the missing
  digits. Exactness must come from a stricter source tier (RPC/log decode);
  this module only guarantees it never invents a different number.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

# Largest magnitude a float64 represents exactly (integers).
DOUBLE_EXACT_INT_LIMIT = 2**53


def num_str(value: Any) -> str | None:
    """Render a lake numeric as a decimal string, preserving NULL.

    Args:
        value: Raw lake column value (``DOUBLE``, ``None``, or text).

    Returns:
        ``None`` when the lake value is NULL, otherwise a decimal string with
        no exponent notation for integral values. Non-numeric input is passed
        through as ``str(value)``.
    """
    if value is None:
        return None
    try:
        fvalue = float(value)
    except (TypeError, ValueError):
        logger.debug("lake_number_fallback", value=str(value)[:40])
        return str(value)
    if not fvalue.is_integer():
        return str(fvalue)
    if abs(fvalue) > DOUBLE_EXACT_INT_LIMIT:
        # Rounded inside the lake; surfaced so a precision-limited backfill is
        # visible rather than silent.
        logger.debug("lake_value_precision_limited", value=fvalue)
    return format(fvalue, ".0f")


_num_str = num_str

__all__ = ["DOUBLE_EXACT_INT_LIMIT", "num_str"]
