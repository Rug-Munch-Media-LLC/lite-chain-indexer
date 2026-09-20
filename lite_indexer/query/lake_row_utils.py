"""Shared row-value converters for lake sources (split from biglake_source).

Kept tiny + side-effect free: address normalization and numeric
stringification used when mapping lake rows to the warehouse shape.
"""

from __future__ import annotations

from typing import Any


def _num_str(value: Any) -> str:
    """Stringify lake numerics without float noise for integral values.

    NOTE: the lake stores value columns as DOUBLE, so very large integers
    can lose precision beyond 2**53. Exactness must come from a stricter
    source tier when needed.
    """
    if value is None:
        return "0"
    try:
        fvalue = float(value)
    except (TypeError, ValueError):
        return str(value)
    return str(int(fvalue)) if fvalue.is_integer() else str(fvalue)


def _norm_addr(value: Any) -> str:
    """Normalize AWS hex addresses (32-byte left-padded) to canonical 0x+40."""
    t = str(value or "").lower()
    t = t[2:] if t.startswith("0x") else t
    return "0x" + t[-40:] if t else ""
