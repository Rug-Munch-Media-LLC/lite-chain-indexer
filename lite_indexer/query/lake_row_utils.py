"""Shared row-value converters for lake sources (split from biglake_source).

Kept tiny + side-effect free: address normalization and the numeric
stringification (in :mod:`lite_indexer.lake_numbers`, shared with the sonarx
ingest mapper so the NULL rules cannot drift between the two lake paths).
"""

from __future__ import annotations

from typing import Any

from lite_indexer.lake_numbers import _num_str  # re-exported for tests

__all__ = ["_norm_addr", "_num_str"]


def _norm_addr(value: Any) -> str:
    """Normalize AWS hex addresses (32-byte left-padded) to canonical 0x+40."""
    text = str(value or "").lower()
    text = text[2:] if text.startswith("0x") else text
    return "0x" + text[-40:] if text else ""
