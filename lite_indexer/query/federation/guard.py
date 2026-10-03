"""Static detection of large tables and of a partition bound in SQL text.

Separated from :mod:`partition` (which *builds* bounds) so the guard's
heuristics are testable in isolation and reusable by any caller that assembles
SQL by hand.

The bound test is deliberately permissive: a false positive lets a query
through to the server, which is the safe direction. A false negative would
block a legitimate query. Only *large* tables are ever gated.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import Final

from lite_indexer.query.federation.errors import UnboundedQueryError
from lite_indexer.query.federation.schemas import LARGE_TABLES

_TABLE_REF: Final[re.Pattern[str]] = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
_BOUND_OP: Final[re.Pattern[str]] = re.compile(
    r"\b(?:block_timestamp|timestamp|block_number|block_slot|min_date)\s*"
    r"(?:>=|<=|>|<|=|between\b|\bin\b)",
    re.IGNORECASE,
)
_PARTITION_HINT: Final[tuple[str, ...]] = ("_partition", "partition(", "partition ")


def extract_tables(sql: str) -> list[str]:
    """Dotted ``db.table`` references in FROM/JOIN, de-duplicated, in order."""
    seen: list[str] = []
    for match in _TABLE_REF.finditer(sql):
        name = match.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def has_bound(sql: str) -> bool:
    """Whether ``sql`` carries a time- or block-range predicate."""
    lowered = sql.lower()
    if any(hint in lowered for hint in _PARTITION_HINT):
        return True
    return bool(_BOUND_OP.search(sql))


def large_tables_in(sql: str) -> list[str]:
    """Large tables (>10e9-capable) referenced by ``sql``."""
    return [t for t in extract_tables(sql) if t in LARGE_TABLES]


def assert_bounded(sql: str, *, extra_large: Sequence[str] = ()) -> tuple[str, ...]:
    """Raise :class:`UnboundedQueryError` if a large table lacks a bound.

    Returns the large tables actually touched, so callers can record which
    tables a query hit.
    """
    guarded = large_tables_in(sql)
    guarded.extend(t for t in extra_large if t not in guarded)
    if guarded and not has_bound(sql):
        raise UnboundedQueryError(guarded, sql)
    return tuple(guarded)
