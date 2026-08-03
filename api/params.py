"""Shared query-parameter helpers: chain filter, date range, pagination."""

from __future__ import annotations

from datetime import datetime

from api.config import DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT, MAX_PAGE_OFFSET
from api.db import normalize_date

# Chain names present in the warehouse (from system.tables / dex_events).
KNOWN_CHAINS: frozenset[str] = frozenset(
    {
        "ethereum",
        "base",
        "bsc",
        "polygon",
        "arbitrum",
        "optimism",
        "avalanche",
        "fantom",
        "linea",
        "scroll",
        "zksync",
        "mantle",
        "gnosis",
        "celo",
        "tron",
        "solana",
    }
)


def clamp_limit(limit: int | None) -> int:
    """Clamp a page limit into [1, MAX_PAGE_LIMIT] (default DEFAULT_PAGE_LIMIT)."""
    if limit is None:
        return DEFAULT_PAGE_LIMIT
    return max(1, min(int(limit), MAX_PAGE_LIMIT))


def clamp_offset(offset: int | None) -> int:
    """Clamp a page offset into [0, MAX_PAGE_OFFSET]."""
    if offset is None:
        return 0
    return max(0, min(int(offset), MAX_PAGE_OFFSET))


def chain_clause(chain: str | None, column: str = "chain") -> tuple[str, dict[str, str]]:
    """Return (SQL fragment, params) for an optional chain filter.

    The chain value is normalized to lowercase; when it is not a known
    chain the fragment is empty (all chains) — unknown chains simply
    return no rows downstream, matching ClickHouse semantics.
    """
    if not chain:
        return "", {}
    normalized = chain.strip().lower()
    return f" AND {column} = %(chain)s", {"chain": normalized}


def date_range_clause(
    start_date: str | None,
    end_date: str | None,
    column: str = "block_timestamp",
) -> tuple[str, dict[str, datetime]]:
    """Return (SQL fragment, params) for an inclusive date range filter.

    ``start_date``/``end_date`` accept ``YYYY-MM-DD`` or ISO datetimes.
    Values that fail to parse are ignored (the filter is best-effort).
    """
    start = normalize_date(start_date)
    end = normalize_date(end_date)
    clauses: list[str] = []
    params: dict[str, datetime] = {}
    if start is not None:
        clauses.append(f" {column} >= %(start_ts)s")
        params["start_ts"] = start
    if end is not None:
        # Inclusive end-of-day for date-only inputs.
        if len(end_date.strip()) == 10 if end_date else False:
            end = end.replace(hour=23, minute=59, second=59)
        clauses.append(f" {column} <= %(end_ts)s")
        params["end_ts"] = end
    if not clauses:
        return "", {}
    return " AND" + " AND".join(clauses), params


def has_more(rows_returned: int, limit: int) -> bool:
    """True when a page is full, suggesting more rows exist."""
    return rows_returned >= limit
