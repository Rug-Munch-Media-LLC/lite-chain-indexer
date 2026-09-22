"""Typed request object shared by every QueryRouter leg.

Grouping the per-leg parameters keeps each leg signature at two arguments
(router + request) and gives one place to own the cache-key contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LegRequest:
    """One on-demand query routed across the hot/cold/lake/portal/RPC legs."""

    table: str
    chain: str
    from_block: int | None = None
    to_block: int | None = None
    filters: dict[str, Any] | list[dict[str, Any]] | None = None
    limit: int = 100

    def cache_key(self) -> tuple[Any, ...]:
        """Stable cache key for a range query (filters normalized to tuples)."""
        filters = self.filters
        if isinstance(filters, dict):
            fkey: Any = tuple(sorted(filters.items()))
        elif isinstance(filters, list):
            fkey = tuple(tuple(sorted(item.items())) for item in filters)
        else:
            fkey = ()
        return (self.chain, self.table, self.from_block, self.to_block, fkey, int(self.limit))
