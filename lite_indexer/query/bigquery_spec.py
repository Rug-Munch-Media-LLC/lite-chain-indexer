"""Typed query spec shared by the BigQuery source and its table builders."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BigQuerySpec:
    """One BigQuery read: table plus optional range/date/filter narrowing."""

    table: str
    chain: str = "eth"
    address: str | None = None
    from_block: int | None = None
    to_block: int | None = None
    dates: list[str] | None = None
    limit: int = 100
    mints: list[str] | None = None
    offset: int | None = None
