"""Archive table spec: cursor semantics and TTL exposure for one table."""

from __future__ import annotations

from dataclasses import dataclass

CURSOR = "cursor"
SNAPSHOT = "snapshot"


@dataclass(frozen=True)
class TableSpec:
    """One archivable table: cursor semantics + TTL exposure."""

    table: str
    kind: str
    cursor: str
    mode: str
    chain_column: str | None
    ttl_column: str
    ttl_days: int
    columns: tuple[str, ...]
    dataset: str = ""
    chain_literal: str = ""

    @property
    def date_column(self) -> str:
        """Column the unit date is derived from."""
        return self.cursor
