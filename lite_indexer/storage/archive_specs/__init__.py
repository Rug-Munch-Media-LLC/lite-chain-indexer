"""Declarative archive contract: which tables ship, and how their cursor works.

The B2 parquet archive is permanent; ClickHouse is a rolling window. A table
that is not in :data:`ARCHIVE_TABLES` has **no export path at all** and will be
eaten by its own TTL — exactly how ``evm_transfers`` lost its history. This
module is the single source of truth for that decision, so the omission is a
data error rather than a silent gap.

Two modes:

``cursor``
    The table has a monotonic column (``ingested_at`` or a chain timestamp).
    One unit = (chain, table, UTC date of the cursor). Rows are append-only,
    so a unit is final once the date is closed and re-exports are idempotent.

``snapshot``
    No monotonic column exists. There is no correct "date" to cut on, so the
    unit is a full-table image per run, dated by the export day. Snapshot
    tables are never deleted from and never claimed to be complete for a date.
"""

from __future__ import annotations

from lite_indexer.storage.archive_specs._registry_evm import EVM_SPECS
from lite_indexer.storage.archive_specs._registry_multichain import MULTICHAIN_SPECS
from lite_indexer.storage.archive_specs._registry_polymarket import POLYMARKET_SPECS
from lite_indexer.storage.archive_specs._registry_scanner import SCANNER_SPECS
from lite_indexer.storage.archive_specs._spec import CURSOR, SNAPSHOT, TableSpec

ARCHIVE_TABLES: dict[str, TableSpec] = {
    **EVM_SPECS,
    **MULTICHAIN_SPECS,
    **POLYMARKET_SPECS,
    **SCANNER_SPECS,
}

# The tables this task proved were unexported (325 GiB, no path at all).
GAP_TABLES: tuple[str, ...] = (
    "address_activity",
    "solana_spl_balance_changes",
    "internal_transfers",
)

TABLE_KINDS: dict[str, str] = {name: spec.kind for name, spec in ARCHIVE_TABLES.items()}


def gaps_outside_registry(clickhouse_tables: set[str]) -> list[str]:
    """Return data tables with no archive spec — the silent-loss set."""
    return sorted(t for t in clickhouse_tables if t not in ARCHIVE_TABLES)


def validate() -> list[str]:
    """Static self-check: every spec must declare a coherent cursor/mode."""
    problems: list[str] = []
    for name, spec in ARCHIVE_TABLES.items():
        if spec.mode not in (CURSOR, SNAPSHOT):
            problems.append(f"{name}: unknown mode {spec.mode!r}")
        if spec.mode == CURSOR and not spec.cursor:
            problems.append(f"{name}: cursor mode without a cursor column")
        if spec.mode == SNAPSHOT and spec.cursor:
            problems.append(f"{name}: snapshot mode must not declare a cursor")
        if not spec.columns:
            problems.append(f"{name}: no exported columns")
        if spec.cursor and spec.cursor not in spec.columns:
            problems.append(f"{name}: cursor {spec.cursor!r} missing from columns")
    return problems


__all__ = [
    "ARCHIVE_TABLES",
    "CURSOR",
    "GAP_TABLES",
    "SNAPSHOT",
    "TableSpec",
    "gaps_outside_registry",
    "validate",
]
