"""Live column introspection for the CryptoHouse ClickHouse endpoint.

Callers cannot invent columns. Every table this layer touches is described
from ``system.columns`` once and cached, so a renamed/dropped column fails
loudly at the boundary instead of silently changing a result set.

Findings were verified live against ``crypto-clickhouse.clickhouse.com``
(ClickHouse 26.6.1.2191) on 2026-10-04; the account is ``readonly=1``.

Import order must never decide whether the lake read path loads. The federation
modules form a cycle (client -> guard -> schemas -> client), so a module-level
import here made ``import client`` fail with "partially initialized module"
while the tests passed — they happen to import schemas first. query_rows is
therefore imported lazily inside :func:`describe`.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Final

_IDENT: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

#: Tables verified to exceed ``max_rows_to_read`` (10e9). Any query touching one
#: of these MUST carry a time or block bound or it hard-fails server-side.
#: Row counts observed 2026-10-04; these are orders-of-magnitude and only used
#: to decide *which tables need* a bound, never to estimate scan cost.
LARGE_TABLES: Final[frozenset[str]] = frozenset(
    {
        "base.traces",
        "base.logs",
        "base.transactions",
        "base.erc721_transfers",
        "ethereum.traces",
        "ethereum.logs",
        "ethereum.transactions",
        "solana.instructions",
        "solana.transactions",
        "solana.token_transfers",
        "solana.transactions_non_voting",
        "solana.block_rewards",
        "solana.accounts",
    }
)

#: Tables that are small/dimension-like: a bound is still good practice but an
#: accidental full scan will not trip the 10e9 cap.
SMALL_TABLES: Final[frozenset[str]] = frozenset(
    {
        "base.blocks",
        "ethereum.blocks",
        "ethereum.first_time_for_address",
        "solana.blocks",
        "solana.tokens",
        "solana.token_metadata",
        "polymarket.orders_filled",
        "polymarket.orders_matched",
        "polymarket.user_positions",
        "polymarket.user_balances",
        "polymarket.markets",
        "polymarket.token_id_condition",
        "polymarket.assets",
    }
)


def validate_ident(name: str) -> str:
    """Return ``name`` if it is a safe SQL identifier, else raise.

    Table and column names are interpolated into SQL text (ClickHouse's HTTP
    interface has no parameter binding for identifiers), so every caller-
    supplied name passes through here first. This is the only thing standing
    between this layer and an injection, so it is deliberately strict:
    identifiers only, no quoting, no dots, no spaces.
    """
    if not _IDENT.match(name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    """One column as reported by ClickHouse."""

    name: str
    type: str
    default_kind: str
    default_expression: str

    @property
    def is_array(self) -> bool:
        return self.type.startswith("Array(")

    @property
    def base_type(self) -> str:
        """Strip ``Array(...)`` so callers can switch on the scalar type."""
        if self.is_array:
            return self.type[len("Array(") : -1]
        return self.type


@dataclass(frozen=True, slots=True)
class TableInfo:
    """Introspected shape of one table."""

    database: str
    name: str
    engine: str
    columns: tuple[ColumnInfo, ...] = field(repr=False)
    partition_key: str
    sorting_key: str

    @property
    def qualified(self) -> str:
        return f"{self.database}.{self.name}"

    def has(self, column: str) -> bool:
        return any(c.name == column for c in self.columns)

    def require(self, *columns: str) -> None:
        """Raise ``KeyError`` naming every missing column.

        Callers pass the columns they intend to read. Failing here beats
        letting ClickHouse reject the query with an opaque message.
        """
        missing = [c for c in columns if not self.has(c)]
        if missing:
            raise KeyError(f"{self.qualified} has no column(s) {missing}")

    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def is_replacing(self) -> bool:
        return "ReplacingMergeTree" in self.engine


_cache: dict[str, TableInfo] = {}
_lock = threading.Lock()


def describe(table: str, *, refresh: bool = False) -> TableInfo:
    """Introspect ``db.table`` (dotted) and cache the result.

    ``base.blocks`` style short names are rejected on purpose: this fleet's
    whole bug class is dotted-vs-underscored table names resolving to nothing.
    """
    from lite_indexer.query.federation.client import query_rows  # noqa: PLC0415  # lazy: breaks the import cycle

    if "." not in table:
        raise ValueError(f"table must be dotted db.table, got {table!r}")
    if not refresh and (cached := _cache.get(table)) is not None:
        return cached

    database, name = table.split(".", 1)
    validate_ident(database)
    validate_ident(name)

    rows = query_rows(
        "SELECT name, type, default_kind, default_expression "
        "FROM system.columns WHERE database = {db:String} AND table = {tbl:String} "
        "ORDER BY position",
        {"db": database, "tbl": name},
    )
    if not rows:
        raise KeyError(
            f"table {table!r} does not exist on this endpoint — introspect system.tables; do not guess a name"
        )

    meta = query_rows(
        "SELECT engine, partition_key, sorting_key FROM system.tables "
        "WHERE database = {db:String} AND name = {tbl:String}",
        {"db": database, "tbl": name},
    )
    engine, partition_key, sorting_key = ("", "", "")
    if meta:
        engine = str(meta[0].get("engine") or "")
        partition_key = str(meta[0].get("partition_key") or "")
        sorting_key = str(meta[0].get("sorting_key") or "")

    info = TableInfo(
        database=database,
        name=name,
        engine=engine,
        columns=tuple(
            ColumnInfo(
                name=str(r["name"]),
                type=str(r["type"]),
                default_kind=str(r.get("default_kind") or ""),
                default_expression=str(r.get("default_expression") or ""),
            )
            for r in rows
        ),
        partition_key=partition_key,
        sorting_key=sorting_key,
    )
    with _lock:
        _cache[table] = info
    return info


def columns(table: str) -> list[str]:
    """Real column names for ``table`` (cached)."""
    return describe(table).column_names()


def has_column(table: str, column: str) -> bool:
    """Whether ``column`` exists on ``table`` (cached, does not raise)."""
    return column in columns(table)


def require_columns(table: str, *cols: str) -> None:
    """Assert every column in ``cols`` exists on ``table``."""
    describe(table).require(*cols)


def cache_clear() -> None:
    """Drop the introspection cache (tests, and after a schema refresh)."""
    with _lock:
        _cache.clear()
