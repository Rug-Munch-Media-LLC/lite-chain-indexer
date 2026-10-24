"""ClickHouse access for the ``pm_*`` tables (all ReplacingMergeTree, db wallet_memory).

The connection is the indexer's own :class:`ClickHouseWriter` client, not a new
one: same host/credentials from the environment, same thread-local lifecycle.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Sequence

from lite_indexer.logging import get_logger
from lite_indexer.storage.clickhouse import ClickHouseWriter

logger = get_logger(__name__)

DB = "wallet_memory"


def _client_or_raise(writer: ClickHouseWriter):
    client = writer.client()
    if client is None:
        raise RuntimeError("ClickHouse unavailable (no client)")
    return client


def insert(writer: ClickHouseWriter, table: str, columns: Sequence[str], rows: list[tuple]) -> int:
    """Bulk insert; a repeat of the ORDER BY key is collapsed by the table engine."""
    if not rows:
        return 0
    client = _client_or_raise(writer)
    client.execute(f"INSERT INTO {DB}.{table} ({', '.join(columns)}) VALUES", rows)
    return len(rows)


def latest(writer: ClickHouseWriter, table: str, column: str) -> dt.datetime | None:
    """Max value of a DateTime column, or None when the table is empty."""
    client = _client_or_raise(writer)
    result = client.execute(f"SELECT max({column}) FROM {DB}.{table}")
    value = result[0][0] if result else None
    return value if isinstance(value, dt.datetime) else None


def scalars(writer: ClickHouseWriter, sql: str) -> list[tuple]:
    """Run a small read query (the producer's pre-conditions, e.g. top markets)."""
    client = _client_or_raise(writer)
    return client.execute(sql)
