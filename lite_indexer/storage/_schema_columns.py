"""Core column DDL (source, raw-payload, dedup key) for writer tables.

Extracted from ``storage/_schema.py`` (shrink-only size ratchet). Composed
into :class:`lite_indexer.storage._schema.SchemaMixin` by plain multiple
inheritance — the halves declare no base and never import each other.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.storage._ddl_memo import _ddl_memo

logger = get_logger(__name__)


class _SchemaCoreColumnMixin:

    """Idempotent column/projection/TTL DDL for writer tables."""

    @_ddl_memo
    def _ensure_source_column(self, client: Any, table: str) -> None:
        """Ensure source column exists in ClickHouse table."""
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s AND name = 'source'",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS source String DEFAULT ''"
            )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            logger.info("source_column_added", table=table)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("source_column_skip", error=str(exc)[:80])

    @_ddl_memo
    def _ensure_raw_payload_column(self, client: Any, table: str) -> None:
        """Ensure raw_payload exists so writers can name it in INSERTs.

        Older deployments predate the column, so add it idempotently with
        ``DEFAULT ''`` — historical rows stay valid and every new row
        carries the serialized raw RPC payload when the parser emitted one.
        Mirrors ``_ensure_source_column`` (migrations/010 adds the same
        column to ``wallet_memory.transactions`` for operator provisioning).
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s AND name = 'raw_payload'",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS raw_payload String DEFAULT ''"
            )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            logger.info("raw_payload_column_added", table=table)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("raw_payload_column_skip", error=str(exc)[:80])

    @_ddl_memo
    def _ensure_dedup_key_column(self, client: Any, table: str) -> None:
        """Ensure ``tx_log_key`` exists so writers can name it in INSERTs.

        Added WITHOUT a default expression on purpose: ClickHouse 26.7 forbids
        appending a newly added column that has a default expression to the
        sorting key ("Newly added column ... has a default expression"), so
        migrations/018_evm_transfers_dedup_key_v2.sql must bear the no-default
        form. An older defaulted column would silently break that migration,
        hence no ``DEFAULT ''`` here either. Pre-migration rows read as ``''``.

        Runtime fallback only — the canonical key change is migration 018,
        applied with the indexer stopped. Mirrors ``_ensure_source_column``.
        """
        try:
            result = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(database)s AND table = %(table)s AND name = 'tx_log_key'",
                {
                    "database": table.split(".")[0] if "." in table else "wallet_memory",
                    "table": table.split(".")[-1],
                },
            )
            if result:
                return
            client.execute(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS tx_log_key String"
            )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            logger.info("dedup_key_column_added", table=table)
        except Exception as exc:
            if "already exists" in str(exc) or "privileges" in str(exc).lower():
                return
            logger.debug("dedup_key_column_skip", error=str(exc)[:80])
