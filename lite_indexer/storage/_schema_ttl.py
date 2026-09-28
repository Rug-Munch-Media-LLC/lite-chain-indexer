"""Table TTL, column-TTL and projection DDL for ClickHouse writer tables.

Extracted from ``storage/_schema.py`` (shrink-only size ratchet). Composed
into :class:`lite_indexer.storage._schema.SchemaMixin` by plain multiple
inheritance — the halves declare no base and never import each other.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    CH_DEX_TABLE,
    CH_TABLE,
    DEX_SUMMARY_COLUMNS,
    EVT_SUMMARY_COLUMNS,
    RETENTION_DAYS,
)
from lite_indexer.logging import get_logger
from lite_indexer.retention import ensure_retention_policies
from lite_indexer.storage._ddl_memo import _TTL_ENSURED_AT, _TTL_REENSURE_SECONDS, _ddl_memo

logger = get_logger(__name__)


class _SchemaTtlMixin:
    # Cross-half contract: these live in the sibling mixins composed into
    # ``SchemaMixin`` (_schema_columns / _schema_derived_columns). Declared here
    # so each half type-checks standalone and the coupling is explicit.
    def _column_types(self, client: Any, table: str) -> dict[str, str]:
        raise NotImplementedError

    def _degrade_column_ttl(self, client: Any, table: str, column: str, days: int) -> None:
        raise NotImplementedError

    @_ddl_memo
    def _ensure_summary_projection(self, client: Any, table: str, summary_columns: tuple[str, ...]) -> None:
        """Add a summary projection over the columns scanners actually use.

        The projection materializes a compact, well-ordered copy of the
        summary columns so aggregation queries can skip the heavy detail
        columns entirely.

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            summary_columns: Columns to include in the projection.
        """
        if not summary_columns:
            return
        try:
            self._column_types(client, table)  # ensure table/column introspection works
            projection_name = f"{table.split('.')[-1]}_summary"
            select = ", ".join(summary_columns)
            client.execute(
                f"ALTER TABLE {table} "
                f"ADD PROJECTION IF NOT EXISTS {projection_name} "
                f"(SELECT {select} ORDER BY block_timestamp)"
            )
            logger.info("summary_projection_ensured", table=table, projection=projection_name)
        except Exception as exc:
            if "privileges" in str(exc).lower():
                return
            logger.debug("summary_projection_skip", table=table, error=str(exc)[:80])

    def ensure_ttl_policies(self, client: Any) -> None:
        """Apply retention + column-degradation policies to CH tables.

        Idempotent and non-fatal: every DDL is wrapped so a missing
        table, column, or lacking privileges degrades to a debug log.
        Called on first write of each cycle when ``CH_TTL_ENABLED``.

        Memoised for ``_TTL_REENSURE_SECONDS`` per process: this issues ~20
        DDL statements, and running it per insert batch is what caused the
        2026-09-23 I/O storm (see _TTL_ENSURED_AT above).

        Args:
            client: ClickHouse client.
        """
        import time

        key = "schema"
        now = time.monotonic()
        if now - _TTL_ENSURED_AT.get(key, 0.0) < _TTL_REENSURE_SECONDS:
            return
        # evm_transfers: full precision for 30 days, then degrade
        # token_symbol/token_name (heavy String columns) to defaults.
        days = RETENTION_DAYS.get("evm_transfers", 30)
        for column in ("token_symbol", "token_name"):
            self._degrade_column_ttl(client, CH_TABLE, column, days)
        self._ensure_summary_projection(client, CH_TABLE, EVT_SUMMARY_COLUMNS)

        # dex_events: keep swap details for 90 days, then degrade the
        # detail columns (sender/recipient/amounts) keeping identity.
        dex_days = RETENTION_DAYS.get("dex_events", 90)
        for column in (
            "sender",
            "recipient",
            "amount0_in",
            "amount1_in",
            "amount0_out",
            "amount1_out",
            "amount0",
            "amount1",
            "whale",
            "ingested_at",
            "source",
        ):
            self._degrade_column_ttl(client, CH_DEX_TABLE, column, dex_days)
        self._ensure_summary_projection(client, CH_DEX_TABLE, DEX_SUMMARY_COLUMNS)

        # Table-level hot-window TTLs for every retention class (single
        # source: lite_indexer.config.RETENTION_POLICY). Mirrors migration
        # 013 so a fresh deployment gets the same TTLs the migration applies.
        applied = ensure_retention_policies(client)
        if applied:
            logger.info("retention_ttls_ensured", tables=len(applied))
        _TTL_ENSURED_AT[key] = now
