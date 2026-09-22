"""Parquet export state mixin (state table, markers, row deletion).

Split out of writer.py — the export flow and the state bookkeeping
change for different reasons; composed into ParquetExporter.
"""
from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from lite_indexer import metrics
from lite_indexer.config import CH_PARQUET_STATE_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.storage.parquet.schema import (
    _STATE_TABLE_DDL,
    _STATUS_DELETED,
    _STATUS_FAILED,
    _STATUS_UPLOADED,
)

logger = get_logger(__name__)


class ExportStateMixin:
    """Export-state bookkeeping (pending/exporting/uploaded/deleted)."""

    def _ensure_state_table(self, client: Any) -> None:
        """Create the parquet_export_state table if it does not exist."""
        try:
            client.execute(_STATE_TABLE_DDL)
        except Exception as exc:
            logger.debug("parquet_state_table_ensure_failed", error=str(exc)[:100])


    def _record_state(
        self,
        client: Any,
        chain: str,
        day: date,
        *,
        status: str,
        transfers_rows: int = 0,
        dex_rows: int = 0,
        parquet_bytes: int = 0,
        error_message: str = "",
    ) -> None:
        """Upsert a (chain, date) row into parquet_export_state.

        ReplacingMergeTree keeps the last-inserted row per (chain, date),
        so a fresh INSERT for the latest status is an upsert.

        Args:
            client: ClickHouse client.
            chain: Chain name.
            day: Export date.
            status: One of pending/exporting/uploaded/deleted/failed.
            transfers_rows: Rows exported from the transfers table.
            dex_rows: Rows exported from the DEX table.
            parquet_bytes: Bytes written to Parquet.
            error_message: Error detail (failed status only).
        """
        now = datetime.now(UTC)
        if status == _STATUS_FAILED:
            metrics.incr_labeled("lite_indexer_parquet_export_failed", chain=chain, date=day.isoformat())
        uploaded_at = now if status == _STATUS_UPLOADED else None
        deleted_at = now if status == _STATUS_DELETED else None
        try:
            client.execute(
                f"INSERT INTO {CH_PARQUET_STATE_TABLE} "
                "(chain, date, status, evm_transfers_rows, dex_events_rows, "
                "parquet_bytes, uploaded_at, deleted_at, error_message) VALUES",
                [
                    (
                        chain,
                        day,
                        status,
                        transfers_rows,
                        dex_rows,
                        parquet_bytes,
                        uploaded_at,
                        deleted_at,
                        error_message,
                    )
                ],
            )
        except Exception as exc:
            logger.warning("parquet_state_record_failed", chain=chain, day=day.isoformat(), error=str(exc)[:100])


    def _delete_day_rows(self, client: Any, table: str, chain: str, day: date) -> int:
        """Delete a day's rows from a ClickHouse table (lightweight delete).

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            chain: Chain name.
            day: Export date.

        Returns:
            Number of mutated tables (0 when the mutation fails).
        """
        try:
            client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                f"ALTER TABLE {table} DELETE WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s",
                {"chain": chain, "day": day.isoformat()},
            )
            logger.info("parquet_deleted_rows", table=table, chain=chain, day=day.isoformat())
            return 1
        except Exception as exc:
            logger.warning("parquet_delete_failed", table=table, chain=chain, error=str(exc)[:120])
            return 0


    def _mark_exported(
        self,
        marker: Path,
        day: date,
        transfers_rows: int,
        dex_rows: int,
    ) -> None:
        """Write the state marker recording an exported (chain, date).

        Args:
            marker: Marker path.
            day: Export date.
            transfers_rows: Rows exported from the transfers table.
            dex_rows: Rows exported from the DEX table.
        """
        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(
                json.dumps(
                    {
                        "day": day.isoformat(),
                        "transfers_rows": transfers_rows,
                        "dex_rows": dex_rows,
                        "exported_at": datetime.now(UTC).isoformat(),
                    },
                    indent=2,
                )
            )
        except OSError as exc:
            logger.warning("parquet_marker_failed", path=str(marker), error=str(exc)[:80])

