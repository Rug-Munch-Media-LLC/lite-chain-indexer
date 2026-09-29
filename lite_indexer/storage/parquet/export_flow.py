"""Parquet export flow mixin (date planning + per-day orchestration).

Split out of writer.py — the flow answers "which days, in what order",
the per-day work lives in export_day.py.
"""

from __future__ import annotations

from datetime import date

from lite_indexer.config import BOOTSTRAP_B2_BUCKET, BOOTSTRAP_PARQUET_PATH, PARQUET_COLD_DAYS
from lite_indexer.logging import get_logger
from lite_indexer.storage.parquet.export_day import DayExportOptions
from lite_indexer.storage.parquet.helpers import _state_marker
from lite_indexer.storage.parquet.mixin_api import ParquetApi
from lite_indexer.storage.parquet.schema import (
    _STATUS_PENDING,
    ExportSummary,
)

logger = get_logger(__name__)


class ExportFlowMixin:
    """Date planning + per-day orchestration for parquet exports."""

    def export_chain(
        self: ParquetApi,
        chain: str,
        *,
        dates: list[date] | None = None,
        cold_days: int = PARQUET_COLD_DAYS,
        upload: bool = False,
        delete: bool = False,
        dry_run: bool = False,
        publish_snapshot: bool = False,
        snapshot_bucket: str = BOOTSTRAP_B2_BUCKET,
        snapshot_path: str = BOOTSTRAP_PARQUET_PATH,
    ) -> ExportSummary:
        """Export a chain's rows to Parquet (and B2).

        When ``dates`` is given, export exactly those (chain, date)
        pairs (default: yesterday per the CLI); otherwise fall back to
        ``eligible_dates(chain, cold_days)`` backfill mode.

        Rows are deleted from ClickHouse only after the B2 upload has
        been verified (or when there was nothing to export).

        With ``publish_snapshot``, every exported (chain, date) is also
        mirrored to the bootstrap snapshot location with a generated
        ``manifest.json`` (see :meth:`publish_snapshot`) so
        ``lite-indexer bootstrap`` can discover and replay it.

        Args:
            chain: Chain name.
            dates: Explicit dates to export (overrides cold_days mode).
            cold_days: Min age (days) before a date is exported.
            upload: Upload files to the B2 remote via rclone.
            delete: Delete exported rows from ClickHouse after verified upload.
            dry_run: Preview without writing files, uploading, or deleting.
            publish_snapshot: Mirror the export to the bootstrap snapshot
                location with a manifest (requires a real export).
            snapshot_bucket: rclone remote root for snapshots.
            snapshot_path: Path prefix inside the bucket.

        Returns:
            :class:`ExportSummary` with per-run stats.
        """
        summary = ExportSummary(chain=chain)
        if dates is None:
            dates = self.eligible_dates(chain, cold_days)
        else:
            dates = [d for d in dates if not _state_marker(self.state_dir, chain, d).exists()]
        summary.dates_attempted = len(dates)

        if not dates:
            logger.info("parquet_nothing_to_export", chain=chain)
            return summary

        if dry_run:
            summary.dates_exported = len(dates)
            logger.info(
                "parquet_dry_run",
                chain=chain,
                dates=[d.isoformat() for d in dates],
                upload=upload,
                delete=delete,
            )
            return summary

        client = self._connect()
        if client is None:
            summary.errors.append("clickhouse unavailable")
            return summary

        self._ensure_state_table(client)

        # Schedule every planned date as pending first, then transition
        # each one through exporting → uploaded/deleted/failed below.
        for day in dates:
            self._record_state(client, chain, day, status=_STATUS_PENDING)

        for day in dates:
            opts = DayExportOptions(
                summary=summary,
                upload=upload,
                delete=delete,
                publish_snapshot=publish_snapshot,
                snapshot_bucket=snapshot_bucket,
                snapshot_path=snapshot_path,
            )
            self._export_one_day(client, chain, day, opts)

        return summary
