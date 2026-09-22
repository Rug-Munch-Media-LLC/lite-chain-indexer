"""Per-day parquet export mixin (one day of export logic; split from writer)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from lite_indexer.config import CH_DEX_TABLE, CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.storage.parquet.chunking import DayPlan, fetch_day_kinds, write_kind_parts
from lite_indexer.storage.parquet.helpers import _block_range, _local_dir, _local_path, _remote_dir, _state_marker
from lite_indexer.storage.parquet.reader import _day_row_stats, _fetch_day_rows, _max_rows_per_day
from lite_indexer.storage.parquet.schema import _STATUS_DELETED, _STATUS_EXPORTING, _STATUS_FAILED, _STATUS_UPLOADED

logger = get_logger(__name__)

_KIND_TABLES: tuple[tuple[str, str], ...] = (("transfers", CH_TABLE), ("dex", CH_DEX_TABLE))

@dataclass(frozen=True)
class DayExportOptions:
    """Knobs + summary threaded through one day's export."""

    summary: Any
    upload: bool
    delete: bool
    publish_snapshot: bool
    snapshot_bucket: str
    snapshot_path: str

class ExportDayMixin:
    """One-day export: chunked fetch, write+verify, upload, delete, snapshot."""

    def _export_one_day(self, client: Any, chain: str, day: date, opts: DayExportOptions) -> None:
        """Export one (chain, day); all failures recorded on the summary."""
        marker = _state_marker(self.state_dir, chain, day)
        if marker.exists():
            opts.summary.skipped_existing += 1
            return

        try:
            self._record_state(client, chain, day, status=_STATUS_EXPORTING)
            kinds_parts = fetch_day_kinds(
            _fetch_day_rows, _day_row_stats, _KIND_TABLES, DayPlan(chain, day, _max_rows_per_day(), client)
        )
            transfer_rows = [r for _suffix, rows in kinds_parts["transfers"] for r in rows]
            dex_rows = [r for _suffix, rows in kinds_parts["dex"] for r in rows]
        except Exception as exc:
            opts.summary.errors.append(f"{day.isoformat()}: {str(exc)[:100]}")
            self._record_state(client, chain, day, status=_STATUS_FAILED, error_message=str(exc)[:200])
            return

        total_rows = len(transfer_rows) + len(dex_rows)
        if total_rows == 0:
            # No data for this date — archive an empty day. Nothing
            # to write, opts.upload, or opts.delete, so record the terminal
            # state and mark the date done.
            self._record_state(
                client,
                chain,
                day,
                status=_STATUS_DELETED if opts.delete else _STATUS_UPLOADED,
            )
            self._mark_exported(marker, day, 0, 0)
            opts.summary.dates_exported += 1
            logger.info("parquet_day_empty", chain=chain, day=day.isoformat())
            return

        # Write + verify each kind.
        bytes_written = 0
        kinds_ok: dict[str, bool] = {}
        for kind, parts in kinds_parts.items():
            local = _local_path(self.data_dir, chain, day, kind)
            written, kind_ok = write_kind_parts(self._write_parquet, self._verify_parquet, local, parts)
            bytes_written += written
            rows_total = sum(len(rows) for _suffix, rows in parts)
            if kind == "transfers":
                opts.summary.transfers_rows += rows_total
            else:
                opts.summary.dex_rows += rows_total
            kinds_ok[kind] = kind_ok
        opts.summary.bytes_written += bytes_written

        if not all(kinds_ok.values()):
            message = "parquet write/verify failed"
            opts.summary.errors.append(f"{day.isoformat()}: {message}")
            self._record_state(
                client,
                chain,
                day,
                status=_STATUS_FAILED,
                transfers_rows=len(transfer_rows),
                dex_rows=len(dex_rows),
                parquet_bytes=bytes_written,
                error_message=message,
            )
            return

        # Upload the whole chain/date directory, then verify it.
        uploaded_ok = False
        if opts.upload:
            uploaded_ok = self._upload_dir(
                _local_dir(self.data_dir, chain, day),
                _remote_dir(self.remote, chain, day),
            )
            if uploaded_ok:
                opts.summary.uploaded += 1

        if opts.upload and not uploaded_ok:
            message = "opts.upload not verified"
            opts.summary.errors.append(f"{day.isoformat()}: {message}")
            self._record_state(
                client,
                chain,
                day,
                status=_STATUS_FAILED,
                transfers_rows=len(transfer_rows),
                dex_rows=len(dex_rows),
                parquet_bytes=bytes_written,
                error_message=message,
            )
            return

        # Terminal archive state: 'uploaded' (uploaded_at is set when
        # the files are actually on B2, NULL for local-only exports).
        self._record_state(
            client,
            chain,
            day,
            status=_STATUS_UPLOADED,
            transfers_rows=len(transfer_rows),
            dex_rows=len(dex_rows),
            parquet_bytes=bytes_written,
        )

        # Delete rows from ClickHouse ONLY after the opts.upload has been
        # verified — never before (defense-in-depth; the CLI already
        # rejects --opts.delete-after-opts.upload without --opts.upload).
        deleted = 0
        if opts.delete:
            if not opts.upload:
                logger.warning(
                    "parquet_delete_requires_upload",
                    chain=chain,
                    day=day.isoformat(),
                    rows=total_rows,
                )
            elif uploaded_ok:
                deleted += self._delete_day_rows(client, CH_TABLE, chain, day)
                deleted += self._delete_day_rows(client, CH_DEX_TABLE, chain, day)
                opts.summary.deleted_rows += deleted
                self._record_state(
                    client,
                    chain,
                    day,
                    status=_STATUS_DELETED,
                    transfers_rows=len(transfer_rows),
                    dex_rows=len(dex_rows),
                    parquet_bytes=bytes_written,
                )

        self._mark_exported(marker, day, len(transfer_rows), len(dex_rows))
        opts.summary.dates_exported += 1
        logger.info(
            "parquet_day_exported",
            chain=chain,
            day=day.isoformat(),
            rows=total_rows,
            bytes=bytes_written,
            uploaded=uploaded_ok,
            deleted=opts.delete,
        )

        # Optional: mirror this (chain, date) to the bootstrap snapshot
        # location with a manifest so `lite-indexer bootstrap` can
        # discover and replay it. Only for real (non-empty) exports.
        if opts.publish_snapshot and total_rows > 0:
            start_block, end_block = _block_range(transfer_rows, dex_rows)
            if self.publish_snapshot(
                chain,
                day,
                transfers_rows=len(transfer_rows),
                dex_rows=len(dex_rows),
                start_block=start_block,
                end_block=end_block,
                bucket=opts.snapshot_bucket,
                snapshots_path=opts.snapshot_path,
            ):
                opts.summary.uploaded += 1
                logger.info(
                    "parquet_snapshot_published",
                    chain=chain,
                    day=day.isoformat(),
                    block_range=[start_block, end_block],
                )
            else:
                opts.summary.errors.append(f"{day.isoformat()}: snapshot publish failed")
