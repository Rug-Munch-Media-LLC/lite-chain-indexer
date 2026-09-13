"""Parquet write/upload/export orchestration (ParquetExporter)."""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from lite_indexer import metrics
from lite_indexer.config import (
    BOOTSTRAP_B2_BUCKET,
    BOOTSTRAP_PARQUET_PATH,
    CH_DEX_TABLE,
    CH_PARQUET_STATE_TABLE,
    CH_TABLE,
    PARQUET_COLD_DAYS,
    PARQUET_DIR,
    PARQUET_RCLONE_BIN,
    PARQUET_REMOTE,
    PARQUET_STATE_DIR,
)
from lite_indexer.logging import get_logger
from lite_indexer.snapshot_manifest import MANIFEST_NAME, build_manifest, manifest_json
from lite_indexer.storage.parquet.helpers import _block_range, _local_dir, _local_path, _remote_dir, _state_marker
from lite_indexer.storage.parquet.reader import _fetch_day_rows, _verify_parquet
from lite_indexer.storage.parquet.schema import (
    _STATE_TABLE_DDL,
    _STATUS_DELETED,
    _STATUS_EXPORTING,
    _STATUS_FAILED,
    _STATUS_PENDING,
    _STATUS_UPLOADED,
    PYARROW_AVAILABLE,
    ExportSummary,
    pa,
    pq,
)

logger = get_logger(__name__)


class ParquetExporter:
    """Export old ClickHouse rows to Parquet and upload them to B2.

    Safe defaults: ``upload=False``/``delete=False`` until explicitly
    enabled (CLI flags), and ``dry_run=True`` to preview without
    touching anything. Deletion is only attempted after the B2 upload
    has been verified.
    """

    def __init__(
        self,
        data_dir: Path = PARQUET_DIR,
        remote: str = PARQUET_REMOTE,
        rclone_bin: str = PARQUET_RCLONE_BIN,
        state_dir: Path = PARQUET_STATE_DIR,
    ) -> None:
        self.data_dir = data_dir
        self.remote = remote
        self.rclone_bin = rclone_bin
        self.state_dir = state_dir

    # ── ClickHouse helpers ──────────────────────────────────────────

    def _connect(self) -> Any | None:
        """Lazily connect to ClickHouse (None when unavailable)."""
        from lite_indexer.storage.clickhouse import ClickHouseWriter

        writer = ClickHouseWriter()
        return writer._connect()  # noqa: SLF001 — reuse the writer's lazy client

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

    def _fetch_day_rows(self, client: Any, table: str, chain: str, day: date) -> list[dict[str, Any]]:
        """Fetch all rows for a chain/date (delegates to reader)."""
        return _fetch_day_rows(client, table, chain, day)

    @staticmethod
    def _delete_day_rows(client: Any, table: str, chain: str, day: date) -> int:
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

    # ── Parquet writing & verification ──────────────────────────────

    def _write_parquet(
        self,
        rows: list[dict[str, Any]],
        path: Path,
    ) -> int:
        """Write rows to a Parquet file; returns bytes written (0 on error).

        Falls back to gzipped JSONL when pyarrow is unavailable so the
        cold tier still works (DuckDB reads both).

        Args:
            rows: Rows as dicts (must be non-empty).
            path: Destination parquet path.

        Returns:
            Bytes written.
        """
        if not rows:
            return 0
        path.parent.mkdir(parents=True, exist_ok=True)

        if PYARROW_AVAILABLE:
            try:
                table = pa.Table.from_pylist(rows)
                pq.write_table(table, path)  # type: ignore[no-untyped-call]
                return path.stat().st_size
            except Exception as exc:
                logger.warning("parquet_write_failed", path=str(path), error=str(exc)[:120])
                return 0

        # Fallback: gzipped JSONL (queryable via DuckDB read_json_auto).
        import gzip

        try:
            with gzip.open(str(path) + ".jsonl.gz", "wt", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, default=str) + "\n")
            return Path(str(path) + ".jsonl.gz").stat().st_size
        except OSError as exc:
            logger.error("parquet_fallback_write_failed", path=str(path), error=str(exc))
            return 0

    _verify_parquet = staticmethod(_verify_parquet)

    # ── Upload (rclone) ─────────────────────────────────────────────

    def _upload_dir(self, local: Path, remote: str) -> bool:
        """Upload a chain/date directory to B2 and verify it.

        Runs ``rclone copy {local}/ {remote}/`` then ``rclone check``
        (size + modtime) to confirm every file landed. Returns True only
        when the upload verifies — this is what gates the ClickHouse
        deletion.

        Args:
            local: Local directory containing the parquet files.
            remote: Remote directory (``b2-crypt:.../{chain}/{date}``).

        Returns:
            True when the upload is verified.
        """
        if not self.rclone_bin or shutil.which(self.rclone_bin) is None:
            logger.warning("parquet_rclone_missing", bin=self.rclone_bin)
            return False
        try:
            copy = subprocess.run(
                [self.rclone_bin, "copy", str(local), remote],
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
            if copy.returncode != 0:
                logger.warning("parquet_upload_failed", remote=remote, error=copy.stderr[-300:])
                return False
            verify = subprocess.run(
                [self.rclone_bin, "check", str(local), remote],
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
            if verify.returncode != 0:
                logger.warning("parquet_upload_verify_failed", remote=remote, error=verify.stderr[-300:])
                return False
            logger.info("parquet_upload_verified", local=str(local), remote=remote)
            return True
        except (subprocess.SubprocessError, OSError) as exc:
            logger.warning("parquet_upload_error", remote=remote, error=str(exc)[:120])
            return False

    # ── Export orchestration ────────────────────────────────────────

    def eligible_dates(self, chain: str, cold_days: int) -> list[date]:
        """Return the dates eligible for cold storage (old rows only).

        Excludes dates already exported (state markers).

        Args:
            chain: Chain name.
            cold_days: Minimum age in days before a date is eligible.

        Returns:
            Chronologically-ordered list of exportable dates.
        """
        cutoff = datetime.now(UTC).date() - timedelta(days=cold_days)
        dates: list[date] = []
        for offset in range(cold_days, cold_days + 366):
            day = datetime.now(UTC).date() - timedelta(days=offset)
            if day > cutoff:
                continue
            marker = _state_marker(self.state_dir, chain, day)
            if marker.exists():
                continue
            dates.append(day)
        return dates

    def export_chain(
        self,
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
            marker = _state_marker(self.state_dir, chain, day)
            if marker.exists():
                summary.skipped_existing += 1
                continue

            try:
                self._record_state(client, chain, day, status=_STATUS_EXPORTING)
                transfer_rows = self._fetch_day_rows(client, CH_TABLE, chain, day)
                dex_rows = self._fetch_day_rows(client, CH_DEX_TABLE, chain, day)
            except Exception as exc:
                summary.errors.append(f"{day.isoformat()}: {str(exc)[:100]}")
                self._record_state(client, chain, day, status=_STATUS_FAILED, error_message=str(exc)[:200])
                continue

            total_rows = len(transfer_rows) + len(dex_rows)
            if total_rows == 0:
                # No data for this date — archive an empty day. Nothing
                # to write, upload, or delete, so record the terminal
                # state and mark the date done.
                self._record_state(
                    client,
                    chain,
                    day,
                    status=_STATUS_DELETED if delete else _STATUS_UPLOADED,
                )
                self._mark_exported(marker, day, 0, 0)
                summary.dates_exported += 1
                logger.info("parquet_day_empty", chain=chain, day=day.isoformat())
                continue

            # Write + verify each kind.
            bytes_written = 0
            kinds_ok: dict[str, bool] = {}
            for kind, rows in (("transfers", transfer_rows), ("dex", dex_rows)):
                if not rows:
                    kinds_ok[kind] = True  # nothing to write for this kind
                    continue
                local = _local_path(self.data_dir, chain, day, kind)
                written = self._write_parquet(rows, local)
                bytes_written += written
                summary.transfers_rows += len(rows) if kind == "transfers" else 0
                summary.dex_rows += len(rows) if kind == "dex" else 0
                if written and self._verify_parquet(local, len(rows), set(rows[0].keys())):
                    kinds_ok[kind] = True
                else:
                    kinds_ok[kind] = False
            summary.bytes_written += bytes_written

            if not all(kinds_ok.values()):
                message = "parquet write/verify failed"
                summary.errors.append(f"{day.isoformat()}: {message}")
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
                continue

            # Upload the whole chain/date directory, then verify it.
            uploaded_ok = False
            if upload:
                uploaded_ok = self._upload_dir(
                    _local_dir(self.data_dir, chain, day),
                    _remote_dir(self.remote, chain, day),
                )
                if uploaded_ok:
                    summary.uploaded += 1

            if upload and not uploaded_ok:
                message = "upload not verified"
                summary.errors.append(f"{day.isoformat()}: {message}")
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
                continue

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

            # Delete rows from ClickHouse ONLY after the upload has been
            # verified — never before (defense-in-depth; the CLI already
            # rejects --delete-after-upload without --upload).
            deleted = 0
            if delete:
                if not upload:
                    logger.warning(
                        "parquet_delete_requires_upload",
                        chain=chain,
                        day=day.isoformat(),
                        rows=total_rows,
                    )
                elif uploaded_ok:
                    deleted += self._delete_day_rows(client, CH_TABLE, chain, day)
                    deleted += self._delete_day_rows(client, CH_DEX_TABLE, chain, day)
                    summary.deleted_rows += deleted
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
            summary.dates_exported += 1
            logger.info(
                "parquet_day_exported",
                chain=chain,
                day=day.isoformat(),
                rows=total_rows,
                bytes=bytes_written,
                uploaded=uploaded_ok,
                deleted=delete,
            )

            # Optional: mirror this (chain, date) to the bootstrap snapshot
            # location with a manifest so `lite-indexer bootstrap` can
            # discover and replay it. Only for real (non-empty) exports.
            if publish_snapshot and total_rows > 0:
                start_block, end_block = _block_range(transfer_rows, dex_rows)
                if self.publish_snapshot(
                    chain,
                    day,
                    transfers_rows=len(transfer_rows),
                    dex_rows=len(dex_rows),
                    start_block=start_block,
                    end_block=end_block,
                    bucket=snapshot_bucket,
                    snapshots_path=snapshot_path,
                ):
                    summary.uploaded += 1
                    logger.info(
                        "parquet_snapshot_published",
                        chain=chain,
                        day=day.isoformat(),
                        block_range=[start_block, end_block],
                    )
                else:
                    summary.errors.append(f"{day.isoformat()}: snapshot publish failed")

        return summary

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

    # ── Snapshot publish (bootstrap discovery) ─────────────────────
    # With ``publish_snapshot=True`` each exported (chain, date) is
    # mirrored to ``{BOOTSTRAP_B2_BUCKET}/{BOOTSTRAP_PARQUET_PATH}/{date}/
    # {chain}/`` with a generated ``manifest.json``. The manifest is what
    # ``lite-indexer bootstrap`` lists and replays — it turns the daily
    # cold-storage export into a public bootstrap snapshot.

    def publish_snapshot(
        self,
        chain: str,
        day: date,
        *,
        transfers_rows: int = 0,
        dex_rows: int = 0,
        start_block: int = 0,
        end_block: int = 0,
        bucket: str = BOOTSTRAP_B2_BUCKET,
        snapshots_path: str = BOOTSTRAP_PARQUET_PATH,
    ) -> bool:
        """Mirror an exported (chain, date) to the bootstrap snapshot location.

        Writes ``manifest.json`` into the local export directory and
        uploads the whole directory to
        ``{bucket}/{snapshots_path}/{date}/{chain}/`` (rclone copy +
        rclone check). Safe to call only after the export files exist.

        Args:
            chain: Chain name.
            day: Export date.
            transfers_rows: Rows exported from the transfers table.
            dex_rows: Rows exported from the DEX table.
            start_block: First block in the snapshot (0 = unknown).
            end_block: Last block in the snapshot (0 = unknown).
            bucket: rclone remote root for snapshots.
            snapshots_path: Path prefix inside the bucket.

        Returns:
            True when the snapshot (manifest + parquet) was uploaded and
            verified on B2.
        """
        local = _local_dir(self.data_dir, chain, day)
        if not local.exists():
            logger.warning("snapshot_publish_missing_dir", chain=chain, day=day.isoformat())
            return False

        tables: dict[str, dict[str, Any]] = {}
        for kind, table_name in (("transfers", "evm_transfers"), ("dex", "dex_events")):
            path = _local_path(self.data_dir, chain, day, kind)
            if not path.exists():
                continue
            tables[table_name] = {
                "rows": transfers_rows if kind == "transfers" else dex_rows,
                "size_bytes": path.stat().st_size,
                "parquet_files": [path.name],
            }
        if not tables:
            logger.warning("snapshot_publish_no_files", chain=chain, day=day.isoformat())
            return False

        manifest = build_manifest(
            chain,
            day.isoformat(),
            start_block,
            end_block,
            tables,
        )
        manifest_path = local / MANIFEST_NAME
        try:
            manifest_path.write_text(manifest_json(manifest))
        except OSError as exc:
            logger.warning("snapshot_manifest_write_failed", path=str(manifest_path), error=str(exc)[:100])
            return False

        remote = manifest.snapshot_dir(bucket, snapshots_path)
        ok = self._upload_dir(local, remote)
        if ok:
            logger.info(
                "snapshot_published",
                chain=chain,
                date=day.isoformat(),
                remote=remote,
                rows=manifest.total_rows(),
            )
        else:
            logger.warning("snapshot_publish_failed", chain=chain, date=day.isoformat(), remote=remote)
        return ok


__all__ = ["ParquetExporter"]
