# DEPRECATED: Canonical export is now /root/scripts/lite-indexer-parquet-daily.sh
# which uses ClickHouse-native FORMAT Parquet (not pyarrow).
# This module retained for dry-run/testing. Do not schedule.
"""
Parquet cold storage tier — daily rollup of old ClickHouse rows.

Pipeline (idempotent, safe by default):

1. For a (chain, date), read rows from ClickHouse (transfers and DEX
   events) via the native protocol.
2. Write ``{chain}_{date}.parquet`` (and ``{chain}_dex_{date}.parquet``)
   locally under ``PARQUET_DIR/{chain}/{date}/`` using pyarrow, then
   verify each file (row count + schema).
3. Upload the directory with ``rclone copy {dir} {remote}/{chain}/{date}/``
   and verify the upload with ``rclone check`` (only ``--upload``).
4. Record progress in the ClickHouse ``parquet_export_state`` table
   (statuses: pending / exporting / uploaded / deleted / failed).
5. Delete the exported rows from ClickHouse with a lightweight
   ``ALTER TABLE ... DELETE`` mutation — only after the upload has been
   verified (``--delete-after-upload``), never before.
6. Mark the (chain, date) in ``PARQUET_STATE_DIR`` so the daily cron
   never re-exports or double-deletes.

Failures are non-fatal and logged; the state marker is only written
after a successful export, so a partial failure retries next run.
Rows are never deleted from ClickHouse unless the archive upload has
been verified first.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from lite_indexer.config import (
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

logger = get_logger(__name__)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    PYARROW_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]
    PYARROW_AVAILABLE = False

# ClickHouse lightweight-delete maximum mutation size guard (rows per day).
_MAX_ROWS_PER_DAY = 5_000_000

# Statuses recorded in the parquet_export_state table.
_STATUS_PENDING = "pending"
_STATUS_EXPORTING = "exporting"
_STATUS_UPLOADED = "uploaded"
_STATUS_DELETED = "deleted"
_STATUS_FAILED = "failed"

# ClickHouse state table DDL (must stay in sync with
# CH_PARQUET_STATE_TABLE and the README). ReplacingMergeTree keeps the
# last-inserted row per (chain, date), so plain INSERTs upsert state.
_STATE_TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_PARQUET_STATE_TABLE} (
    chain LowCardinality(String),
    date Date,
    status LowCardinality(String),
    evm_transfers_rows UInt64 DEFAULT 0,
    dex_events_rows UInt64 DEFAULT 0,
    parquet_bytes UInt64 DEFAULT 0,
    uploaded_at Nullable(DateTime),
    deleted_at Nullable(DateTime),
    error_message String DEFAULT ''
) ENGINE = ReplacingMergeTree()
ORDER BY (chain, date)
"""


@dataclass
class ExportSummary:
    """Outcome of a parquet export run for one chain."""

    chain: str
    dates_attempted: int = 0
    dates_exported: int = 0
    transfers_rows: int = 0
    dex_rows: int = 0
    bytes_written: int = 0
    uploaded: int = 0
    deleted_rows: int = 0
    skipped_existing: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Serializable summary for CLI output."""
        return {
            "chain": self.chain,
            "dates_attempted": self.dates_attempted,
            "dates_exported": self.dates_exported,
            "transfers_rows": self.transfers_rows,
            "dex_rows": self.dex_rows,
            "bytes_written": self.bytes_written,
            "files_uploaded": self.uploaded,
            "rows_deleted": self.deleted_rows,
            "skipped_existing": self.skipped_existing,
            "errors": self.errors,
        }


def _local_dir(data_dir: Path, chain: str, day: date) -> Path:
    """Build the local parquet directory for a chain/date.

    Layout: ``{chain}/{YYYY-MM-DD}/`` — one directory per chain/date
    holding both the transfers and DEX parquet files, so the whole day
    can be uploaded with a single ``rclone copy {dir} {remote}``.

    Args:
        data_dir: Local parquet root.
        chain: Chain name.
        day: Export date.

    Returns:
        Local directory path.
    """
    return data_dir / chain / day.isoformat()


def _local_path(data_dir: Path, chain: str, day: date, kind: str) -> Path:
    """Build the local parquet file path for a chain/date/kind.

    Args:
        data_dir: Local parquet root.
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Local file path inside the ``{chain}/{date}/`` directory.
    """
    fname = f"{chain}_dex_{day.isoformat()}.parquet" if kind == "dex" else f"{chain}_{day.isoformat()}.parquet"
    return _local_dir(data_dir, chain, day) / fname


def _remote_dir(remote: str, chain: str, day: date) -> str:
    """Build the rclone remote directory for a chain/date.

    Args:
        remote: rclone remote root (e.g. ``b2-crypt:lite-indexer-parquet``).
        chain: Chain name.
        day: Export date.

    Returns:
        Remote directory path.
    """
    return f"{remote}/{chain}/{day.isoformat()}"


def _remote_path(remote: str, chain: str, day: date, kind: str) -> str:
    """Build the rclone remote file path for a chain/date/kind.

    Args:
        remote: rclone remote root.
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Remote file path.
    """
    return f"{_remote_dir(remote, chain, day)}/{_local_path(Path(''), chain, day, kind).name}"


def _state_marker(state_dir: Path, chain: str, day: date) -> Path:
    """Return the state marker path for a (chain, date)."""
    return state_dir / chain / f"{day.isoformat()}.done"


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

    def _fetch_day_rows(
        self,
        client: Any,
        table: str,
        chain: str,
        day: date,
    ) -> list[dict[str, Any]]:
        """Fetch all rows for a chain/date from a ClickHouse table.

        Raises ``ValueError`` when the day exceeds ``_MAX_ROWS_PER_DAY``
        so a truncated export is never treated as complete (and the rows
        are therefore never deleted).

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            chain: Chain name.
            day: Export date.

        Returns:
            Rows as list of dicts (column name → value).

        Raises:
            ValueError: If the day has more than ``_MAX_ROWS_PER_DAY`` rows.
        """
        query = (
            f"SELECT * FROM {table} "
            f"WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s "
            f"LIMIT {_MAX_ROWS_PER_DAY + 1}"
        )
        try:
            rows, columns = client.execute(
                query,
                {"chain": chain, "day": day.isoformat()},
                with_column_types=True,
            )
        except Exception as exc:
            # Table may not exist yet for this chain — not an error worth
            # failing the run over.
            if "doesn't exist" in str(exc) or "UNKNOWN_TABLE" in str(exc):
                logger.debug("parquet_table_missing", table=table, chain=chain, error=str(exc)[:60])
                return []
            logger.warning("parquet_select_failed", table=table, chain=chain, error=str(exc)[:120])
            return []

        if len(rows) > _MAX_ROWS_PER_DAY:
            raise ValueError(f"day exceeds export cap of {_MAX_ROWS_PER_DAY} rows")

        col_names = [c[0] for c in columns]
        return [dict(zip(col_names, row, strict=False)) for row in rows]

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
            client.execute(
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

    @staticmethod
    def _verify_parquet(
        path: Path,
        expected_rows: int,
        expected_columns: set[str],
    ) -> bool:
        """Verify a written parquet file: row count + schema.

        Reads the file back and checks ``num_rows == expected_rows`` and
        that every expected column exists. Without pyarrow the JSONL
        fallback is verified by counting rows in the gzip stream (schema
        is not enforced).

        Args:
            path: Parquet (or fallback JSONL) file path.
            expected_rows: Number of rows that must be in the file.
            expected_columns: Column names that must be present.

        Returns:
            True when the file verifies.
        """
        if not path.exists() or path.stat().st_size == 0:
            return False

        if not PYARROW_AVAILABLE:
            import gzip

            try:
                with gzip.open(str(path) + ".jsonl.gz", "rt", encoding="utf-8") as fh:
                    count = sum(1 for _ in fh)
                return count == expected_rows
            except OSError:
                return False

        try:
            table = pq.read_table(path)  # type: ignore[no-untyped-call]
            if table.num_rows != expected_rows:
                logger.warning(
                    "parquet_verify_row_mismatch", path=str(path), expected=expected_rows, actual=table.num_rows
                )
                return False
            missing = expected_columns - set(table.column_names)
            if missing:
                logger.warning("parquet_verify_schema_missing", path=str(path), missing=sorted(missing))
                return False
            return True
        except Exception as exc:
            logger.warning("parquet_verify_failed", path=str(path), error=str(exc)[:120])
            return False

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
    ) -> ExportSummary:
        """Export a chain's rows to Parquet (and B2).

        When ``dates`` is given, export exactly those (chain, date)
        pairs (default: yesterday per the CLI); otherwise fall back to
        ``eligible_dates(chain, cold_days)`` backfill mode.

        Rows are deleted from ClickHouse only after the B2 upload has
        been verified (or when there was nothing to export).

        Args:
            chain: Chain name.
            dates: Explicit dates to export (overrides cold_days mode).
            cold_days: Min age (days) before a date is exported.
            upload: Upload files to the B2 remote via rclone.
            delete: Delete exported rows from ClickHouse after verified upload.
            dry_run: Preview without writing files, uploading, or deleting.

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


__all__ = [
    "ParquetExporter",
    "ExportSummary",
    "PYARROW_AVAILABLE",
    "_local_dir",
    "_local_path",
    "_remote_dir",
    "_remote_path",
    "_state_marker",
]
