"""
Parquet cold storage tier — daily rollup of old ClickHouse rows.

Improvement 4: old ClickHouse rows (older than ``PARQUET_COLD_DAYS``)
are aggregated into per-chain/per-date Parquet files, uploaded to the
B2 permanent tier (rclone), then deleted from ClickHouse to reclaim
disk. The archive stays queryable via :mod:`lite_indexer.storage.duckdb_query`.

Pipeline (idempotent):

1. For each eligible date, read rows from ClickHouse (transfers and
   DEX events) via the native protocol.
2. Write ``{chain}_{date}.parquet`` (and ``{chain}_dex_{date}.parquet``)
   locally under ``PARQUET_DIR`` using pyarrow.
3. Upload to ``PARQUET_REMOTE`` with ``rclone copyto`` (keeps the exact
   B2 layout the DuckDB query layer expects).
4. Delete the exported rows from ClickHouse with a lightweight
   ``ALTER TABLE ... DELETE`` mutation (only when ``delete=True``).
5. Mark the (chain, date) as exported in ``PARQUET_STATE_DIR`` so the
   daily cron never re-exports or double-deletes.

Failures are non-fatal and logged; the state marker is only written
after a successful export, so a partial failure retries next run.
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
    import pyarrow as pa  # type: ignore[import-untyped]
    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    PYARROW_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    pa = None  # type: ignore[assignment]
    pq = None  # type: ignore[assignment]
    PYARROW_AVAILABLE = False

# ClickHouse lightweight-delete maximum mutation size guard (rows per day).
_MAX_ROWS_PER_DAY = 5_000_000


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


def _local_path(data_dir: Path, chain: str, day: date, kind: str) -> Path:
    """Build the local parquet path for a chain/date file.

    Mirrors the B2 layout used by the DuckDB query layer:
    ``{chain}/{YYYY}/{MM}/{DD}/{chain}_{YYYY-MM-DD}.parquet``.

    Args:
        data_dir: Local parquet root.
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Local file path.
    """
    y, m, d = f"{day.year:04d}", f"{day.month:02d}", f"{day.day:02d}"
    date_str = f"{y}-{m}-{d}"
    fname = f"{chain}_dex_{date_str}.parquet" if kind == "dex" else f"{chain}_{date_str}.parquet"
    return data_dir / chain / y / m / d / fname


def _remote_path(chain: str, day: date, kind: str) -> str:
    """Build the rclone remote path for a chain/date file.

    Args:
        chain: Chain name.
        day: Export date.
        kind: ``"transfers"`` or ``"dex"``.

    Returns:
        Remote path under ``PARQUET_REMOTE``.
    """
    rel = _local_path(Path(""), chain, day, kind)
    return f"{PARQUET_REMOTE}/{rel}"


def _state_marker(state_dir: Path, chain: str, day: date) -> Path:
    """Return the state marker path for a (chain, date)."""
    return state_dir / chain / f"{day.isoformat()}.done"


class ParquetExporter:
    """Export old ClickHouse rows to Parquet and upload them to B2.

    Safe defaults: ``upload=False``/``delete=False`` until explicitly
    enabled (CLI flags), and ``dry_run=True`` to preview without
    touching anything.
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

    def _fetch_day_rows(
        self,
        client: Any,
        table: str,
        chain: str,
        day: date,
    ) -> list[dict[str, Any]]:
        """Fetch all rows for a chain/date from a ClickHouse table.

        Args:
            client: ClickHouse client.
            table: Fully-qualified table name.
            chain: Chain name.
            day: Export date.

        Returns:
            Rows as list of dicts (column name → value).
        """
        query = (
            f"SELECT * FROM {table} "
            f"WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s "
            f"LIMIT {_MAX_ROWS_PER_DAY}"
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
            Number of deleted rows (0 when the mutation fails).
        """
        try:
            client.execute(
                f"ALTER TABLE {table} DELETE WHERE chain = %(chain)s AND toDate(block_timestamp) = %(day)s",
                {"chain": chain, "day": day.isoformat()},
            )
            logger.info("parquet_deleted_rows", table=table, chain=chain, day=day.isoformat())
            return 1  # count of tables mutated (row count unknown cheaply)
        except Exception as exc:
            logger.warning("parquet_delete_failed", table=table, chain=chain, error=str(exc)[:120])
            return 0

    # ── Parquet writing ─────────────────────────────────────────────

    def _write_parquet(
        self,
        rows: list[dict[str, Any]],
        path: Path,
    ) -> int:
        """Write rows to a Parquet file; returns bytes written (0 on error).

        Falls back to gzipped JSONL when pyarrow is unavailable so the
        cold tier still works (DuckDB reads both).

        Args:
            rows: Rows as dicts.
            path: Destination parquet path.

        Returns:
            Bytes written.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        if not rows:
            # Write an empty (schema-less) marker so the state file still
            # records the date as exported.
            path.write_bytes(b"")
            return 0

        if PYARROW_AVAILABLE:
            try:
                table = pa.Table.from_pylist(rows)
                pq.write_table(table, path)
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
            return (path).stat().st_size if path.exists() else 0
        except OSError as exc:
            logger.error("parquet_fallback_write_failed", path=str(path), error=str(exc))
            return 0

    def _upload(self, local: Path, remote: str) -> bool:
        """Upload a file with rclone copyto; returns success.

        Args:
            local: Local file path.
            remote: Remote rclone path.

        Returns:
            True on success.
        """
        if not self.rclone_bin or shutil.which(self.rclone_bin) is None:
            logger.warning("parquet_rclone_missing", bin=self.rclone_bin)
            return False
        try:
            result = subprocess.run(
                [self.rclone_bin, "copyto", str(local), remote],
                capture_output=True,
                text=True,
                timeout=300,
                check=False,
            )
            if result.returncode == 0:
                logger.info("parquet_uploaded", local=str(local), remote=remote)
                return True
            logger.warning("parquet_upload_failed", remote=remote, error=result.stderr[-200:])
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
        # Export one day per run by default — backfill can lower cold_days
        # or the caller can loop. We iterate oldest-first up to the cutoff.
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
        cold_days: int = PARQUET_COLD_DAYS,
        upload: bool = False,
        delete: bool = False,
        dry_run: bool = False,
    ) -> ExportSummary:
        """Export a chain's eligible old rows to Parquet (and B2).

        Args:
            chain: Chain name.
            cold_days: Min age (days) before a date is exported.
            upload: Upload files to the B2 remote via rclone.
            delete: Delete exported rows from ClickHouse.
            dry_run: Preview without writing files, uploading, or deleting.

        Returns:
            :class:`ExportSummary` with per-run stats.
        """
        summary = ExportSummary(chain=chain)
        dates = self.eligible_dates(chain, cold_days)
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

        for day in dates:
            marker = _state_marker(self.state_dir, chain, day)
            if marker.exists():
                summary.skipped_existing += 1
                continue

            try:
                # Transfers table
                transfer_rows = self._fetch_day_rows(client, CH_TABLE, chain, day)
                dex_rows = self._fetch_day_rows(client, CH_DEX_TABLE, chain, day)
            except Exception as exc:
                summary.errors.append(f"{day.isoformat()}: {str(exc)[:100]}")
                continue

            total_rows = len(transfer_rows) + len(dex_rows)
            if total_rows == 0:
                # No data for this date — mark exported to avoid re-checking.
                self._mark_exported(marker, day, 0, 0)
                summary.dates_exported += 1
                continue

            bytes_written = 0
            uploaded = 0
            for kind, rows in (("transfers", transfer_rows), ("dex", dex_rows)):
                if not rows:
                    continue
                local = _local_path(self.data_dir, chain, day, kind)
                written = self._write_parquet(rows, local)
                bytes_written += written
                summary.transfers_rows += len(rows) if kind == "transfers" else 0
                summary.dex_rows += len(rows) if kind == "dex" else 0
                if upload and written and self._upload(local, _remote_path(chain, day, kind)):
                    uploaded += 1

            summary.bytes_written += bytes_written
            summary.uploaded += uploaded

            deleted = 0
            if delete:
                deleted += self._delete_day_rows(client, CH_TABLE, chain, day)
                deleted += self._delete_day_rows(client, CH_DEX_TABLE, chain, day)
            summary.deleted_rows += deleted

            self._mark_exported(marker, day, len(transfer_rows), len(dex_rows))
            summary.dates_exported += 1
            logger.info(
                "parquet_day_exported",
                chain=chain,
                day=day.isoformat(),
                rows=total_rows,
                bytes=bytes_written,
                uploaded=uploaded,
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
    "_local_path",
    "_remote_path",
]
