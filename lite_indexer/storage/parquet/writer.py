"""Parquet write/upload/export orchestration (ParquetExporter)."""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from lite_indexer.config import (
    BOOTSTRAP_B2_BUCKET,
    BOOTSTRAP_PARQUET_PATH,
    PARQUET_DIR,
    PARQUET_RCLONE_BIN,
    PARQUET_REMOTE,
    PARQUET_STATE_DIR,
)
from lite_indexer.logging import get_logger
from lite_indexer.snapshot_manifest import MANIFEST_NAME, build_manifest, manifest_json
from lite_indexer.storage.parquet.export_day import ExportDayMixin
from lite_indexer.storage.parquet.export_flow import ExportFlowMixin
from lite_indexer.storage.parquet.export_state import ExportStateMixin
from lite_indexer.storage.parquet.helpers import _local_dir, _local_path, _state_marker
from lite_indexer.storage.parquet.reader import _verify_parquet
from lite_indexer.storage.parquet.schema import (
    PYARROW_AVAILABLE,
    pa,
    pq,
)
from lite_indexer.storage.parquet.tooling import run as tool_run
from lite_indexer.storage.parquet.tooling import which as tool_which

logger = get_logger(__name__)


class ParquetExporter(ExportFlowMixin, ExportDayMixin, ExportStateMixin):
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
        if not self.rclone_bin or tool_which(self.rclone_bin) is None:
            logger.warning("parquet_rclone_missing", bin=self.rclone_bin)
            return False
        try:
            copy = tool_run(
                [self.rclone_bin, "copy", str(local), remote],
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
            if copy.returncode != 0:
                logger.warning("parquet_upload_failed", remote=remote, error=copy.stderr[-300:])
                return False
            verify = tool_run(
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
            # Chunked exports write `{stem}-partNN{suffix}` (one file per block
            # window) and never the bare name, so resolve the real files rather
            # than requiring the unsuffixed one: a day with dex_rows=0 or any
            # chunked day otherwise published an empty manifest (arbitrum/base,
            # 2026-09-23: "snapshot_publish_no_files" + exit 1). The single-file
            # layout still matches the same pattern.
            files = sorted(local.glob(f"{path.stem}*{path.suffix}"))
            if not files:
                continue
            tables[table_name] = {
                "rows": transfers_rows if kind == "transfers" else dex_rows,
                "size_bytes": sum(f.stat().st_size for f in files),
                "parquet_files": [f.name for f in files],
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
