"""
NDJSON writer — appends events as JSONL files organized by chain/date.

Auto-compresses files older than 15 minutes to .zst after each write.
This prevents uncontrolled disk growth between rotation cycles.
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lite_indexer.config import DATA_DIR, DISK_CAP_BYTES
from lite_indexer.logging import get_logger
from lite_indexer.storage.disk import check_write_ok

logger = get_logger(__name__)

# Compress NDJSON files older than this many seconds (15 min).
# Keeps recent data hot for parallel readers, compresses quickly after.
_STALE_AGE_SECS = 900

# Zstd compression level — fast since JSONL compresses easily.
_ZSTD_LEVEL = 3


class NDJSONWriter:
    """Append events as NDJSON (JSONL) files.

    File path: {data_dir}/{chain}/{date}/events.jsonl
    DEX path:  {data_dir}/{chain}/dex/{date}/events.jsonl

    After each write, stale files (older than 15 min) are compressed
    in-place to .zst and the raw .jsonl is removed.
    """

    def __init__(self, data_dir: Path = DATA_DIR, disk_cap: int = DISK_CAP_BYTES) -> None:
        self.data_dir = data_dir
        # Explicit disk cap so check_write_ok() honors the configured cap
        # (e.g. --disk-cap 50000000000 = 50 GB) instead of falling back to
        # the 10 GB DISK_CAP_BYTES default. Without this, NDJSON/parquet
        # archive writes silently abort when usage exceeds 10 GB.
        self.disk_cap = disk_cap

    def write_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Append transfer events to NDJSON. Returns bytes written."""
        if not events:
            return 0
        if not check_write_ok("ndjson", data_dir=self.data_dir, cap_bytes=self.disk_cap):
            return 0

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        out_dir = self.data_dir / chain / today
        out_path = out_dir / "events.jsonl"

        written = self._write_lines(events, out_path)
        self._compress_stale()
        return written

    def write_dex_events(self, chain: str, events: list[dict[str, Any]]) -> int:
        """Append DEX swap events to NDJSON. Returns bytes written."""
        if not events:
            return 0
        if not check_write_ok("dex_ndjson", data_dir=self.data_dir, cap_bytes=self.disk_cap):
            return 0

        today = datetime.now(UTC).strftime("%Y-%m-%d")
        out_dir = self.data_dir / chain / "dex" / today
        out_path = out_dir / "events.jsonl"

        written = self._write_lines(events, out_path)
        self._compress_stale()
        return written

    def _write_lines(self, events: list[dict[str, Any]], out_path: Path) -> int:
        """Write NDJSON lines to a file in one batch. Returns bytes written."""
        try:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [json.dumps(e, ensure_ascii=False) + "\n" for e in events]
            bytes_written = 0
            with open(out_path, "a") as f:
                for line in lines:
                    f.write(line)
                    bytes_written += len(line.encode("utf-8"))

            logger.info(
                "ndjson_written",
                path=str(out_path),
                events=len(events),
                bytes_written=bytes_written,
            )
            return bytes_written
        except OSError as exc:
            logger.error("ndjson_write_failed", path=str(out_path), error=str(exc))
            return 0

    def _compress_stale(self) -> None:
        """Compress .jsonl files older than _STALE_AGE_SECS to .zst and remove raw."""
        now = time.time()
        compressed_count = 0
        freed_bytes = 0
        for jsonl_file in self.data_dir.rglob("*.jsonl"):
            if not jsonl_file.is_file():
                continue
            age = now - jsonl_file.stat().st_mtime
            if age < _STALE_AGE_SECS:
                continue
            zst_path = jsonl_file.with_suffix(".jsonl.zst")
            if zst_path.exists():
                # Already compressed — just remove the stale raw file
                raw_size = jsonl_file.stat().st_size
                jsonl_file.unlink(missing_ok=True)
                freed_bytes += raw_size
                compressed_count += 1
                logger.debug("ndjson_removed_raw", path=str(jsonl_file))
                continue
            try:
                raw_size = jsonl_file.stat().st_size
                subprocess.run(
                    ["zstd", f"-{_ZSTD_LEVEL}", "--rm", str(jsonl_file), "-o", str(zst_path)],
                    capture_output=True,
                    timeout=60,
                    check=True,
                )
                freed_bytes += raw_size
                compressed_count += 1
                logger.info(
                    "ndjson_compressed",
                    path=str(zst_path),
                    raw_bytes=raw_size,
                    age_seconds=int(age),
                )
            except (subprocess.CalledProcessError, FileNotFoundError) as exc:
                logger.error("ndjson_compress_failed", path=str(jsonl_file), error=str(exc))
        if compressed_count:
            logger.info(
                "ndjson_compress_batch",
                files=compressed_count,
                freed_bytes=freed_bytes,
            )
