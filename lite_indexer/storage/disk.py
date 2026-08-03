"""
Disk space management — free space checks and capacity enforcement.

Provides utilities for checking disk availability before write operations
to prevent disk-full scenarios.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from lite_indexer.config import DATA_DIR, DISK_CAP_BYTES, MIN_FREE_DISK_BYTES
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def check_disk_space(min_free_bytes: int = MIN_FREE_DISK_BYTES) -> bool:
    """Check if there's enough disk space to continue.

    Returns True if OK, False if dangerously low.
    """
    try:
        # Check the parent of DATA_DIR or /srv/data if DATA_DIR is relative
        check_path = DATA_DIR.parent if DATA_DIR.is_absolute() else Path("/srv/data")
        usage = shutil.disk_usage(check_path)
        free = usage.free
        if free < min_free_bytes:
            logger.error(
                "disk_full",
                free_gb=round(free / 1e9, 2),
                min_gb=round(min_free_bytes / 1e9, 2),
            )
            return False
        return True
    except OSError as exc:
        logger.warning("disk_space_check_failed", error=str(exc)[:80])
        return True  # Fail open


def check_disk_cap(
    data_dir: Path = DATA_DIR,
    cap_bytes: int = DISK_CAP_BYTES,
    exclude_patterns: tuple[str, ...] = (),
) -> bool:
    """Check if data directory is under the disk cap.

    Args:
        data_dir: Directory to measure.
        cap_bytes: Maximum allowed bytes.
        exclude_patterns: Glob patterns to exclude from the size
            calculation (passed to ``du --exclude``). Used to omit the
            NDJSON replay buffer (``*.jsonl`` / ``*.jsonl.zst``) when
            ``NDJSON_ENABLED`` is off so the cap only counts the durable
            Parquet/state data.

    Returns True if we can write, False if cap exceeded.
    """
    if not data_dir.exists():
        return True

    try:
        cmd = ["/usr/bin/du", "-sb", str(data_dir)]
        for pattern in exclude_patterns:
            cmd += ["--exclude", pattern]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0:
            total_bytes = int(result.stdout.split()[0])
            if total_bytes >= cap_bytes:
                logger.error(
                    "disk_cap_exceeded",
                    usage_gb=round(total_bytes / 1024**3, 2),
                    cap_gb=round(cap_bytes / 1024**3, 2),
                )
                return False
            return True
    except (subprocess.SubprocessError, FileNotFoundError):
        pass

    # Fallback
    try:
        stat = os.statvfs(str(data_dir))
        free_bytes = stat.f_frsize * stat.f_bavail
        return free_bytes > 1024**3  # At least 1GB free
    except (OSError, AttributeError):
        return True  # Fail open


def check_write_ok(label: str = "", data_dir: Path = DATA_DIR, cap_bytes: int = DISK_CAP_BYTES) -> bool:
    """Combined disk cap + free space check before a write operation."""
    if not check_disk_cap(data_dir, cap_bytes):
        logger.error("write_aborted_disk_cap", label=label)
        return False
    if not check_disk_space():
        logger.error("write_aborted_disk_space", label=label)
        return False
    return True
