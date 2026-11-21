"""sqd_parts.py — assemble and clear the parquet parts of one day.

WHY THIS MODULE
    Part of splitting sqd_daily.py (was 302 lines, over the 300-line cap) by
    responsibility. This module owns the PART FILES on disk: reading them
    back, counting rows, and removing them once a day is published.

    It knows nothing about cursors or the portal. Give it a directory of
    parquet parts and it answers "what rows do I have".
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

#: Part files are newline-delimited JSON while a sitting is in progress and
#: parquet once a sitting completes. Both must be readable, or a resumed day
#: loses the rows already fetched.
PART_GLOBS = ("*.parquet", "*.ndjson")


def _read_part(path: Path) -> list[dict[str, Any]]:
    """One part file -> its rows. ndjson and parquet are both valid parts."""
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        return pq.read_table(path).to_pylist()
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def assemble_parts(part_dir: Path) -> list[dict[str, Any]]:
    """Read every part in a day's directory into one row list.

    Parts are written one per sitting, so a day is a set of files rather
    than one big in-memory structure. This is the read-back used before
    publishing: the caller compares len() against the cursor's rows_so_far.
    """
    rows: list[dict[str, Any]] = []
    if not part_dir.is_dir():
        return rows
    seen: set[Path] = set()
    for pattern in PART_GLOBS:
        for part in sorted(part_dir.glob(pattern)):
            if part in seen:
                continue
            seen.add(part)
            rows.extend(_read_part(part))
    return rows


def clear_parts(part_dir: Path) -> int:
    """Delete a day's part files after a successful publish. Returns the count.

    Only called once the day is published and verified -- clearing earlier
    would lose rows that were never archived.
    """
    if not part_dir.is_dir():
        return 0
    removed = 0
    for pattern in PART_GLOBS:
        for part in part_dir.glob(pattern):
            if not part.is_file():
                continue
            part.unlink()
            removed += 1
    # the cursor json is the resume state; leave it until the day is gone
    return removed


def read_state_json(path: Path) -> dict[str, Any] | None:
    """Read a cursor file for inspection (used by the operator CLI)."""
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None
