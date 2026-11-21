"""sqd_day_state.py — one day's SQD fetch progress, persisted as JSON.

WHY THIS MODULE
    Part of splitting sqd_daily.py (was 302 lines, over the 300-line cap) by
    responsibility. This module owns the DAY and its CURSOR: what has been
    fetched, where to resume, and whether the window set is complete.

    It deliberately knows nothing about HTTP, the portal, or parquet. A
    resumed backfill only needs this.

The cursor is a JSON file, not a database row: one per (chain, table, day),
inspectable and diffable, and small enough that a table would add an
operational dependency for no gain.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

#: Where per-day window state lives. One file per (chain, table, day).
DEFAULT_STATE_DIR = Path("/var/lib/lite-chain-indexer/sqd-windows")


@dataclass
class WindowState:
    """Progress of one day's fetch across sittings.

    ``last_block`` is fixed once at first run so a moving chain tip cannot
    change the day's boundary mid-fetch. ``next_block`` is the first block NOT
    yet fetched; the day is complete when it passes ``last_block``.
    """

    chain: str
    table: str
    day: str
    next_block: int
    last_block: int
    rows_so_far: int = 0
    sittings: int = 0
    complete: bool = False

    @property
    def remaining_blocks(self) -> int:
        return max(0, self.last_block - self.next_block + 1)


def _state_path(state_dir: Path, chain: str, table: str, day: str) -> Path:
    return state_dir / f"{chain}__{table}__{day}.json"


def load_state(state_dir: Path, chain: str, table: str, day: str) -> WindowState | None:
    """Read a day's cursor, or None when it has never been started."""
    path = _state_path(state_dir, chain, table, day)
    if not path.is_file():
        return None
    try:
        return WindowState(**json.loads(path.read_text()))
    except (OSError, ValueError, TypeError):
        # A corrupt cursor must not silently restart the day from block 0 --
        # that would re-fetch rows already archived. Report and treat as absent,
        # which open_day handles by re-deriving the day bounds.
        return None


def save_state(state_dir: Path, state: WindowState) -> None:
    """Persist the cursor atomically (temp file + rename)."""
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _state_path(state_dir, state.chain, state.table, state.day)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(asdict(state), indent=2))
    tmp.replace(path)


def day_block_range(src: Any, chain: str, table: str, day: dt.date) -> tuple[int, int]:
    """(first_block, last_block) for one UTC day.

    Both bounds come from the source once, at first run, so a later run cannot
    silently move the day's end under a still-running fetch.
    """
    from lite_indexer.query.sqd_day import day_block_range as _range

    return _range(src, chain, table, day)
