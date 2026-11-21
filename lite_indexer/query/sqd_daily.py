"""sqd_daily.py — one day of the SQD windowed backfill (facade).

WHY THIS FILE IS NOW THIN (2026-11-21)
    It was 302 lines and did five jobs, which failed the size ratchet and --
    more importantly -- meant one module answered several questions. Split by
    responsibility:

      sqd_day_state.py   the DAY and its CURSOR (WindowState, load/save, bounds)
      sqd_sitting.py     ONE SITTING (fetch a window, stop at the ceiling)
      sqd_parts.py       the PARQUET PARTS (assemble, count, clear)

    This module is the facade: it re-exports the public names so existing
    imports (`from lite_indexer.query.sqd_daily import run_sitting`) keep
    working, and it owns the one thing that spans all three -- opening a day.

    Anything new belongs in the module that owns the job, not here.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from lite_indexer.query.sqd_day_state import (  # noqa: F401  (re-exported)
    DEFAULT_STATE_DIR,
    WindowState,
    _state_path,
    day_block_range,
    load_state,
    save_state,
)
from lite_indexer.query.sqd_parts import (  # noqa: F401  (re-exported)
    assemble_parts,
    clear_parts,
)
from lite_indexer.query.sqd_sitting import (  # noqa: F401  (re-exported)
    DEFAULT_BLOCK_BATCH,
    DEFAULT_ROWS_PER_SITTING,
    DEFAULT_SLEEP_S,
    SitConfig,
    build_sit_config,
    run_sitting,
)


def open_day(
    chain: str,
    table: str,
    day: dt.date,
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    src: Any | None = None,
) -> WindowState:
    """Resume a day's state, or create it from the chain's day bounds.

    Resuming is the whole point: a day does not fit in one process lifetime,
    so the cursor file is what makes a later sitting continue rather than
    restart.
    """
    existing = load_state(state_dir, chain, table, day.isoformat())
    if existing is not None:
        return existing

    from lite_indexer.query.sqd_source import SQDSource

    source = src or SQDSource()
    first, last = day_block_range(source, chain, table, day)
    state = WindowState(
        chain=chain,
        table=table,
        day=day.isoformat(),
        next_block=first,
        last_block=last,
    )
    save_state(state_dir, state)
    return state


__all__ = [
    "DEFAULT_BLOCK_BATCH",
    "DEFAULT_ROWS_PER_SITTING",
    "DEFAULT_SLEEP_S",
    "DEFAULT_STATE_DIR",
    "SitConfig",
    "WindowState",
    "assemble_parts",
    "build_sit_config",
    "clear_parts",
    "day_block_range",
    "load_state",
    "open_day",
    "run_sitting",
    "save_state",
]
