"""sqd_sitting.py — fetch one sitting of a day and advance the cursor.

WHY THIS MODULE
    Part of splitting sqd_daily.py (was 302 lines, over the 300-line cap) by
    responsibility. This module owns ONE SITTING: pull a window of rows,
    stop at the measured ceiling, persist the cursor.

    It knows the day (WindowState) and the knobs (SitConfig) but nothing
    about assembling parts or clearing them.

WHY A CURSOR AND NOT A WHOLE-DAY PASS
    The SQD portal does not enforce the documented ~20 req / 10 s. Measured
    2026-11-18: bare block requests 8/8 batches OK; ERC-20 log requests trip
    HTTP 529 at batch 3 after 61,352 rows; a polygon day is ~598,000 rows =
    9.7x the ceiling. Lowering the request rate changed nothing, which is how
    we know it is a VOLUME ceiling, not a rate one. Retrying into a volume
    ceiling is what gets an account banned, and a ban loses all 14 chains at
    once. So a day is fetched as a sequence of sittings.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_backfill import stream_window
from lite_indexer.query.sqd_day_state import (
    DEFAULT_STATE_DIR,
    WindowState,
    save_state,
)
from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

logger = get_logger(__name__)

#: Rows per sitting. The portal sheds on volume (~61k), so this stays well
#: under it. A day is many sittings by design.
DEFAULT_ROWS_PER_SITTING = 50_000

#: Default block batch per request.
DEFAULT_BLOCK_BATCH = 200

#: Default pause between requests.
DEFAULT_SLEEP_S = 0.55


@dataclass(frozen=True)
class SitConfig:
    """The knobs one sitting needs, bundled to stay under the 5-param ceiling.

    All "how much / how fast" settings; the day itself lives in WindowState.
    A 7-parameter call is easy to transpose and hard to read at a call site.
    """

    state_dir: Path = DEFAULT_STATE_DIR
    part_dir: Path = Path(".")
    rows_per_sitting: int = DEFAULT_ROWS_PER_SITTING
    block_batch: int = DEFAULT_BLOCK_BATCH
    sleep_s: float = DEFAULT_SLEEP_S


def run_sitting(
    state: WindowState,
    cfg: SitConfig,
    *,
    src: SQDSource | None = None,
) -> WindowState:
    """Fetch one sitting, appending a parquet part and advancing the cursor.

    Stops when ``rows_per_sitting`` would be exceeded -- the cap that keeps us
    under the measured 61k shed point -- and persists the cursor. Returns the
    updated state; the caller decides whether to sit again.
    """
    if state.complete:
        return state
    source = src or SQDSource()
    dataset = source.EVM_DATASETS[state.chain]
    part_dir = cfg.part_dir
    part_dir.mkdir(parents=True, exist_ok=True)
    part = part_dir / f"part-{state.sittings:03d}.ndjson"

    collected = 0
    cursor = state.next_block
    shed = False
    last_row: dict[str, Any] = {}
    with part.open("w", encoding="utf-8") as fh:
        try:
            for row in stream_window(
                source,
                state.chain,
                dataset,
                state.table,
                state.next_block,
                state.last_block,
                cfg.block_batch,
                cfg.sleep_s,
            ):
                fh.write(json.dumps(row, default=str))
                fh.write("\\n")
                collected += 1
                last_row = row
                if collected >= cfg.rows_per_sitting:
                    break
        except SQDPortalError as exc:
            # The portal sheds on VOLUME, so a 529 mid-sitting is expected,
            # not exceptional. What must NOT happen is losing the rows already
            # collected: swallowing it wrong discards a sitting's work and
            # leaves the cursor unmoved, so the next run repeats the same
            # rows forever. Persist what we have; the caller cools down.
            shed = True
            logger.info(
                "sqd_window_shed",
                chain=state.chain,
                table=state.table,
                day=state.day,
                rows=collected,
                detail=str(exc)[:120],
            )

    # advance only to the last block actually consumed
    if collected and last_row:
        cursor = int(last_row.get("block_number", cursor)) + 1
    elif not shed:
        # the stream ended before the cap: the day is fully fetched
        cursor = state.last_block + 1

    state.next_block = max(cursor, state.next_block)
    state.rows_so_far += collected
    state.sittings += 1
    state.complete = state.next_block > state.last_block
    save_state(cfg.state_dir, state)
    logger.info(
        "sqd_window_sitting",
        chain=state.chain,
        table=state.table,
        day=state.day,
        rows=collected,
        next_block=state.next_block,
        remaining=state.remaining_blocks,
        complete=state.complete,
    )
    return state


def build_sit_config(
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    part_dir: Path = Path("."),
    rows_per_sitting: int = DEFAULT_ROWS_PER_SITTING,
) -> SitConfig:
    """Convenience constructor so callers do not hand-assemble the dataclass."""
    return SitConfig(
        state_dir=state_dir,
        part_dir=part_dir,
        rows_per_sitting=rows_per_sitting,
    )
