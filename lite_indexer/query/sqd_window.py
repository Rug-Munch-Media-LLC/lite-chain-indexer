"""Resumable windowed pull for SQD day units.

WHY THIS EXISTS — measured 2026-11-18, not assumed
    The SQD portal does **not** enforce the documented ``~20 req / 10 s``. Walking
    batches until it answers HTTP 529:

        bare block requests : 8/8 batches OK, twice, 30 s apart
        ERC-20 log requests : trips at batch 3, after 61,352 rows
        polygon day         : ~598,000 rows  = 9.7x the ceiling

    Coding the request rate (``SQD_MIN_INTERVAL`` 0.55 s -> 1.2 s) changed
    **nothing**, which is how we know it is a VOLUME ceiling and not a
    request-rate one. A day therefore cannot be fetched in one sitting at any
    pacing, and `sqd_backfill.publish_day` — which materialises the whole day and
    fetches it in one pass — cannot complete a real chain.

    The fix is not more retries. Retrying into a volume ceiling is what gets an
    account banned, and a ban loses all 14 chains at once.

THE CONTRACT
    A day is fetched as a sequence of WINDOWS. Each window is one sitting:
    it advances a cursor, stops before the measured ceiling, and persists
    (chain, table, day, next_block, rows_so_far). A later sitting resumes from
    that cursor. Only when the cursor passes the day's last block is the window
    set complete, and only then may the day be published.

    The partial rows are held as parquet parts on local disk between sittings —
    not in memory, because the whole point is that a day does not fit in one
    process lifetime. `write_unit` still owns the publish: it re-derives the row
    count from the assembled parquet and raises on any disagreement.

WHY A CURSOR FILE AND NOT A DATABASE
    One row per (chain, table, day). A JSON file is inspectable, diffable and
    survives a container restart; the state is small enough that a table would
    add an operational dependency for no gain.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.query.sqd_backfill import (
    DEFAULT_BLOCK_BATCH,
    DEFAULT_SLEEP_S,
    TABLES,
    day_block_range,
    require_table,
    stream_window,
)
from lite_indexer.query.sqd_source import SQDPortalError, SQDSource

logger = get_logger(__name__)

#: Rows per sitting. Below the measured 61,352 ceiling with headroom for the
#: batch that crosses it — the portal sheds on VOLUME, so the cap must be on
#: rows, not on requests or seconds.
DEFAULT_ROWS_PER_SITTING = 50_000

#: Where per-day window state lives. One file per (chain, table, day).
DEFAULT_STATE_DIR = Path("/var/lib/lite-indexer/sqd-windows")


@dataclass
class WindowState:
    """Progress of one day's fetch across sittings.

    Attributes:
        chain: Local chain name.
        table: Warehouse table being fetched.
        day: UTC day, ISO date.
        next_block: First block NOT yet fetched. Resumes here.
        last_block: The day's final block, fixed once at first run so a moving
            dataset head cannot silently extend the work.
        rows_so_far: Rows collected across completed sittings.
        sittings: How many sittings have run, for observability.
        complete: True once ``next_block`` has passed ``last_block``.
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
        """Blocks still owed, clamped at zero."""
        return max(0, self.last_block - self.next_block + 1)


def _state_path(state_dir: Path, chain: str, table: str, day: str) -> Path:
    return state_dir / f"{chain}__{table}__{day}.json"


def load_state(state_dir: Path, chain: str, table: str, day: str) -> WindowState | None:
    """Read persisted window state, or None when this day has not started."""
    path = _state_path(state_dir, chain, table, day)
    try:
        raw = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    try:
        return WindowState(**raw)
    except TypeError:
        logger.warning("sqd_window_state_unreadable", path=str(path))
        return None


def save_state(state_dir: Path, state: WindowState) -> None:
    """Persist window state atomically (write-then-rename).

    A half-written cursor would restart the day from an unknown point, which is
    the one failure mode that could silently duplicate rows. Rename is atomic on
    the same filesystem, so the file is either the old state or the new one.
    """
    state_dir.mkdir(parents=True, exist_ok=True)
    path = _state_path(state_dir, state.chain, state.table, state.day)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(asdict(state), indent=1) + "\n")
    tmp.replace(path)


def open_day(
    chain: str,
    table: str,
    day: dt.date,
    *,
    state_dir: Path = DEFAULT_STATE_DIR,
    src: SQDSource | None = None,
) -> WindowState:
    """Resume a day's state, or create it from the chain's day bounds."""
    existing = load_state(state_dir, chain, table, day.isoformat())
    if existing is not None:
        return existing
    source = src or SQDSource()
    require_table(chain, table)
    dataset = source.EVM_DATASETS[chain]
    head = source.head(dataset)
    if head is None:
        raise RuntimeError(f"no dataset head for {dataset}; cannot bound {day}")
    bounds = day_block_range(source, dataset, day, head)
    if bounds is None:
        raise RuntimeError(f"{chain}/{day} produced no block range")
    first, last = bounds
    state = WindowState(
        chain=chain,
        table=table,
        day=day.isoformat(),
        next_block=first,
        last_block=last,
    )
    save_state(state_dir, state)
    return state


@dataclass(frozen=True)
class SitConfig:
    """The knobs one sitting needs, bundled so the function stays under 5 params.

    These are all "how much / how fast", not "which day" -- the state already
    carries the day. Bundling them keeps the signature honest instead of a
    7-parameter call whose arguments are easy to transpose.
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

    Stops when ``rows_per_sitting`` would be exceeded — the cap that keeps us
    under the measured 61k shed point — and persists the cursor. Returns the
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
                fh.write("\n")
                collected += 1
                if collected >= cfg.rows_per_sitting:
                    # Cursor advances only to the last block actually consumed.
                    # stream_window yields per block batch, so the safest resume
                    # point is the last row's block_number + 1.
                    cursor = int(row.get("block_number", cursor)) + 1
                    break
        except SQDPortalError as exc:
            # The portal sheds on VOLUME, so a 529 mid-sitting is expected, not
            # exceptional. What must NOT happen is losing the rows already
            # collected: swallowing it wrong discards a whole sitting's work and
            # leaves the cursor unmoved, so the next run repeats the same 50k
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
            if collected:
                cursor = int(row.get("block_number", cursor)) + 1
    if not shed and collected < cfg.rows_per_sitting:
        # The stream ended before the cap: the day is fully fetched.
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


def assemble_parts(part_dir: Path) -> list[dict[str, Any]]:
    """Load every sitting's part back into rows, in order.

    Used only at publish time, after the window set is complete. The rows are
    re-derived from disk rather than held in memory across sittings, because
    holding a day in memory is the thing the windowing exists to avoid — at
    publish time the writer needs the rows anyway, and the count it sees is the
    count it publishes.
    """
    rows: list[dict[str, Any]] = []
    for part in sorted(part_dir.glob("part-*.ndjson")):
        with part.open(encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def clear_parts(part_dir: Path) -> int:
    """Delete a day's parts after a verified publish. Returns files removed."""
    removed = 0
    for part in sorted(part_dir.glob("part-*.ndjson")):
        part.unlink(missing_ok=True)
        removed += 1
    return removed


__all__ = [
    "DEFAULT_ROWS_PER_SITTING",
    "DEFAULT_STATE_DIR",
    "TABLES",
    "WindowState",
    "assemble_parts",
    "clear_parts",
    "load_state",
    "open_day",
    "run_sitting",
    "save_state",
]
