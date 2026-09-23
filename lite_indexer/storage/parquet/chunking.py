"""Block-window chunk planning for oversized parquet export days.

The reader materializes one window in memory; days above the row cap
(polygon ~16.5M rows/day) are split into block-number windows sized to
~60% of the cap so exports stay memory-safe without changing the
(chain, date) contract or the upload/delete flow.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

#: Rows per block window. Sized for memory: a window's rows are materialized
#: as dicts AND a pyarrow table (~4-5 KB/row) — 3M-row windows hit ~14 GB and
#: OOM-killed the box (2026-09-22). 1M rows ≈ 4-5 GB peak; override with
#: ``PARQUET_EXPORT_CHUNK_ROWS``.
_EXPORT_CHUNK_ROWS = int(os.environ.get("PARQUET_EXPORT_CHUNK_ROWS", "1000000"))


def chunk_rows(cap: int) -> int:
    """Rows per window: the memory target, never above the safety cap."""
    return max(1, min(cap, _EXPORT_CHUNK_ROWS))


def plan_day_chunks(
    count: int,
    min_block: int | None,
    max_block: int | None,
    cap: int,
) -> list[tuple[int | None, int | None]]:
    """Block windows for a day, sized by the MEMORY target (not just the cap).

    ``cap`` (``_max_rows_per_day()``) is only the point at which a day *must*
    be split at all. The number of windows is driven by ``chunk_rows(cap)`` —
    the per-window row target from ``PARQUET_EXPORT_CHUNK_ROWS`` — because a
    day under the cap can still be far too large to materialise in one pandas
    frame: measured ~4.7 GB per 1M rows, so ethereum 2026-09-22 (3.93M rows,
    under the cap) loaded whole-day, peaked at 8.1 GB and was OOM-killed
    (2026-09-23, twice; a 12G ceiling made it worse — swap thrash).

    Returns ``[(None, None)]`` — one unsuffixed window — only when the day fits
    the memory target; the multi-part layout appears above it.
    """
    if min_block is None or max_block is None or max_block < min_block:
        return [(None, None)]
    target = chunk_rows(cap)
    if count <= target:
        return [(None, None)]
    windows = max(2, (count + target - 1) // target)
    step = max(1, (max_block - min_block + 1 + windows - 1) // windows)
    out: list[tuple[int | None, int | None]] = []
    start = min_block
    while start <= max_block:
        out.append((start, min(start + step - 1, max_block)))
        start += step
    return out


def fetch_write_day_kinds(
    fetch: Callable[..., list[dict[str, Any]]],
    stats: Callable[..., tuple[int, int | None, int | None]],
    tables: tuple[tuple[str, str], ...],
    plan: DayPlan,
    write: Callable[[list[dict[str, Any]], Any], int],
    verify: Callable[[Any, int, set[str]], bool],
    local_path_for: Callable[[str], Any],
) -> tuple[dict[str, int], int, dict[str, bool]]:
    """Fetch and WRITE each block window before fetching the next.

    Streaming matters as much as window sizing. The previous flow collected
    every window of every kind and then flattened them into a second full copy
    (``[r for _s, rows in parts for r in rows]``), so a day existed twice in
    RAM: polygon OOM-killed the export unit at 6.7 GB (2026-09-23) even after
    the windows were sized down. Here each window is written and released
    before the next fetch, so the peak is a single window (~250k rows with
    ``PARQUET_EXPORT_CHUNK_ROWS=250000``).

    Returns:
        ``(rows_per_kind, bytes_written, ok_per_kind, block_span)`` — counts and
        the (start, end) block span, but never the rows themselves. The span is
        accumulated here because ``_block_range()`` used to need the row lists
        (the snapshot manifest records it); (0, 0) when no row carries a block.
    """
    rows_per_kind: dict[str, int] = {}
    ok_per_kind: dict[str, bool] = {}
    bytes_written = 0
    span_start: int | None = None
    span_end: int | None = None
    for kind, table in tables:
        count, min_block, max_block = stats(plan.client, table, plan.chain, plan.day)
        chunks = plan_day_chunks(count, min_block, max_block, plan.cap)
        base_path = local_path_for(kind)
        kind_rows = 0
        kind_ok = True
        for idx, (block_from, block_to) in enumerate(chunks):
            rows = fetch(plan.client, table, plan.chain, plan.day, bounds=(block_from, block_to))
            if not rows:
                continue
            for row in rows:  # span while the window is in hand (mirrors _block_range)
                raw = row.get("block_number")
                if raw is None:
                    continue
                try:
                    block = int(raw)
                except (TypeError, ValueError):
                    continue
                if span_start is None or block < span_start:
                    span_start = block
                if span_end is None or block > span_end:
                    span_end = block
            suffix = f"part{idx:02d}" if len(chunks) > 1 else ""
            local = base_path
            if suffix:
                local = base_path.with_name(f"{base_path.stem}-{suffix}{base_path.suffix}")
            written = write(rows, local)
            bytes_written += written
            kind_rows += len(rows)
            if not (written and verify(local, len(rows), set(rows[0].keys()))):
                kind_ok = False
            del rows  # release this window before fetching the next
        rows_per_kind[kind] = kind_rows
        ok_per_kind[kind] = kind_ok
    return rows_per_kind, bytes_written, ok_per_kind, (
        span_start if span_start is not None else 0,
        span_end if span_end is not None else 0,
    )


@dataclass(frozen=True)
class DayPlan:
    """Chain/day/cap context for one export-day fetch."""

    chain: str
    day: Any
    cap: int
    client: Any


