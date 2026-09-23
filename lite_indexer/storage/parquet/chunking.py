"""Block-window chunk planning for oversized parquet export days.

The reader materializes one window in memory; days above the row cap
(polygon ~16.5M rows/day) are split into block-number windows sized to
~60% of the cap so exports stay memory-safe without changing the
(chain, date) contract or the upload/delete flow.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from typing import Any

import os

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


def fetch_kind_parts(
    fetch: Callable[..., list[dict[str, Any]]],
    table: str,
    chain: str,
    day: Any,
    chunks: list[tuple[int | None, int | None]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Fetch each window; single-window days keep the unsuffixed part name."""
    parts: list[tuple[str, list[dict[str, Any]]]] = []
    for idx, (block_from, block_to) in enumerate(chunks):
        suffix = f"part{idx:02d}" if len(chunks) > 1 else ""
        parts.append((suffix, fetch(table, chain, day, bounds=(block_from, block_to))))
    return parts


def write_kind_parts(
    write: Callable[[list[dict[str, Any]], Any], int],
    verify: Callable[[Any, int, set[str]], bool],
    base_path: Any,
    parts: list[tuple[str, list[dict[str, Any]]]],
) -> tuple[int, bool]:
    """Write + verify each part; returns (bytes_written, all_ok)."""
    bytes_written = 0
    ok = True
    for suffix, rows in parts:
        if not rows:
            continue
        local = base_path
        if suffix:
            local = base_path.with_name(f"{base_path.stem}-{suffix}{base_path.suffix}")
        written = write(rows, local)
        bytes_written += written
        if not (written and verify(local, len(rows), set(rows[0].keys()))):
            ok = False
    return bytes_written, ok


@dataclass(frozen=True)
class DayPlan:
    """Chain/day/cap context for one export-day fetch."""

    chain: str
    day: Any
    cap: int
    client: Any


def fetch_day_kinds(
    fetch: Callable[..., list[dict[str, Any]]],
    stats: Callable[[str, str, Any], tuple[int, int | None, int | None]],
    tables: tuple[tuple[str, str], ...],
    plan: DayPlan,
) -> dict[str, list[tuple[str, list[dict[str, Any]]]]]:
    """Plan + fetch every kind's block-window parts for one day."""
    out: dict[str, list[tuple[str, list[dict[str, Any]]]]] = {}
    for kind, table in tables:
        count, min_block, max_block = stats(plan.client, table, plan.chain, plan.day)
        bound = partial(fetch, plan.client)
        chunks = plan_day_chunks(count, min_block, max_block, plan.cap)
        out[kind] = fetch_kind_parts(bound, table, plan.chain, plan.day, chunks)
    return out
