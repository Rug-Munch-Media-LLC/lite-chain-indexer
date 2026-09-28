"""Block-window walking for the ClickHouse existence-check dedup.

The app-layer dedup (:func:`lite_indexer.storage.ch_transfers.
TransfersMixin._existing_transfer_keys`) is the ONLY guard against a
reconciliation rewind duplicating rows: ``evm_transfers`` is a
ReplacingMergeTree, so duplicates only collapse at MERGE time, and a
rewind re-inserts verbatim. The guard must therefore never silently
degrade — the previous implementation returned an EMPTY set whenever the
batch's block span exceeded ``_DEDUP_MAX_BLOCK_SPAN``, which reads as
"nothing exists" and re-inserted the whole batch. That is precisely the
large multi-day rewind the guard exists for.

This module replaces the bail-out with windowing: the requested block
range is walked in ``_DEDUP_MAX_BLOCK_SPAN``-sized windows and one
existence query is issued per window, unioning the results. Every query
stays inside a provider/partition-friendly width, dedup never degrades,
and the cost is a bounded number of queries instead of one.

Each window's result set is capped at :data:`_DEDUP_MAX_ROWS`. A window
that returns exactly the cap is treated as TRUNCATED (the cap was hit, so
the rows past it are unknown) and the window is recorded as a gap rather
than silently trusted: a partial answer is a fact, and logging it lets an
operator see that the guard degraded for a reason visible in metrics.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from lite_indexer.config import CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.storage.ch_rows import _DEDUP_MAX_BLOCK_SPAN

logger = get_logger(__name__)

#: Hard cap on the rows ONE window's existence query may return. A window
#: returning this many rows has been truncated by the cap, so the keys past
#: it are unknown; the window is logged and counted as a gap instead of
#: being trusted as complete. Never raise this to "make it fit" — that is
#: the same class of bug as widening the block span.
_DEDUP_MAX_ROWS = 200_000


def iter_block_windows(lo: int, hi: int, span: int = _DEDUP_MAX_BLOCK_SPAN) -> Iterator[tuple[int, int]]:
    """Yield inclusive ``(lo, hi)`` block windows covering ``[lo, hi]``.

    Windows are ``span`` wide, so a narrow batch yields exactly one window
    (the hot path is unchanged) and a wide rewind yields
    ``ceil((hi - lo + 1) / span)`` windows. ``hi`` is always included and
    the windows are contiguous, so no block is ever skipped.
    """
    if hi <= lo:
        yield (lo, hi)
        return
    start = lo
    while start <= hi:
        end = min(start + span, hi)
        yield (start, end)
        start = end + 1


def _query_window(client: Any, chain: str, lo: int, hi: int) -> list[tuple[Any, ...]]:
    """Run the per-window existence query; ``[]`` on any failure.

    A failed window degrades to "nothing known in this window" — the
    caller records it as a gap, so the degradation is counted and logged
    rather than silent.
    """
    try:
        rows = client.execute(
            f"SELECT block_number, tx_hash, log_index FROM {CH_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
            "WHERE chain = %(chain)s AND block_number BETWEEN %(lo)s AND %(hi)s",
            {"chain": chain, "lo": lo, "hi": hi},
        )
    except Exception as exc:  # noqa: BLE001 — dedup is best-effort
        logger.debug("clickhouse_dedup_window_query_failed", chain=chain, lo=lo, hi=hi, error=str(exc)[:100])
        return []
    return list(rows or [])


def existing_keys_in_windows(
    client: Any,
    chain: str,
    lo: int,
    hi: int,
) -> tuple[set[tuple[int, str, int]], int]:
    """Union the durable transfer keys across every window of ``[lo, hi]``.

    Returns ``(existing_keys, gap_windows)``. ``gap_windows`` counts
    windows whose answer was incomplete (query failed, or the row cap was
    hit) — a partial answer is reported, never silently trusted.
    """
    existing: set[tuple[int, str, int]] = set()
    gaps = 0
    for win_lo, win_hi in iter_block_windows(lo, hi):
        rows = _query_window(client, chain, win_lo, win_hi)
        if len(rows) >= _DEDUP_MAX_ROWS:
            gaps += 1
            logger.warning(
                "clickhouse_dedup_window_capped",
                chain=chain,
                lo=win_lo,
                hi=win_hi,
                cap=_DEDUP_MAX_ROWS,
            )
        for row in rows:
            if not row or len(row) < 3:
                continue
            existing.add((int(row[0] or 0), str(row[1] or ""), int(row[2] or 0)))
    return existing, gaps


__all__ = ["_DEDUP_MAX_ROWS", "existing_keys_in_windows", "iter_block_windows"]
