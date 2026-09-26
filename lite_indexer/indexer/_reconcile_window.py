"""Reconcile-window computation — clamped to what the durable store can answer.

Bug (2026-09-26): the reconcile window was derived from the sync cycle's
``(from_block, to_block)`` and counted against ClickHouse *after* the head
had advanced past it. Rows for the tail of the range were still in flight
(still in an unmerged part, or simply not written yet), so ``count()``
returned 0, ``discrepancy_pct`` came back at 100.0, and the indexer raised
``LiteIndexerDataReconciliationMismatch`` plus a bogus "state will be
rewound for backfill" warning.

Verified against the live store on robinhood: head ``2626310``,
``max(block_number)`` ``2625507``, and the count for the head's tail window
``[2625508, 2626310]`` is ``0`` — a 100% "mismatch" that is really just
un-flushed data.

The window is therefore clamped to the store's own ``max(block_number)``:

* a range whose upper bound is beyond the durable max is truncated to it;
* if truncation leaves nothing (the whole range is in the in-flight tail),
  the check is **not verifiable** and is reported as skipped — never as a
  mismatch, so no alert and no rewind fire;
* genuinely short data below the durable max is still verified and still
  rewinds, so real drift is not masked.

Pure function, no I/O — the regression test drives it directly.
"""

from __future__ import annotations

from typing import Any


def resolve_reconcile_window(
    from_block: int,
    to_block: int,
    durable_max_block: int | None,
) -> dict[str, Any]:
    """Resolve the ClickHouse verification window for a reconcile check.

    Args:
        from_block: Start of the range the sync cycle just wrote.
        to_block: End of the range the sync cycle just wrote.
        durable_max_block: Highest block the durable store actually holds
            (``max(block_number)`` for this chain), or ``None`` when the
            store is empty or the query failed.

    Returns:
        ``{"ok", "from_block", "to_block", "verifiable", "reason", "clamped"}``.

        ``verifiable`` is False when the window cannot be compared against
        the store yet (no durable max, inverted range, or the whole range
        sits above it). Callers must treat that as "skip the check", never
        as a mismatch.
    """
    lo = int(from_block)
    hi = int(to_block)

    if hi < lo:
        return {
            "ok": False,
            "from_block": lo,
            "to_block": hi,
            "verifiable": False,
            "reason": "inverted_range",
            "clamped": False,
        }

    # No durable max: the store is empty or the stats query failed. Any
    # count against it is meaningless, so never derive a mismatch from it.
    if durable_max_block is None:
        return {
            "ok": False,
            "from_block": lo,
            "to_block": hi,
            "verifiable": False,
            "reason": "durable_max_unknown",
            "clamped": False,
        }

    durable_max = int(durable_max_block)

    # Whole range is in the in-flight tail above the durable max — the
    # exact false positive this module exists to stop.
    if lo > durable_max:
        return {
            "ok": False,
            "from_block": lo,
            "to_block": hi,
            "verifiable": False,
            "reason": "ahead_of_durable_max",
            "clamped": False,
        }

    # Head moved into the range: verify only the part the store can answer.
    clamped = hi > durable_max
    window_hi = min(hi, durable_max)
    return {
        "ok": True,
        "from_block": lo,
        "to_block": window_hi,
        "verifiable": True,
        "reason": "",
        "clamped": clamped,
    }


__all__ = ["resolve_reconcile_window"]
