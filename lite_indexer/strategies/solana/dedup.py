"""Dedup keys for ``wallet_memory.solana_spl_balance_changes``.

Ported from the EVM approach (``storage/ch_transfers.py``): the Solana
write path inserted every row unconditionally, so re-running the same
slot window (a reconciliation rewind, a resumed catch-up, a duplicate
``getBlock`` page) double-inserted every balance-change row and the table
— unlike ``evm_transfers`` — is a plain MergeTree with no
ReplacingMergeTree dedup key to collapse them at merge time.

Two layers, matching the EVM path:

1. **In-batch** — collapse duplicate keys within the list being written
   (:func:`dedup_balance_events`).
2. **Existing** — skip keys already durable in ClickHouse
   (:func:`existing_balance_keys`), so a re-extracted window is idempotent
   instead of merely deduped-within-itself.

The key is ``(slot, signature, token_account)``: a balance change is
uniquely identified by the account whose balance moved, inside the
transaction that moved it, at the slot it happened in. The log index is
not available on the Solana side (an instruction has no per-instruction
index in the parsed payload we key off), and the pair above is unique
without it.

Throughput: the existence check is ONE ranged query per WINDOW, never one
query per key. The table sorts on ``(mint, slot, token_account)`` so a
``slot BETWEEN`` filter cannot use the primary index directly, but the
slot predicate still prunes partitions/granules far cheaper than N
sequential round-trips on the hot path — measured 200 slots ≈ 6.8K rows
returned in a single call. A normal batch is one window (one query); only
a rewind wider than ``MAX_DEDUP_SLOT_SPAN`` costs more than one, and that
is the price of not re-appending a permanent duplicate.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config.chains import SOLANA_CH_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.storage.ch_dedup_windows import iter_block_windows

logger = get_logger(__name__)

#: WIDTH OF ONE WINDOW, not a gate. A batch spanning more slots than this
#: is walked in ceil(span / MAX_DEDUP_SLOT_SPAN) windows (one query each,
#: results unioned) — see ``storage.ch_dedup_windows.iter_block_windows``
#: for the shared walker. It must never become a bail-out: returning "nothing
#: exists" for a wide range makes the insert re-append every row, and this
#: table is a plain MergeTree with NO ReplacingMergeTree to collapse them
#: later, so duplicates are permanent.
MAX_DEDUP_SLOT_SPAN = 50_000

#: Rows the ONE window's query may return before the answer is reported as
#: incomplete. The cap bounds per-batch wire cost; hitting it means the
#: window's key set is PARTIAL, so it is logged and counted, never silently
#: trusted.
MAX_DEDUP_ROWS = 200_000


def balance_event_key(event: dict[str, Any]) -> tuple[int, str, str]:
    """Dedup key for one SPL balance-change event."""
    return (
        int(event.get("slot") or 0),
        str(event.get("signature") or event.get("txHash") or event.get("tx_hash") or ""),
        str(event.get("token_account") or ""),
    )


def dedup_balance_events(events: list[dict[str, Any]]) -> dict[tuple[int, str, str], dict[str, Any]]:
    """Collapse duplicate ``(slot, signature, token_account)`` keys.

    First occurrence wins, so a later duplicate can never overwrite the
    row the parser produced first.
    """
    unique: dict[tuple[int, str, str], dict[str, Any]] = {}
    for event in events:
        unique.setdefault(balance_event_key(event), event)
    return unique


def existing_balance_keys(
    client: Any,
    keys: list[tuple[int, str, str]],
) -> set[tuple[int, str, str]]:
    """Return the dedup keys already durable for this chain.

    One ranged query PER WINDOW over the batch's slot range, then filter the
    returned rows against the requested keys — mirroring
    ``ch_transfers._existing_transfer_keys``. A per-key loop was the first
    cut and it was wrong: a 200-slot window of balance changes is
    thousands of sequential round-trips on the Solana hot path.

    A range wider than ``MAX_DEDUP_SLOT_SPAN`` is WALKED in that-width
    windows, never abandoned. The earlier bail-out returned an empty set
    for a wide range, which reads as "nothing exists" and re-inserts the
    whole batch — and because this table has no ReplacingMergeTree, those
    duplicate rows are permanent. A narrow batch still issues exactly ONE
    query.

    Best-effort per window: a failed window (or one whose rows hit
    ``MAX_DEDUP_ROWS``) is logged and counted as a gap, never silently
    trusted (availability beats dedup — the alternative is refusing to
    write on a transient ClickHouse error).
    """
    if not keys:
        return set()
    lo = min(k[0] for k in keys)
    hi = max(k[0] for k in keys)
    windows = 0
    gaps = 0
    wanted = set(keys)
    existing: set[tuple[int, str, str]] = set()
    for win_lo, win_hi in iter_block_windows(lo, hi, MAX_DEDUP_SLOT_SPAN):
        windows += 1
        try:
            rows = client.execute(
                f"SELECT slot, tx_hash, token_account FROM {SOLANA_CH_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
                "WHERE slot BETWEEN %(lo)s AND %(hi)s LIMIT %(limit)s",
                {"lo": win_lo, "hi": win_hi, "limit": MAX_DEDUP_ROWS},
            )
        except Exception as exc:  # noqa: BLE001 — dedup is best-effort
            gaps += 1
            logger.debug("solana_dedup_query_failed", lo=win_lo, hi=win_hi, error=str(exc)[:100])
            continue
        rows = list(rows or [])
        if len(rows) >= MAX_DEDUP_ROWS:
            gaps += 1
            logger.warning("solana_dedup_window_capped", lo=win_lo, hi=win_hi, cap=MAX_DEDUP_ROWS)
        for row in rows:
            if not row or len(row) < 3:
                continue
            key = (int(row[0] or 0), str(row[1] or ""), str(row[2] or ""))
            if key in wanted:
                existing.add(key)
    if windows > 1:
        logger.warning("solana_dedup_span_chunked", span=hi - lo, windows=windows)
    if gaps:
        logger.warning("solana_dedup_degraded", gap_windows=gaps, windows=windows)
    return existing


__all__ = [
    "MAX_DEDUP_ROWS",
    "MAX_DEDUP_SLOT_SPAN",
    "balance_event_key",
    "dedup_balance_events",
    "existing_balance_keys",
]
