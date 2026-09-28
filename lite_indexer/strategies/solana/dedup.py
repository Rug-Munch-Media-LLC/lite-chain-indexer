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

Throughput: the existence check is ONE ranged query per batch, never one
query per key. The table sorts on ``(mint, slot, token_account)`` so a
``slot BETWEEN`` filter cannot use the primary index directly, but the
slot predicate still prunes partitions/granules far cheaper than N
sequential round-trips on the hot path — measured 200 slots ≈ 6.8K rows
returned in a single call.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config.chains import SOLANA_CH_TABLE
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: A batch spanning more slots than this is not existence-checked: one
#: query per batch keeps the dedup cheap, and an unbounded window would
#: make the existence query dominate the insert it is protecting.
MAX_DEDUP_SLOT_SPAN = 50_000

#: Rows the single ranged query may return before it gives up and lets
#: the insert proceed undeduped. Bounds the per-batch wire cost on the
#: hot path; exceeding it degrades to availability (see below), never to
#: a refused write.
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

    One ranged query over the batch's slot window, then filter the
    returned rows against the requested keys — mirroring
    ``ch_transfers._existing_transfer_keys``. A per-key loop was the
    first cut and it was wrong: a 200-slot window of balance changes is
    thousands of sequential round-trips on the Solana hot path.

    Best-effort: a query failure, an oversized window, or an oversized
    result returns an empty set so the insert still proceeds
    (availability beats dedup — the alternative is refusing to write on
    a transient ClickHouse error).
    """
    if not keys:
        return set()
    lo = min(k[0] for k in keys)
    hi = max(k[0] for k in keys)
    if hi - lo > MAX_DEDUP_SLOT_SPAN:
        logger.warning("solana_dedup_span_skipped", span=hi - lo)
        return set()
    try:
        rows = client.execute(
            f"SELECT slot, tx_hash, token_account FROM {SOLANA_CH_TABLE} "  # nosemgrep: formatted-sql-query — table is a module constant; values parameterized
            "WHERE slot BETWEEN %(lo)s AND %(hi)s LIMIT %(limit)s",
            {"lo": lo, "hi": hi, "limit": MAX_DEDUP_ROWS},
        )
    except Exception as exc:  # noqa: BLE001 — dedup is best-effort
        logger.debug("solana_dedup_query_failed", error=str(exc)[:100])
        return set()
    wanted = set(keys)
    existing: set[tuple[int, str, str]] = set()
    for row in rows or []:
        if not row or len(row) < 3:
            continue
        key = (int(row[0] or 0), str(row[1] or ""), str(row[2] or ""))
        if key in wanted:
            existing.add(key)
    return existing


__all__ = ["MAX_DEDUP_ROWS", "MAX_DEDUP_SLOT_SPAN", "balance_event_key", "dedup_balance_events", "existing_balance_keys"]
