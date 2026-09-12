"""
Recovery + integrity tooling for LiteChainIndexer.

Provides the machinery behind the ``--recover`` CLI flag and the
``integrity-check`` subcommand:

1. :func:`recover_chain_state` — query ClickHouse for the actual max
   block/slot per chain, then reset the state file (and the monotonic
   high-water mark) to that value. Fixes the observed 08/03 + 08/04
   state regressions (``last_block`` clobbered to ~93K while CH held
   25.6M) even when the in-process guard never ran.

2. :func:`run_integrity_check` — per-chain ClickHouse row/range audit:
   min/max block, distinct blocks present, total rows, and a
   contiguity percentage. This is the daily integrity check — it flags
   chains whose durable store has gaps or whose row counts do not match
   the expected range, so operators know before data compounds.

3. :func:`find_missing_ranges` — cheap gap detection over the block
   axis (sampled strided distinct blocks) used by ``--recover
   --backfill`` to tell the operator exactly which ranges are missing
   below the CH max.

All ClickHouse interactions are best-effort: an unreachable CH returns
a structured error result and never raises.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from lite_indexer.config import CH_TABLE, SOLANA_CH_TABLE, STATE_DIR
from lite_indexer.logging import get_logger
from lite_indexer.state import StateHighWater, StateManager

logger = get_logger(__name__)

# Coarse stride for the missing-range scan. Fine enough to catch a
# state-regression genesis gap (which is millions of blocks wide), cheap
# enough to run on a 359M-row table (uniqExact over strided blocks is a
# small fraction of the table's distinct-block cardinality).
GAP_SCAN_STRIDE: int = 1000


def ch_table_for(chain: str, chain_type: str = "evm") -> tuple[str, str]:
    """Return (table, block_column) for a chain's durable store.

    Args:
        chain: Chain name.
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.

    Returns:
        ``(table, column)`` tuple.
    """
    if chain_type == "solana":
        return SOLANA_CH_TABLE, "slot"
    return CH_TABLE, "block_number"


def query_chain_stats(ch_client: Any, chain: str, chain_type: str = "evm") -> dict[str, Any]:
    """Query per-chain durable-store stats (min/max/distinct/count).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        chain: Chain name.
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.

    Returns:
        ``{ok, min_block, max_block, distinct_blocks, total_rows}`` with
        ``ok=False`` and an ``error`` when the query failed.
    """
    result: dict[str, Any] = {
        "ok": False,
        "min_block": None,
        "max_block": None,
        "distinct_blocks": None,
        "total_rows": 0,
        "error": "",
    }
    if ch_client is None:
        result["error"] = "clickhouse_unavailable"
        return result

    table, column = ch_table_for(chain, chain_type)
    try:
        if chain_type == "solana":
            rows = ch_client.execute(f"SELECT min({column}), max({column}), uniqExact({column}), count() FROM {table}")  # nosemgrep: formatted-sql-query, sqlalchemy-execute-raw-query — identifiers are module constants; values parameterized
        else:
            rows = ch_client.execute(
                f"SELECT min({column}), max({column}), uniqExact({column}), count() "
                f"FROM {table} WHERE chain = %(chain)s",
                {"chain": chain},
            )
    except Exception as exc:  # noqa: BLE001 — best-effort audit
        result["error"] = str(exc)[:120]
        logger.debug("integrity_stats_failed", chain=chain, error=result["error"])
        return result

    if rows and rows[0]:
        row = rows[0]
        result["min_block"] = int(row[0]) if row[0] is not None else None
        result["max_block"] = int(row[1]) if row[1] is not None else None
        result["distinct_blocks"] = int(row[2]) if row[2] is not None else None
        result["total_rows"] = int(row[3]) if row[3] is not None else 0
    result["ok"] = True
    return result


def compute_integrity(stats: dict[str, Any]) -> dict[str, Any]:
    """Derive integrity metrics from :func:`query_chain_stats` output.

    Args:
        stats: Output of :func:`query_chain_stats`.

    Returns:
        ``stats`` extended with ``contiguous_pct`` (distinct blocks /
        expected span) and ``status`` (``ok`` / ``gaps`` / ``empty`` /
        ``unavailable``).
    """
    if not stats.get("ok"):
        stats["status"] = "unavailable"
        return stats
    if stats.get("max_block") is None:
        stats["status"] = "empty"
        return stats
    min_block = stats["min_block"] or 0
    max_block = stats["max_block"]
    expected_span = max(0, max_block - min_block + 1)
    distinct = stats.get("distinct_blocks") or 0
    contiguous_pct = 100.0 * distinct / expected_span if expected_span else 100.0
    stats["expected_span"] = expected_span
    stats["contiguous_pct"] = round(contiguous_pct, 2)
    stats["status"] = "ok" if contiguous_pct >= 99.0 else "gaps"
    return stats


def find_missing_ranges(
    ch_client: Any,
    chain: str,
    chain_type: str = "evm",
    stride: int = GAP_SCAN_STRIDE,
) -> list[tuple[int, int]]:
    """Detect missing block ranges below the ClickHouse max.

    Samples every ``stride``-th block and reports contiguous spans that
    have no rows. A state regression (state file reset to ~93K while CH
    held 25.6M) re-fetches from genesis and leaves the genesis range
    populated — the gap shows up as a large missing span between the old
    resume point and the CH max.

    Best-effort: returns ``[]`` on any query failure.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.
        stride: Sampling stride on the block axis.

    Returns:
        List of ``(from_block, to_block)`` missing ranges, or ``[]``.
    """
    stats = query_chain_stats(ch_client, chain, chain_type)
    if not stats.get("ok") or stats.get("max_block") is None:
        return []
    min_block = stats["min_block"] or 0
    max_block = stats["max_block"]
    if max_block - min_block < stride:
        return []

    table, column = ch_table_for(chain, chain_type)
    try:
        if chain_type == "solana":
            rows = ch_client.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE {column} % {stride} = 0 "
                f"AND {column} BETWEEN %(f)s AND %(t)s",
                {"f": min_block, "t": max_block},
            )
        else:
            rows = ch_client.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE chain = %(chain)s "
                f"AND {column} % {stride} = 0 AND {column} BETWEEN %(f)s AND %(t)s",
                {"chain": chain, "f": min_block, "t": max_block},
            )
    except Exception as exc:  # noqa: BLE001 — best-effort scan
        logger.debug("gap_scan_failed", chain=chain, error=str(exc)[:100])
        return []

    present = {int(r[0]) for r in rows if r and r[0] is not None}
    expected = set(range(min_block, max_block + 1, stride))
    missing = sorted(expected - present)
    if not missing:
        return []

    # Collapse consecutive missing sampled blocks into ranges.
    ranges: list[tuple[int, int]] = []
    start = missing[0]
    prev = missing[0]
    for block in missing[1:]:
        if block - prev > stride:
            ranges.append((start, prev))
            start = block
        prev = block
    ranges.append((start, prev))
    # Expand sampled ranges to full block spans (minus the stride step).
    return [(lo, min(max_block, hi + stride - 1)) for lo, hi in ranges]


def recover_chain_state(
    ch_client: Any,
    chain: str,
    chain_type: str = "evm",
    state_dir: Path | None = None,
    *,
    backfill: bool = False,
) -> dict[str, Any]:
    """Reset a chain's state file to the ClickHouse max block.

    Queries CH for the actual max block/slot, writes the state file
    (``last_block``) and the monotonic high-water mark to that value, and
    reports any missing ranges (optionally rewinding ``from`` so the next
    indexer run backfills the first gap).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        chain: Chain name.
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.
        state_dir: State directory override (defaults to
            :data:`STATE_DIR`).
        backfill: When True, additionally rewind the state to the start
            of the earliest missing range so the next sync re-fetches it.

    Returns:
        ``{chain, ok, previous_state, ch_max, recovered_to, missing_ranges,
        backfill_from, error}``.
    """
    result: dict[str, Any] = {
        "chain": chain,
        "ok": False,
        "previous_state": None,
        "ch_max": None,
        "recovered_to": None,
        "missing_ranges": [],
        "backfill_from": None,
        "error": "",
    }
    state_dir = state_dir or STATE_DIR

    # Patch the state module's directory for the duration of this call so
    # StateManager/StateHighWater write into the right place.
    import lite_indexer.state as state_mod

    original = state_mod.STATE_DIR
    state_mod.STATE_DIR = state_dir

    try:
        stats = query_chain_stats(ch_client, chain, chain_type)
        if not stats.get("ok"):
            result["error"] = stats.get("error", "clickhouse_unavailable")
            return result
        ch_max = stats.get("max_block")
        if ch_max is None:
            result["error"] = "no_data_in_clickhouse"
            return result

        manager = StateManager(chain)
        previous = manager.load().get("last_block")
        result["previous_state"] = previous

        highwater = StateHighWater(chain)
        highwater.update(ch_max)
        manager.save(ch_max, 0, source="recover")
        result["ok"] = True
        result["ch_max"] = ch_max
        result["recovered_to"] = ch_max

        missing = find_missing_ranges(ch_client, chain, chain_type)
        result["missing_ranges"] = missing

        if backfill and missing:
            backfill_from = missing[0][0]
            # Rewind below the first missing range start so the next sync
            # re-fetches it (the indexer resumes at from_block + 1).
            if previous is not None and backfill_from <= previous:
                manager.save(max(0, backfill_from - 1), 0, source="recover-backfill")
                result["backfill_from"] = backfill_from
        return result
    finally:
        state_mod.STATE_DIR = original


def run_integrity_check(
    ch_client: Any,
    chains: list[str],
    chain_types: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Run the daily per-chain integrity audit.

    For each chain: query CH min/max/distinct/count and derive the
    contiguity percentage. Returns a structured report suitable for JSON
    output from the ``integrity-check`` CLI subcommand.

    Args:
        ch_client: ClickHouse client.
        chains: Chain names to audit.
        chain_types: Optional {chain: type} map (defaults to ``evm``).

    Returns:
        ``{chain: stats}`` with each chain's stats (see
        :func:`compute_integrity`).
    """
    types = chain_types or {}
    report: dict[str, dict[str, Any]] = {}
    for chain in chains:
        stats = query_chain_stats(ch_client, chain, types.get(chain, "evm"))
        report[chain] = compute_integrity(stats)
    return report


def summarize_report(report: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Summarize an integrity report for JSON CLI output.

    Args:
        report: Output of :func:`run_integrity_check`.

    Returns:
        ``{chains, ok_count, gap_count, empty_count, unavailable_count,
        total_rows, chains_with_gaps}``.
    """
    ok_count = gap_count = empty_count = unavailable_count = 0
    total_rows = 0
    chains_with_gaps: list[str] = []
    for chain, stats in report.items():
        status = stats.get("status")
        if status == "ok":
            ok_count += 1
        elif status == "gaps":
            gap_count += 1
            chains_with_gaps.append(chain)
        elif status == "empty":
            empty_count += 1
        else:
            unavailable_count += 1
        total_rows += int(stats.get("total_rows") or 0)
    return {
        "chains": len(report),
        "ok_count": ok_count,
        "gap_count": gap_count,
        "empty_count": empty_count,
        "unavailable_count": unavailable_count,
        "total_rows": total_rows,
        "chains_with_gaps": chains_with_gaps,
    }


__all__ = [
    "ch_table_for",
    "compute_integrity",
    "find_missing_ranges",
    "query_chain_stats",
    "recover_chain_state",
    "run_integrity_check",
    "summarize_report",
]
