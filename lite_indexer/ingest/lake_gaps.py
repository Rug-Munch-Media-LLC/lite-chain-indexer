"""Detect (chain, date) partitions missing in ClickHouse but present in the lake.

The AWS v1.1 sonarx lake advances daily with roughly a one-week lag. This
module closes the automation gap left after a manual recovery: it finds
partitions the lake CAN serve (metadata-cheap row count > 0) while ClickHouse
holds ZERO rows for that day, then hands those dates to
:func:`lite_indexer.ingest.lake_backfill.backfill_chain_from_lake`
(router/quota-aware). No RPC anywhere.

GAP RULE — a date is a gap iff ``lake_has(chain, date)`` AND
``ch_rows(chain, date) == 0``:

* lake present + CH empty     → GAP (fill it).
* lake present + CH non-zero  → NOT a gap, even when the count is small: the
  ``evm_transfers`` ReplacingMergeTree dedups at merge time and reconciliation
  rewrites are idempotent, so partial counts are normal and a refill would only
  re-insert rows that are already there.
* lake missing + CH empty     → NOT a gap: the lake cannot serve the date, so
  queuing it would be pointless (that is a lake-coverage/RPC problem, not ours).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date
from datetime import timedelta
from typing import Any

from lite_indexer.ingest.lake_backfill import LakeBackfillRequest, backfill_chain_from_lake
from lite_indexer.ingest.lake_sonarx import CHAINS, SonarxLakeSource
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

CH_TABLE = "wallet_memory.evm_transfers"
_CH_ROWS_SQL = (
    f"SELECT count() FROM {CH_TABLE} "  # nosemgrep: formatted-sql-query — constant table, values parameterized
    "WHERE chain = %(chain)s AND block_timestamp >= toDateTime(%(lo)s) "
    "AND block_timestamp < toDateTime(%(hi)s)"
)
_LOG_GAP_CAP = 10


def lookback_dates(days: int, *, end: str | None = None) -> list[str]:
    """Expand a ``days``-long window ending today (UTC) into ISO dates, oldest first."""
    if days <= 0:
        return []
    last = _date.fromisoformat(end) if end else _date.today()
    return [(last - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]


def _lake_has(src: Any, chain: str, day: str) -> bool:
    """True when the lake holds rows for ``(chain, day)`` (parquet metadata only)."""
    try:
        return int(src.count(chain, "transfers", day)) > 0
    except Exception as exc:  # noqa: BLE001 — an absent partition is a normal answer
        logger.debug("lake_gap_partition_absent", chain=chain, date=day, error=str(exc)[:80])
        return False


def _ch_rows(client: Any, chain: str, day: str) -> int:
    """Row count in ``evm_transfers`` for one ``(chain, day)`` partition."""
    nxt = (_date.fromisoformat(day) + timedelta(days=1)).isoformat()
    rows = client.execute(_CH_ROWS_SQL, {"chain": chain, "lo": day, "hi": nxt})
    if not rows:
        return 0
    return int(rows[0][0]) if rows[0] else 0


def _default_source() -> Any:
    """Build the real SonarxLakeSource."""
    return SonarxLakeSource()


def _default_ch_client() -> Any:
    """Build the real ClickHouse client (same one the backfill writer uses)."""
    from lite_indexer.storage.clickhouse import ClickHouseWriter

    return ClickHouseWriter().client()


def find_gaps(chain: str, days: int = 30, *, source: Any | None = None, ch_client: Any | None = None) -> list[str]:
    """Dates in the lookback window that are gaps (lake has them, CH is empty).

    ``source`` / ``ch_client`` are DI seams (tests); defaults are live objects.
    An unavailable source or CH client yields ``[]`` — nothing is provable, and
    the detector must never queue work it cannot confirm.
    """
    if chain not in CHAINS:
        raise ValueError(f"unsupported sonarx chain: {chain!r} (expected one of {CHAINS})")
    src = source if source is not None else _default_source()
    if not src.available():
        logger.warning("lake_gap_source_unavailable", chain=chain)
        return []
    client = ch_client if ch_client is not None else _default_ch_client()
    if client is None:
        logger.warning("lake_gap_ch_unavailable", chain=chain)
        return []
    return [d for d in lookback_dates(days) if _lake_has(src, chain, d) and _ch_rows(client, chain, d) == 0]


def fill_gaps(chain: str, dates: list[str], *, source: Any | None = None, writer: Any | None = None) -> dict[str, int]:
    """Backfill ``dates`` for ``chain`` through the router-aware lake backfill."""
    if not dates:
        return {}
    return backfill_chain_from_lake(LakeBackfillRequest(chain=chain, dates=list(dates)), source=source, writer=writer)


@dataclass
class GapDeps:
    """Optional DI seams for :func:`main` (tests); a ``None`` field is built live."""

    source: Any | None = None
    ch_client: Any | None = None
    writer: Any | None = None


def main(chain: str, days: int = 30, fill: bool = False, *, deps: GapDeps | None = None) -> dict[str, Any]:
    """Scan one chain for gaps, log ``lake_gap_scan``, and optionally fill them."""
    seams = deps if deps is not None else GapDeps()
    gaps = find_gaps(chain, days, source=seams.source, ch_client=seams.ch_client)
    logger.info(
        "lake_gap_scan",
        chain=chain,
        dates_checked=days,
        gaps_found=len(gaps),
        gaps=gaps[:_LOG_GAP_CAP],
    )
    result: dict[str, Any] = {"chain": chain, "dates_checked": days, "gaps_found": len(gaps), "gaps": gaps}
    if fill and gaps:
        counts = fill_gaps(chain, gaps, source=seams.source, writer=seams.writer)
        logger.info("lake_gap_fill", chain=chain, dates=len(gaps), counts=counts)
        result["fill"] = counts
    return result
