"""Measure per-chain temporal freshness from ClickHouse.

Split from :mod:`lite_indexer.freshness` so the POLICY (what counts as stale)
stays pure and testable, and the SQL lives in one place.

Reads ``address_activity`` -- the durable 21-chain history -- not
``evm_transfers``. ``evm_transfers`` is the real-time transfer feed and is
intentionally sparse per chain (arbitrum legitimately holds ~0 rows there while
holding 38.7M in ``address_activity``); auditing it reported healthy chains as
dead. `address_activity` is the table every chain writes to, so it is the one
that can answer "did this chain produce anything today".
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from lite_indexer.freshness import (
    ChainFreshness,
    FreshnessReport,
    classify,
)

logger = logging.getLogger(__name__)

#: The durable per-chain history table. Every chain writes here.
FRESHNESS_TABLE = "wallet_memory.address_activity"

#: Column holding the on-chain timestamp in ``address_activity``.
FRESHNESS_COLUMN = "block_time"

_ROWS_SQL = f"""
SELECT
    chain,
    toString(max(toDate({FRESHNESS_COLUMN})))                                  AS newest_day,
    dateDiff('day', max(toDate({FRESHNESS_COLUMN})), today())                   AS days_stale,
    countIf({FRESHNESS_COLUMN} >= now() - INTERVAL 24 HOUR)                     AS rows_recent
FROM {FRESHNESS_TABLE}
GROUP BY chain
ORDER BY days_stale DESC
"""  # nosemgrep: formatted-sql-query -- identifiers are module constants


def _unavailable(
    report: FreshnessReport, chains: list[str] | None, detail: str
) -> FreshnessReport:
    """Mark every requested chain ``unavailable`` with a reason.

    Split out of :func:`measure` so the happy path and both failure paths each
    stay inside the complexity budget (CCN 13 became 3x small functions).
    """
    for chain in chains or []:
        report.chains[chain] = ChainFreshness(
            chain=chain, status="unavailable", detail=detail
        )
    return report


def _collect(
    report: FreshnessReport, rows: list[tuple[Any, ...]], chains: list[str] | None
) -> FreshnessReport:
    """Turn raw SQL rows into verdicts, then account for requested-but-absent chains."""
    seen: set[str] = set()
    for chain, newest_day, days_stale, rows_recent in rows:
        seen.add(chain)
        report.chains[chain] = classify(
            chain,
            newest_day,
            int(days_stale) if days_stale is not None else None,
            int(rows_recent or 0),
        )
    # A chain that is configured but has never written a row is EMPTY, not
    # absent: silence is the failure, so it must appear in the report.
    for chain in chains or []:
        if chain not in seen:
            report.chains[chain] = classify(chain, None, None, 0)
    return report


def measure(ch_client: Any, chains: list[str] | None = None) -> FreshnessReport:
    """Measure temporal freshness for every chain in the durable table.

    Args:
        ch_client: ClickHouse client exposing ``execute``.
        chains: Optional subset; ``None`` audits every chain present.

    Returns:
        A :class:`FreshnessReport`. A query failure yields a report with every
        requested chain marked ``unavailable`` rather than raising, so a
        monitoring caller reports "cannot tell" instead of crashing.
    """
    report = FreshnessReport(as_of=datetime.now(UTC).isoformat())

    if ch_client is None:
        return _unavailable(report, chains, "clickhouse_unavailable")

    try:
        rows = ch_client.execute(_ROWS_SQL)
    except Exception as exc:  # noqa: BLE001 -- audit must never crash the timer
        logger.warning("freshness_query_failed", error=str(exc)[:160])
        return _unavailable(report, chains, str(exc)[:120])

    return _collect(report, rows, chains)


__all__ = ["FRESHNESS_COLUMN", "FRESHNESS_TABLE", "measure"]
