"""Batch insert path for ``wallet_memory.address_activity``.

Extracted from ``storage/address_activity.py`` (shrink-only size ratchet).
Best-effort by contract: returns 0 on failure without raising, so a
ClickHouse hiccup never stalls the ingest loop.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_ADDRESS_ACTIVITY_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.address_activity_ddl import _DEFAULT_CHAIN, _INSERT_COLUMNS

logger = get_logger(__name__)


def write_address_activity(ch_client: Any, rows: list[dict[str, Any]]) -> int:
    """Insert expanded address-activity rows into ClickHouse.

    Best-effort: returns 0 on failure without raising. Rows follow the
    schema produced by :func:`parse_address_activity` (timestamps may be
    aware datetimes, ISO strings, or Unix seconds).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        rows: Expanded address-activity row dicts.

    Returns:
        Number of rows inserted (0 on failure/unavailable).
    """
    if not rows:
        return 0

    now = datetime.now(UTC)
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        ts = row.get("block_time")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = now
        if isinstance(ts, (int, float)):
            try:
                ts = datetime.fromtimestamp(int(ts), tz=UTC)
            except (ValueError, TypeError, OverflowError):
                ts = now
        if not isinstance(ts, datetime):
            ts = now
        prepared.append(
            (
                str(row.get("address") or ""),
                str(row.get("tx_signature") or ""),
                int(row.get("slot") or 0),
                ts,
                [str(a) for a in (row.get("involved_accounts") or [])],
                [str(p) for p in (row.get("program_ids") or [])],
                bool(row.get("is_signer")),
                now,
                str(row.get("chain") or _DEFAULT_CHAIN),
            )
        )

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_ADDRESS_ACTIVITY_TABLE} ({', '.join(_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("address_activity_inserted", rows=len(prepared))
        metrics_incr("address_activity_written", len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        metrics_incr("clickhouse_errors")
        logger.error(
            "address_activity_insert_failed",
            rows=len(prepared),
            error=str(exc)[:150],
        )
        return 0
