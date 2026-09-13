"""
Internal-transfer trace storage — ``wallet_memory.internal_transfers``.

Every row is one value-moving internal call (CALL with value>0, CREATE
with value>0, SELFDESTRUCT) extracted from ``trace_block`` /
``debug_traceBlockByNumber`` by :mod:`lite_indexer.traces`. This is the
#1 analytical blind spot for the scanner: WETH unwraps, bridge deposits,
fee flows, and SELFDESTRUCT payouts never appear as ERC-20 ``Transfer``
logs, so without this table the wallet graph is missing real ETH flows.

* :func:`ensure_internal_transfers_table` — idempotent DDL.
* :func:`write_internal_transfers` — batch insert of standardized rows.

Schema matches the ClickHouse migration ``migrations/003_*.sql`` and is
also ensured at runtime (idempotent, non-fatal) so the table appears on
first trace write without operator DDL.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_TRACE_TABLE, RETENTION_DAYS
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

_TRACE_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_TRACE_TABLE} (
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime64(3),
    tx_hash String,
    trace_address Array(UInt16),
    from_address String,
    to_address String,
    value_wei UInt256,
    call_type LowCardinality(String),
    gas_used UInt64,
    error String DEFAULT '',
    input_method_id String DEFAULT '',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (chain, block_number, tx_hash, trace_address)
TTL block_timestamp + INTERVAL {RETENTION_DAYS.get("internal_transfers", 90)} DAY
"""

_INSERT_COLUMNS = (
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "trace_address",
    "from_address",
    "to_address",
    "value_wei",
    "call_type",
    "gas_used",
    "error",
    "input_method_id",
)


async def ensure_internal_transfers_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.internal_transfers`` if it does not exist.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(_TRACE_DDL)  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        logger.info("internal_transfers_table_ensured", table=CH_TRACE_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("internal_transfers_table_skip", error=str(exc)[:80])
        else:
            logger.warning("internal_transfers_table_failed", error=str(exc)[:100])
        return False


def write_internal_transfers(ch_client: Any, chain: str, rows: list[dict[str, Any]]) -> int:
    """Insert internal-transfer rows into ClickHouse.

    Best-effort: returns 0 on failure without raising. Rows follow the
    schema produced by ``TraceCollector.extract_internal_transfers``.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        chain: Chain name.
        rows: Standardized internal-transfer row dicts.

    Returns:
        Number of rows inserted (0 on failure/unavailable).
    """
    if not rows:
        return 0

    now = datetime.now(UTC)
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        ts = row.get("block_timestamp")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = now
        if ts is None:
            ts = now
        prepared.append(
            (
                row.get("chain") or chain,
                int(row.get("block_number") or 0),
                ts,
                row.get("tx_hash") or "",
                [int(i) for i in (row.get("trace_address") or [])],
                row.get("from_address") or "",
                row.get("to_address") or "",
                int(row.get("value_wei") or 0),
                row.get("call_type") or "",
                int(row.get("gas_used") or 0),
                row.get("error") or "",
                row.get("input_method_id") or "",
            )
        )

    try:
        ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {CH_TRACE_TABLE} ({', '.join(_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("internal_transfers_inserted", chain=chain, rows=len(prepared))
        metrics_incr("internal_transfers_written", len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        metrics_incr("clickhouse_errors")
        logger.error(
            "internal_transfers_insert_failed",
            chain=chain,
            rows=len(prepared),
            error=str(exc)[:150],
        )
        return 0
