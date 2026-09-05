"""ClickHouse storage for Solana transfer traces.

Split out of the former monolithic ``traces/solana.py`` (939 lines).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

from .helpers import SOLANA_TRANSFERS_TABLE

logger = get_logger(__name__)

_SOLANA_TRACE_DDL = f"""
CREATE TABLE IF NOT EXISTS {SOLANA_TRANSFERS_TABLE} (
    chain String DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    instruction_path Array(UInt16),
    from_address String,
    to_address String,
    amount UInt256,
    mint String DEFAULT '',
    decimals UInt8 DEFAULT 9,
    program_id String,
    transfer_type LowCardinality(String),
    fee_payer String,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (signature, instruction_path)
TTL block_timestamp + INTERVAL 90 DAY
"""

_SOLANA_TRACE_INSERT_COLUMNS = (
    "chain",
    "slot",
    "block_timestamp",
    "signature",
    "instruction_path",
    "from_address",
    "to_address",
    "amount",
    "mint",
    "decimals",
    "program_id",
    "transfer_type",
    "fee_payer",
    "ingested_at",
)


def ensure_solana_transfers_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_transfers`` if it does not exist.

    Idempotent and best-effort, mirroring the EVM trace store: the table
    is also provisioned by ``migrations/004_solana_transfers.sql``; this
    exists so the table appears on first trace write without operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_SOLANA_TRACE_DDL)
        logger.info("solana_transfers_table_ensured", table=SOLANA_TRANSFERS_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_transfers_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_transfers_table_failed", error=str(exc)[:100])
        return False


def write_solana_transfers(ch_client: Any, rows: list[dict[str, Any]]) -> int:
    """Insert Solana transfer-trace rows into ClickHouse.

    Best-effort: returns 0 on failure without raising. Rows follow the
    schema produced by :func:`extract_solana_transfers`.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        rows: Standardized transfer-trace row dicts.

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
                row.get("chain") or "solana",
                int(row.get("slot") or 0),
                ts,
                row.get("signature") or "",
                [int(i) for i in (row.get("instruction_path") or [])],
                row.get("from_address") or "",
                row.get("to_address") or "",
                int(row.get("amount") or 0),
                row.get("mint") or "",
                int(row.get("decimals") or 0),
                row.get("program_id") or "",
                row.get("transfer_type") or "",
                row.get("fee_payer") or "",
                now,
            )
        )

    try:
        ch_client.execute(
            f"INSERT INTO {SOLANA_TRANSFERS_TABLE} ({', '.join(_SOLANA_TRACE_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("solana_transfers_inserted", rows=len(prepared))
        metrics_incr("solana_transfers_written", len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        metrics_incr("clickhouse_errors")
        logger.error(
            "solana_transfers_insert_failed",
            rows=len(prepared),
            error=str(exc)[:150],
        )
        return 0


__all__ = ["ensure_solana_transfers_table", "write_solana_transfers"]
