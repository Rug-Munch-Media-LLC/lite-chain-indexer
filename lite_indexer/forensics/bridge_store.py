"""
Bridge-match persistence — ``wallet_memory.bridge_mappings``.

Every confirmed bridge match found by :class:`FundTracer` is persisted here
so cross-chain paths found once can be reused by later traces without
re-running the time-window heuristic. One row per direction:

* ``source_address`` / ``source_chain`` — the depositor on chain A.
* ``dest_address`` / ``dest_chain`` — the recipient on chain B.
* ``bridge_name`` — registry name of the bridge contract used.
* ``tx_hash`` — the source-chain deposit transaction.
* ``bridged_at`` — source-chain deposit timestamp.
* ``confidence`` — match confidence (0..1).

The table is created by ``migrations/004_forensics.sql`` and also ensured
idempotently at runtime (non-fatal) so the module works on deployments that
predate the migration.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_BRIDGE_MAPPINGS_TABLE
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_BRIDGE_MAPPINGS_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_BRIDGE_MAPPINGS_TABLE} (
    source_address String CODEC(ZSTD(3)),
    source_chain String CODEC(ZSTD(3)),
    dest_address String CODEC(ZSTD(3)),
    dest_chain String CODEC(ZSTD(3)),
    bridge_name LowCardinality(String),
    tx_hash String,
    bridged_at DateTime,
    confidence Float32 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY (source_address, source_chain)
TTL bridged_at + INTERVAL 180 DAY
"""

_INSERT_COLUMNS = (
    "source_address",
    "source_chain",
    "dest_address",
    "dest_chain",
    "bridge_name",
    "tx_hash",
    "bridged_at",
    "confidence",
)


def ensure_bridge_mappings_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.bridge_mappings`` if it does not exist.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False otherwise.
    """
    try:
        ch_client.execute(_BRIDGE_MAPPINGS_DDL)
        logger.info("bridge_mappings_table_ensured", table=CH_BRIDGE_MAPPINGS_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("bridge_mappings_table_skip", error=str(exc)[:80])
        else:
            logger.warning("bridge_mappings_table_failed", error=str(exc)[:100])
        return False


def write_bridge_mappings(ch_client: Any, rows: list[dict[str, Any]]) -> int:
    """Insert bridge-match rows into ClickHouse (best-effort).

    Args:
        ch_client: ClickHouse client.
        rows: Match dicts with keys matching ``_INSERT_COLUMNS``.

    Returns:
        Number of rows inserted (0 on failure/unavailable).
    """
    if not rows:
        return 0
    prepared: list[tuple[Any, ...]] = []
    for row in rows:
        ts = row.get("bridged_at")
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                ts = datetime.now(UTC)
        if ts is None:
            ts = datetime.now(UTC)
        prepared.append(
            (
                row.get("source_address") or "",
                row.get("source_chain") or "",
                row.get("dest_address") or "",
                row.get("dest_chain") or "",
                row.get("bridge_name") or "",
                row.get("tx_hash") or "",
                ts,
                float(row.get("confidence") or 0.0),
            )
        )
    try:
        ch_client.execute(
            f"INSERT INTO {CH_BRIDGE_MAPPINGS_TABLE} ({', '.join(_INSERT_COLUMNS)}) VALUES",
            prepared,
        )
        logger.info("bridge_mappings_inserted", rows=len(prepared))
        return len(prepared)
    except Exception as exc:  # noqa: BLE001 — best-effort insert
        logger.warning("bridge_mappings_insert_failed", rows=len(prepared), error=str(exc)[:120])
        return 0


def find_bridge_mapping(
    ch_client: Any,
    source_address: str,
    source_chain: str,
    dest_chain: str,
    within_hours: int = 48,
) -> list[dict[str, Any]]:
    """Return previously persisted matches for a depositor on a chain.

    Used as a cache so ``find_cross_chain_path`` can reuse earlier matches
    instead of re-running the heuristic every trace.

    Args:
        ch_client: ClickHouse client.
        source_address: Depositor address on the source chain.
        source_chain: Source chain name.
        dest_chain: Destination chain name.
        within_hours: Only consider mappings written this many hours ago.

    Returns:
        List of mapping row dicts (empty when none / unavailable).
    """
    try:
        rows = ch_client.execute(
            f"SELECT {', '.join(_INSERT_COLUMNS)} FROM {CH_BRIDGE_MAPPINGS_TABLE} "
            "WHERE source_address = %(a)s AND source_chain = %(sc)s AND dest_chain = %(dc)s "
            "AND bridged_at > now() - INTERVAL %(h)s HOUR "
            "ORDER BY bridged_at DESC LIMIT 20",
            {"a": source_address, "sc": source_chain, "dc": dest_chain, "h": int(within_hours)},
        )
        return [dict(zip(_INSERT_COLUMNS, row, strict=True)) for row in rows]
    except Exception as exc:  # noqa: BLE001 — cache miss is not fatal
        logger.debug("bridge_mappings_read_failed", error=str(exc)[:80])
        return []
