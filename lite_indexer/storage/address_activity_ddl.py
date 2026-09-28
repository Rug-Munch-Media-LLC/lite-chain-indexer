"""DDL for ``wallet_memory.address_activity`` — the wallet-tracing keystone table.

Extracted from ``storage/address_activity.py`` (shrink-only size ratchet) so
the schema statements, the pre-migration-015 upgrade path and the idempotent
``ensure_*`` helpers live in one place. ``address_activity.py`` re-exports
every public name here, so existing importers keep working unchanged.

Schema matches the ClickHouse migrations ``014_*.sql`` +
``015_address_activity_chain.sql`` and is also ensured at runtime
(idempotent, non-fatal) so the table appears on first address-activity write
without operator DDL.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import CH_ADDRESS_ACTIVITY_TABLE, RETENTION_DAYS
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_ADDRESS_ACTIVITY_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_ADDRESS_ACTIVITY_TABLE} (
    address String CODEC(ZSTD(3)),
    tx_signature String CODEC(ZSTD(3)),
    chain String DEFAULT 'solana' CODEC(ZSTD(3)),
    slot UInt64,
    block_time DateTime,
    involved_accounts Array(String),
    program_ids Array(String),
    is_signer Bool,
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = ReplacingMergeTree()
ORDER BY (address, tx_signature, chain)
TTL block_time + INTERVAL {RETENTION_DAYS.get("address_activity", 365)} DAY
"""

# Upgrade path for a table provisioned by migration 014 (Solana-only, no
# ``chain`` column, sort key ``(address, tx_signature)``). ``CREATE TABLE
# IF NOT EXISTS`` is a no-op on an existing table, so
# :func:`ensure_address_activity_chain_column` runs these ALTERs to bring
# the live table up to the canonical schema. ``MODIFY ORDER BY`` appends
# ``chain`` to the legacy key — ClickHouse supports this because the old
# key remains a prefix of the new one, so no data rewrite is required.
_ADD_CHAIN_COLUMN_DDL = (
    f"ALTER TABLE {CH_ADDRESS_ACTIVITY_TABLE} ADD COLUMN IF NOT EXISTS chain String DEFAULT 'solana' CODEC(ZSTD(3))"
)
_MODIFY_ORDER_BY_DDL = f"ALTER TABLE {CH_ADDRESS_ACTIVITY_TABLE} MODIFY ORDER BY (address, tx_signature, chain)"

_INSERT_COLUMNS = (
    "address",
    "tx_signature",
    "slot",
    "block_time",
    "involved_accounts",
    "program_ids",
    "is_signer",
    "ingested_at",
    "chain",
)

# Fallback chain tag for rows produced before the ``chain`` column existed
# (e.g. Solana rows emitted by an older writer).
_DEFAULT_CHAIN = "solana"


def ensure_address_activity_chain_column(ch_client: Any) -> None:
    """Upgrade a pre-015 table with the ``chain`` discriminator (idempotent).

    ``CREATE TABLE IF NOT EXISTS`` is a no-op on an existing table, so a
    table provisioned by migration 014 (no ``chain`` column, sort key
    ``(address, tx_signature)``) would never gain it from the CREATE alone.
    These ALTERs bring such a table up to the canonical schema. They are
    best-effort: on a fresh database the table does not exist yet, both
    fail harmlessly, and the CREATE provisions the canonical schema.

    Kept separate from :func:`ensure_address_activity_table` so the Solana
    drain path remains a single CREATE + INSERT (migration 015 is the
    operator-facing upgrade); the cross-chain mixin calls this first so
    EVM/TRON ingestion self-heals an un-migrated table.
    """
    # Memoised 6 h: MODIFY ORDER BY rewrites the sort key of a ~29 GiB table.
    # Measured 2026-09-23: 22 executions / 3 min from the write path — the
    # single most expensive DDL in the storm that wedged the host.
    import time

    now = time.monotonic()
    if now - _ENSURE_AT.get("chain_column", 0.0) < _ENSURE_TTL_SECONDS:
        return
    for ddl in (_ADD_CHAIN_COLUMN_DDL, _MODIFY_ORDER_BY_DDL):
        try:
            ch_client.execute(ddl)
        except Exception as exc:  # noqa: BLE001 — table may be absent/fresh
            logger.debug("address_activity_chain_ddl_skip", error=str(exc)[:80])
    _ENSURE_AT["chain_column"] = time.monotonic()


def ensure_address_activity_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.address_activity`` if it does not exist.

    Provisions the canonical multi-chain schema (``chain`` discriminator +
    extended sort key). For a pre-existing Solana-only table, apply
    migration ``015_address_activity_chain.sql`` or call
    :func:`ensure_address_activity_chain_column` first.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(
            _ADDRESS_ACTIVITY_DDL
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        logger.info("address_activity_table_ensured", table=CH_ADDRESS_ACTIVITY_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("address_activity_table_skip", error=str(exc)[:80])
        else:
            logger.warning("address_activity_table_failed", error=str(exc)[:100])
        return False


_ENSURE_AT: dict[str, float] = {}
_ENSURE_TTL_SECONDS = 6 * 3600
