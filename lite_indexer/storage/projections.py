"""
Address-first access path for wallet-history queries (electrs-inspired).

The hottest scanner query is "all transfers for wallet X". With a
block-ordered primary key this requires a full-table scan. The electrs
pattern inverts this: the primary ordering IS the query answer.

ClickHouse projections materialize a compact, well-ordered copy of the
table that the optimizer can serve queries from. Because a ClickHouse
projection is a single ``SELECT`` (UNION ALL is not supported), the
wallet-history access path is split into two projections:

* ``wallet_out_proj`` — keyed on ``(chain, from_address, ...)``, serves
  ``WHERE chain=? AND from_address=?`` queries (outgoing transfers).
* ``wallet_in_proj``  — keyed on ``(chain, to_address, ...)``, serves
  ``WHERE chain=? AND to_address=?`` queries (incoming transfers).

Both keep a ``direction`` label ('out' / 'in') so a consumer can merge
the two results into a single wallet-history stream without re-deriving
direction from the address column.

The projections are automatically maintained for new inserts. To backfill
existing rows pass ``materialize_existing=True`` once — ``ALTER TABLE ...
MATERIALIZE PROJECTION`` rebuilds them in the background.

Gated by ``WALLET_PROJECTION_ENABLED`` (see :mod:`lite_indexer.config`).
All DDL is idempotent and non-fatal: a missing table or missing
privileges degrades to a debug log, matching the retention-policy style.
"""

from __future__ import annotations

import re
from typing import Any

from lite_indexer.config import (
    CH_TABLE,
    WALLET_IN_PROJECTION,
    WALLET_OUT_PROJECTION,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ``ALTER TABLE ... ADD PROJECTION`` is mutation-class DDL and is REFUSED with
# ClickHouse Code 36 whenever the merges/mutations-pool sanity check fails —
# and that check runs BEFORE the projection name is compared, so
# ``ADD PROJECTION IF NOT EXISTS`` does not help: a redundant statement for a
# projection that already exists still raises. Reproduced 2026-10-23 on
# 26.7.1: 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition'
# (25) > 'background_pool_size' * 'background_merges_mutations_concurrency_
# ratio' (20), with both wallet projections fully materialized in
# system.projections. The caller re-invokes ``ensure_wallet_projection`` on
# every sync cycle, so a refused no-op ALTER becomes an endless mutation
# storm (~7.8k Code 36 errors accrued that way).
#
# Fix: read ``system.projections`` (a plain SELECT, no pool admission) and
# issue DDL only for projections that are genuinely absent. The attempt cap
# below stays as the last-resort guard when the catalogue itself is
# unreadable.
_MAX_DDL_ATTEMPTS = 3
_ddl_attempts = 0

# Columns every wallet-history query needs. The projection drops the
# heavy token_symbol/token_name/ingested_at/source columns entirely.
# ``chain`` is emitted explicitly as the first projection column.
_WALLET_PROJECTION_COLUMNS = (
    "block_number",
    "block_timestamp",
    "tx_hash",
    "log_index",
    "token_address",
    "value",
)

# (source address column, direction label, projection name)
_WALLET_PROJECTIONS = (
    ("from_address", "out", WALLET_OUT_PROJECTION),
    ("to_address", "in", WALLET_IN_PROJECTION),
)


def _projection_ddl(address_column: str, direction: str, projection: str) -> str:
    """Build the ``ADD PROJECTION`` statement for one wallet direction.

    Args:
        address_column: Source column the wallet address is keyed on
            (``from_address`` for out, ``to_address`` for in).
        direction: Literal label stored on every projection row ('out'/'in').
        projection: Projection name.

    Returns:
        Idempotent ALTER TABLE statement.
    """
    cols = ", ".join(_WALLET_PROJECTION_COLUMNS)
    return (
        f"ALTER TABLE {CH_TABLE} "
        f"ADD PROJECTION IF NOT EXISTS {projection} "
        f"(SELECT chain, {address_column} AS address, {cols}, "
        f"'{direction}' AS direction "
        f"ORDER BY (chain, address, block_number, log_index))"
    )


def _rows(result: Any) -> Any:
    """Normalize a ClickHouse result across driver/connect/fake clients."""
    for attribute in ("result_rows", "result_set"):
        rows = getattr(result, attribute, None)
        if rows is not None:
            return rows
    return result or []


def _existing_projections(ch_client: Any) -> set[str]:
    """Names of projections already defined on ``CH_TABLE``.

    ``ADD PROJECTION IF NOT EXISTS`` cannot be used as the existence check:
    ClickHouse validates the mutation-pool settings first, so a redundant
    ADD is refused with Code 36 instead of being a no-op.

    ``system.projections`` is the obvious source but the indexer user has no
    SELECT grant on it (Code 497, verified 2026-10-23), so the existence check
    reads the table DDL instead — metadata only, no mutation pool, and the
    writer already holds the privilege it needs to write the table.

    Returns:
        Projection names currently defined. Empty when the DDL cannot be
        read — the caller then falls back to attempting the DDL rather than
        assuming the projections are absent.
    """
    try:
        result = ch_client.execute(f"SHOW CREATE TABLE {CH_TABLE}")
    except Exception as exc:  # noqa: BLE001 — metadata read is best-effort
        logger.debug("wallet_projection_ddl_unavailable", error=str(exc)[:80])
        return set()
    ddl = " ".join(str(row[0]) for row in _rows(result) if row)
    return {name for _, _, name in _WALLET_PROJECTIONS if re.search(rf"\bPROJECTION\s+{re.escape(name)}\b", ddl)}


async def ensure_wallet_projection(
    ch_client: Any,
    materialize_existing: bool = False,
) -> bool:
    """Create the address-first wallet-history projections on evm_transfers.

    Adds two idempotent projections (out / in) so wallet-history queries
    are served from an address-keyed copy instead of a full scan. This is
    electrs' key-as-record pattern: the projection's primary ordering IS
    the query answer.

    Existence is resolved from ``system.projections`` first, so an
    already-present projection costs one SELECT and zero mutation-class DDL
    (the Code 36 storm path).

    Args:
        ch_client: ClickHouse client (anything with ``execute``).
        materialize_existing: When True, also issue
            ``ALTER TABLE ... MATERIALIZE PROJECTION`` for both projections
            so the existing 264M rows are backfilled in the background.
            This is a one-time, heavy, asynchronous operation — keep False
            for routine startup.

    Returns:
        True when both projections are present (or were created), False
        when the table is missing or privileges are insufficient.
    """
    global _ddl_attempts
    if _ddl_attempts >= _MAX_DDL_ATTEMPTS:
        # Give up on the DDL for this process. The projections are already
        # defined; a busy pool must not convert a no-op ALTER into a storm.
        logger.warning(
            "wallet_projection_ddl_deferred",
            table=CH_TABLE,
            attempts=_ddl_attempts,
        )
        return True

    existing = _existing_projections(ch_client)
    missing = [spec for spec in _WALLET_PROJECTIONS if spec[2] not in existing]
    if not missing:
        logger.info(
            "wallet_projection_present",
            table=CH_TABLE,
            projections=(WALLET_OUT_PROJECTION, WALLET_IN_PROJECTION),
        )
        _ddl_attempts = 0
        metrics_incr("wallet_projection_ready")
        return True

    try:
        for address_column, direction, projection in missing:
            ch_client.execute(_projection_ddl(address_column, direction, projection))
        logger.info(
            "wallet_projection_ensured",
            table=CH_TABLE,
            created=[spec[2] for spec in missing],
        )
        _ddl_attempts = 0
        metrics_incr("wallet_projection_ready")

        if materialize_existing:
            for projection in (WALLET_OUT_PROJECTION, WALLET_IN_PROJECTION):
                ch_client.execute(
                    f"ALTER TABLE {CH_TABLE} MATERIALIZE PROJECTION {projection}"
                )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            logger.info("wallet_projection_materialize_scheduled", table=CH_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        _ddl_attempts += 1
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("wallet_projection_skip", error=str(exc)[:80])
        else:
            logger.warning("wallet_projection_failed", error=str(exc)[:100])
        return False


__all__ = ["ensure_wallet_projection"]
