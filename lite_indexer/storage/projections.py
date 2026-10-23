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
existing rows (the 264M rows already in the table) pass
``materialize_existing=True`` once — ``ALTER TABLE ... MATERIALIZE
PROJECTION`` rebuilds them in the background.

Gated by ``WALLET_PROJECTION_ENABLED`` (see :mod:`lite_indexer.config`).
All DDL is idempotent and non-fatal: a missing table or missing
privileges degrades to a debug log, matching the retention-policy style.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.config import (
    CH_TABLE,
    WALLET_IN_PROJECTION,
    WALLET_OUT_PROJECTION,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# ``ALTER TABLE ... ADD PROJECTION`` is mutation-class DDL: ClickHouse admits it
# through the merges+mutations pool and refuses it with Code 36 ("no free
# entries in pool to execute mutation") while that pool is busy merging. The
# caller re-invokes ``ensure_wallet_projection`` on every sync cycle, so a busy
# pool turns one idempotent DDL into an endless mutation storm: on 2026-10-23
# both projections were already fully materialized (283.9M rows, equal to the
# base table) while ~7.8k Code 36 errors accrued, one every few seconds.
#
# The projections exist after the first successful ADD, so the retries add no
# information - only pool admission pressure. Cap the attempts; once exhausted
# this process stops asking and the sync path proceeds unblocked.
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


async def ensure_wallet_projection(
    ch_client: Any,
    materialize_existing: bool = False,
) -> bool:
    """Create the address-first wallet-history projections on evm_transfers.

    Adds two idempotent projections (out / in) so wallet-history queries
    are served from an address-keyed copy instead of a full scan. This is
    electrs' key-as-record pattern: the projection's primary ordering IS
    the query answer.

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

    try:
        statements = [
            _projection_ddl("from_address", "out", WALLET_OUT_PROJECTION),
            _projection_ddl("to_address", "in", WALLET_IN_PROJECTION),
        ]
        for stmt in statements:
            ch_client.execute(stmt)
        logger.info(
            "wallet_projection_ensured",
            table=CH_TABLE,
            projections=(WALLET_OUT_PROJECTION, WALLET_IN_PROJECTION),
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
