"""
Per-token incremental balance cache (electrs lazy cache) + auto MV.

electrs maintains lazy per-address UTXO caches so wallet queries never
replay the full chain. This module does the same for ERC-20 holder
balances — the answer is pre-computed and incrementally maintained
instead of aggregating the 264M-row ``evm_transfers`` table per query.

Two independently-usable implementations:

* **Manual lazy cache** (Feature 3, ``BALANCE_CACHE_ENABLED``):
  :func:`update_holder_balance` read-modify-writes
  ``wallet_memory.holder_balances`` (ReplacingMergeTree, so the latest
  computed balance wins). The indexer calls it once per unique holder
  delta per batch. :func:`get_token_holders` serves top-holder queries
  from the cache.
* **Auto-maintained materialized view** (Feature 4,
  ``HOLDER_BALANCES_MV_ENABLED``): :func:`ensure_holder_balances_mv`
  creates a SummingMergeTree view fed by ``evm_transfers`` inserts, so
  ClickHouse maintains balances itself. Query it with
  :func:`get_auto_holders` (reads the view's inner table
  ``wallet_memory.inner.holder_balances_mv``).

Both are best-effort: transient failures log at debug level and the next
batch retries. Balance values are Int128 — tokens whose raw unit count
exceeds 2**127 - 1 (absurd, >1e38 smallest units) are skipped by the
manual cache and logged; the auto MV follows the same conversion.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import CH_HOLDER_BALANCES_MV, CH_HOLDER_BALANCES_TABLE
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

# Manual cache: one row per (chain, token, holder). ReplacingMergeTree
# keeps only the row with the highest ``version`` for each key, so every
# insert carries the newly-computed cumulative balance (not a delta).
_HOLDER_BALANCES_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_HOLDER_BALANCES_TABLE} (
    chain LowCardinality(String),
    token_address String,
    address String,
    balance Int128,
    transfer_count UInt64 DEFAULT 0,
    last_updated DateTime DEFAULT now(),
    version UInt64 DEFAULT 0
) ENGINE = ReplacingMergeTree(version)
ORDER BY (chain, token_address, address)
"""

# Auto-maintained view: fed by every evm_transfers insert. SummingMergeTree
# sums balance/transfer_count per (chain, token_address, address) on merge.
# ClickHouse auto-creates the inner storage table
# (wallet_memory.inner.holder_balances_mv) — query it via get_auto_holders.
_HOLDER_BALANCES_MV_DDL = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {CH_HOLDER_BALANCES_MV}
ENGINE = SummingMergeTree()
ORDER BY (chain, token_address, address)
AS SELECT
    chain,
    token_address,
    to_address AS address,
    toInt128(value) AS balance,
    count() AS transfer_count
FROM wallet_memory.evm_transfers
GROUP BY chain, token_address, address
"""

# Where the auto MV stores its data (ClickHouse auto-inner-table naming).
_HOLDER_BALANCES_MV_INNER = f"wallet_memory.inner.{CH_HOLDER_BALANCES_MV.split('.')[-1]}"

# Guard against Int128 overflow (absurd token supplies).
_INT128_MAX = 2**127 - 1


async def ensure_balance_cache_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.holder_balances`` if it does not exist.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False when the
        database is missing or privileges are insufficient.
    """
    try:
        ch_client.execute(_HOLDER_BALANCES_DDL)
        logger.info("balance_cache_table_ensured", table=CH_HOLDER_BALANCES_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("balance_cache_table_skip", error=str(exc)[:80])
        else:
            logger.warning("balance_cache_table_failed", error=str(exc)[:100])
        return False


async def update_holder_balance(
    ch_client: Any,
    chain: str,
    token: str,
    address: str,
    delta: int,
) -> bool:
    """Incrementally update a holder's balance in the lazy cache.

    Reads the current cached balance, applies ``delta``, and inserts the
    new cumulative value with a monotonic version so ReplacingMergeTree
    keeps the latest. Called per transfer (deltas are accumulated per
    holder per batch by the caller, so this is at most one
    read-modify-write per holder per batch).

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        token: Token contract address.
        address: Holder address.
        delta: Signed balance delta (positive for incoming transfers,
            negative for outgoing). A delta of 0 is a no-op.

    Returns:
        True when the cache was updated, False when skipped (zero delta,
        Int128 overflow, or transient failure).
    """
    if delta == 0:
        return False

    try:
        current = ch_client.execute(
            f"SELECT balance, transfer_count FROM {CH_HOLDER_BALANCES_TABLE} "
            f"WHERE chain = %(chain)s AND token_address = %(token)s AND address = %(address)s "
            f"LIMIT 1",
            {"chain": chain, "token": token, "address": address},
        )
    except Exception as exc:  # noqa: BLE001 — cache read is best-effort
        logger.debug(
            "balance_cache_read_failed",
            chain=chain,
            token=token[:12],
            address=address[:12],
            error=str(exc)[:80],
        )
        return False

    balance = int(current[0][0]) if current else 0
    transfer_count = int(current[0][1]) if current else 0

    new_balance = balance + delta
    if new_balance > _INT128_MAX or new_balance < -_INT128_MAX:
        logger.warning(
            "balance_cache_overflow_skipped",
            chain=chain,
            token=token[:12],
            address=address[:12],
        )
        return False

    try:
        version = int(datetime.now(UTC).timestamp() * 1_000_000)
        ch_client.execute(
            f"INSERT INTO {CH_HOLDER_BALANCES_TABLE} "
            f"(chain, token_address, address, balance, transfer_count, last_updated, version) VALUES",
            [
                (
                    chain,
                    token,
                    address,
                    new_balance,
                    transfer_count + 1,
                    datetime.now(UTC),
                    version,
                )
            ],
        )
        metrics_incr("balance_cache_updates")
        return True
    except Exception as exc:  # noqa: BLE001 — cache write is best-effort
        logger.debug(
            "balance_cache_update_failed",
            chain=chain,
            token=token[:12],
            address=address[:12],
            error=str(exc)[:80],
        )
        return False


async def get_token_holders(
    ch_client: Any,
    chain: str,
    token: str,
    min_balance: int = 0,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get top token holders from the incremental cache.

    Serves holder queries from the pre-computed cache — no aggregation
    over ``evm_transfers``.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        token: Token contract address.
        min_balance: Minimum balance filter (raw units).
        limit: Max rows (default 100).

    Returns:
        List of ``{"address", "balance", "transfer_count"}`` dicts ordered
        by balance descending.
    """
    try:
        rows = ch_client.execute(
            f"SELECT address, balance, transfer_count FROM {CH_HOLDER_BALANCES_TABLE} "
            f"WHERE chain = %(chain)s AND token_address = %(token)s AND balance >= %(min_balance)s "
            f"ORDER BY balance DESC LIMIT %(limit)s",
            {"chain": chain, "token": token, "min_balance": min_balance, "limit": limit},
        )
    except Exception as exc:  # noqa: BLE001 — query is best-effort
        logger.debug(
            "balance_cache_query_failed",
            chain=chain,
            token=token[:12],
            error=str(exc)[:80],
        )
        return []

    return [{"address": r[0], "balance": r[1], "transfer_count": r[2]} for r in rows]


async def ensure_holder_balances_mv(ch_client: Any) -> bool:
    """Create the auto-maintained holder-balances materialized view.

    The view is fed by every ``evm_transfers`` insert and keeps a
    pre-aggregated SummingMergeTree of balances per
    ``(chain, token_address, address)``. Existing rows are NOT backfilled
    automatically — to backfill, run
    ``INSERT INTO wallet_memory.inner.holder_balances_mv
     SELECT ... FROM wallet_memory.evm_transfers GROUP BY ...`` once.

    Args:
        ch_client: ClickHouse client.

    Returns:
        True when the view exists (or was created), False otherwise.
    """
    try:
        ch_client.execute(_HOLDER_BALANCES_MV_DDL)
        logger.info("holder_balances_mv_ensured", view=CH_HOLDER_BALANCES_MV)
        metrics_incr("holder_balances_mv_ready")
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("holder_balances_mv_skip", error=str(exc)[:80])
        else:
            logger.warning("holder_balances_mv_failed", error=str(exc)[:100])
        return False


async def get_auto_holders(
    ch_client: Any,
    chain: str,
    token: str,
    min_balance: int = 0,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Get top token holders from the auto-maintained materialized view.

    Reads ``wallet_memory.inner.holder_balances_mv`` (the inner table the
    view writes to). Useful when the manual lazy cache is disabled and
    only the MV is enabled.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        token: Token contract address.
        min_balance: Minimum balance filter (raw units).
        limit: Max rows (default 100).

    Returns:
        List of ``{"address", "balance", "transfer_count"}`` dicts ordered
        by balance descending.
    """
    try:
        rows = ch_client.execute(
            f"SELECT address, balance, transfer_count FROM {_HOLDER_BALANCES_MV_INNER} "
            f"WHERE chain = %(chain)s AND token_address = %(token)s AND balance >= %(min_balance)s "
            f"ORDER BY balance DESC LIMIT %(limit)s",
            {"chain": chain, "token": token, "min_balance": min_balance, "limit": limit},
        )
    except Exception as exc:  # noqa: BLE001 — query is best-effort
        logger.debug(
            "auto_holders_query_failed",
            chain=chain,
            token=token[:12],
            error=str(exc)[:80],
        )
        return []

    return [{"address": r[0], "balance": r[1], "transfer_count": r[2]} for r in rows]


__all__ = [
    "ensure_balance_cache_table",
    "update_holder_balance",
    "get_token_holders",
    "ensure_holder_balances_mv",
    "get_auto_holders",
]
