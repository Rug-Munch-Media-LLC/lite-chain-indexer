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
  ``wallet_memory.holder_balances_mv``).

Both are best-effort: transient failures log at debug level and the next
batch retries. Balance values are Int128 — tokens whose raw unit count
exceeds 2**127 - 1 (absurd, >1e38 smallest units) are skipped by the
manual cache and logged; the auto MV follows the same conversion.
"""

from __future__ import annotations

import asyncio
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
# sums the NET balance (credit to_address, debit from_address) and the
# transfer count per (chain, token_address, address) on merge.
#
# 2026-09-02 fix: the previous DDL was invalid ClickHouse SQL — it selected
# the non-aggregated ``value`` column alongside ``count()`` (error 215,
# "Column ... is not under aggregate function"), so the view could NEVER be
# created. It also only credited ``to_address``, overstating balances by
# ignoring the debit side. The rewritten view expands each transfer into
# two rows via ``arrayJoin`` (credit / debit) and aggregates with ``sum``.
#
# ClickHouse auto-creates the inner storage table
# (stored in the view itself) — query it via get_auto_holders.
_HOLDER_BALANCES_MV_DDL = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {CH_HOLDER_BALANCES_MV}
ENGINE = SummingMergeTree()
ORDER BY (chain, token_address, address)
AS SELECT
    chain,
    token_address,
    address,
    sum(delta) AS balance,
    count() AS transfer_count
FROM
(
    SELECT
        chain,
        token_address,
        address,
        if(address = to_address, toInt128OrZero(value), -toInt128OrZero(value)) AS delta
    FROM
    (
        SELECT
            chain,
            token_address,
            from_address,
            to_address,
            value,
            arrayJoin([from_address, to_address]) AS address
        FROM wallet_memory.evm_transfers
        WHERE (from_address != '') AND (to_address != '')
    )
)
GROUP BY chain, token_address, address
"""

# Where the auto MV stores its data. ClickHouse 26.x keeps the MV's inner
# storage under the view itself — ``wallet_memory.inner.<view>`` no longer
# parses (``inner`` is a keyword and the hidden inner table is gone), so
# query the view directly (it acts as its own storage).
_HOLDER_BALANCES_MV_INNER = CH_HOLDER_BALANCES_MV

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
        # clickhouse_driver is a SYNCHRONOUS client — running it inline
        # blocks the asyncio event loop (this exact call froze the whole
        # daemon for 1.5h on 2026-08-03 when ClickHouse was restarting).
        # to_thread keeps the loop responsive for the other 13 chains.
        current = await asyncio.to_thread(
            ch_client.execute,
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
        await asyncio.to_thread(
            ch_client.execute,
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


async def update_holder_balances_bulk(
    ch_client: Any,
    deltas: dict[tuple[str, str, str], int],
) -> int:
    """Apply many holder deltas in TWO ClickHouse round-trips.

    The per-holder :func:`update_holder_balance` does one SELECT + one
    INSERT per holder — fine for live batches, but a deep catch-up batch
    (200K+ transfers → 100K+ unique holders) needs 200K+ sequential
    round-trips and deadlocks the sync cycle (observed 2026-09-02: zero
    EVM sync_completes after the balance cache was re-enabled, every
    cycle grinding 15+ min in wallet features). This bulk path reads all
    current balances for the batch's keys in ONE query and writes all
    updated rows in ONE insert.

    Args:
        ch_client: ClickHouse client.
        deltas: ``{(chain, token_address, address): signed_delta}`` —
            zero deltas are skipped.

    Returns:
        Number of holder rows written (0 when nothing to do / failure).
    """
    if not deltas:
        return 0

    active = {k: v for k, v in deltas.items() if v}
    if not active:
        return 0

    try:
        # One SELECT for all current balances (chunked IN clause).
        keys = list(active.keys())
        current: dict[tuple[str, str, str], tuple[int, int]] = {}
        CHUNK = 10_000
        for i in range(0, len(keys), CHUNK):
            chunk = keys[i : i + CHUNK]
            conditions = ", ".join("(%(chain)s, %(token)s, %(address)s)" for _ in chunk)
            params: dict[str, str] = {}
            cond_parts = []
            for j, (chain, token, address) in enumerate(chunk):
                cond_parts.append(f"({{p{j}c}}, {{p{j}t}}, {{p{j}a}})")
                params[f"p{j}c"] = chain
                params[f"p{j}t"] = token
                params[f"p{j}a"] = address
            rows = await asyncio.to_thread(
                ch_client.execute,
                f"SELECT chain, token_address, address, balance, transfer_count "
                f"FROM {CH_HOLDER_BALANCES_TABLE} WHERE (chain, token_address, address) IN "
                f"({', '.join(cond_parts)})",
                params,
            )
            for r in rows:
                current[(r[0], r[1], r[2])] = (int(r[3]), int(r[4]))

        version = int(datetime.now(UTC).timestamp() * 1_000_000)
        now = datetime.now(UTC)
        rows_to_insert: list[tuple[Any, ...]] = []
        for key, delta in active.items():
            chain, token, address = key
            balance, transfer_count = current.get(key, (0, 0))
            new_balance = balance + delta
            if new_balance > _INT128_MAX or new_balance < -_INT128_MAX:
                logger.warning(
                    "balance_cache_overflow_skipped",
                    chain=chain,
                    token=token[:12],
                    address=address[:12],
                )
                continue
            rows_to_insert.append((chain, token, address, new_balance, transfer_count + 1, now, version))
        if not rows_to_insert:
            return 0
        for i in range(0, len(rows_to_insert), CHUNK):
            await asyncio.to_thread(
                ch_client.execute,
                f"INSERT INTO {CH_HOLDER_BALANCES_TABLE} "
                f"(chain, token_address, address, balance, transfer_count, last_updated, version) VALUES",
                rows_to_insert[i : i + CHUNK],
            )
        metrics_incr("balance_cache_updates", len(rows_to_insert))
        return len(rows_to_insert)
    except Exception as exc:  # noqa: BLE001 — cache write is best-effort
        logger.debug("balance_cache_bulk_failed", error=str(exc)[:100])
        return 0


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
    ``INSERT INTO wallet_memory.holder_balances_mv
     SELECT chain, token_address, address, sum(delta) AS balance,
            count() AS transfer_count FROM (…same expansion as the view…)
     GROUP BY chain, token_address, address`` once.

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

    Reads the auto-maintained MV (ClickHouse 26.x stores the view's data
    in the view itself). Useful when the manual lazy cache is disabled and
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
    "update_holder_balances_bulk",
    "get_token_holders",
    "ensure_holder_balances_mv",
    "get_auto_holders",
]
