"""
Balance reconciliation (TrueBlocks-style).

Compares computed balances derived from indexed ``evm_transfers`` deltas
against on-chain state queried via ``eth_getBalance``. This is a
*data-completeness heuristic*: if the indexer captured every transfer the
delta between computed and on-chain state should trend to zero; a large
gap indicates missing or incorrect indexed rows (e.g. a partial backfill,
a reorg that was not rolled back, or an untracked path such as native
transfers / fee payments).

The reconciliation is deliberately lightweight and non-blocking:

* Only runs on a random sample of wallets (default 100 per chain) — see
  :func:`run_reconciliation`.
* Results are cached in ``wallet_memory.reconciliation_results`` so each
  run is append-only and auditable.
* Gated by ``RECONCILIATION_ENABLED`` (default on) at the
  :func:`run_reconciliation` entry point.

Also provides :func:`check_range_completeness` — a periodic durable-store
row-count check (the indexer compares the ClickHouse row count for a just
synced range against the events fetched from RPC and backfills when the
durable store is short by more than the configured percentage).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from lite_indexer.chains import GET_LOGS_ENDPOINTS
from lite_indexer.config import (
    CH_RECONCILIATION_TABLE,
    CH_TABLE,
    RECONCILIATION_ENABLED,
    RECONCILIATION_SAMPLE_SIZE,
    SOLANA_CH_TABLE,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.rpc import get_best_endpoint

logger = get_logger(__name__)

# One row per (chain, address, checked_at). MergeTree keeps every sample
# so historical reconciliation trends are queryable.
_RECONCILIATION_DDL = f"""
CREATE TABLE IF NOT EXISTS {CH_RECONCILIATION_TABLE} (
    chain LowCardinality(String),
    address String,
    checked_at DateTime,
    computed_balance Int128,
    on_chain_balance Int128,
    delta Int128,
    delta_pct Float64,
    status LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (chain, address, checked_at)
"""

# Max concurrent eth_getBalance RPC calls (keeps sampling gentle).
_RECONCILIATION_CONCURRENCY = 5


async def ensure_reconciliation_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.reconciliation_results`` if missing.

    Idempotent and non-fatal: a missing database or lacking privileges
    degrades to a debug log (the caller treats it as "caching skipped").

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False otherwise.
    """
    try:
        ch_client.execute(_RECONCILIATION_DDL)  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
        logger.info("reconciliation_table_ensured", table=CH_RECONCILIATION_TABLE)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("reconciliation_table_skip", error=str(exc)[:80])
        else:
            logger.warning("reconciliation_table_failed", error=str(exc)[:100])
        return False


def select_random_wallets(
    ch_client: Any,
    chain: str,
    limit: int = RECONCILIATION_SAMPLE_SIZE,
) -> list[str]:
    """Pick a random sample of wallets that appear in ``evm_transfers``.

    Samples distinct senders and receivers (``ORDER BY rand()``) so the
    sample is cheap and unbiased. Returns ``[]`` when ClickHouse is
    unreachable or the table is empty.

    Args:
        ch_client: ClickHouse client.
        chain: Chain name.
        limit: Max wallets to sample (default: 100).

    Returns:
        List of lowercase wallet addresses.
    """
    if limit <= 0:
        return []
    try:
        rows = ch_client.execute(
            f"SELECT address FROM ("
            f"  SELECT to_address AS address FROM {CH_TABLE} "
            f"  WHERE chain = %(chain)s AND to_address != '' "
            f"  GROUP BY to_address"
            f"  UNION ALL"
            f"  SELECT from_address AS address FROM {CH_TABLE} "
            f"  WHERE chain = %(chain)s AND from_address != '' "
            f"  GROUP BY from_address"
            f") GROUP BY address ORDER BY rand() LIMIT %(limit)s",
            {"chain": chain, "limit": limit},
        )
    except Exception as exc:  # noqa: BLE001 — sampling is best-effort
        logger.debug(
            "reconciliation_sample_failed",
            chain=chain,
            error=str(exc)[:80],
        )
        return []
    return [str(r[0]).lower() for r in rows]


async def _fetch_balance(
    rpc_client: Any,
    endpoint: str,
    wallet: str,
) -> int | None:
    """Query ``eth_getBalance`` at latest block via RPC.

    Args:
        rpc_client: RPC client with a ``call`` method (RpcClient or fake).
        endpoint: RPC endpoint URL.
        wallet: Lowercase wallet address.

    Returns:
        Balance in wei (int), or None on any RPC failure.
    """
    try:
        result = await rpc_client.call(endpoint, "eth_getBalance", [wallet, "latest"])
    except Exception as exc:  # noqa: BLE001 — RPC is best-effort
        logger.debug("reconciliation_rpc_failed", wallet=wallet[:12], error=str(exc)[:80])
        return None
    if not result or "result" not in result:
        return None
    try:
        return int(str(result["result"]), 16)
    except (ValueError, TypeError) as exc:
        logger.debug("reconciliation_rpc_non_hex", wallet=wallet[:12], error=str(exc)[:40])
        return None


async def _cache_results(
    ch_client: Any,
    chain: str,
    results: dict[str, dict[str, Any]],
) -> int:
    """Insert reconciliation results into ClickHouse (best-effort)."""
    if not results:
        return 0
    checked_at = datetime.now(UTC)
    rows = [
        (
            chain,
            wallet,
            checked_at,
            info["computed_balance"],
            info["on_chain_balance"],
            info["delta"],
            info["delta_pct"],
            info["status"],
        )
        for wallet, info in results.items()
    ]
    try:
        ch_client.execute(
            f"INSERT INTO {CH_RECONCILIATION_TABLE} "
            "(chain, address, checked_at, computed_balance, on_chain_balance, "
            "delta, delta_pct, status) VALUES",
            rows,
        )
        metrics_incr("reconciliation_results_cached", len(rows))
        return len(rows)
    except Exception as exc:  # noqa: BLE001 — caching is best-effort
        logger.warning(
            "reconciliation_cache_failed",
            chain=chain,
            rows=len(rows),
            error=str(exc)[:100],
        )
        return 0


def _classify(delta: int, on_chain: int) -> tuple[str, float]:
    """Classify a reconciliation result.

    Args:
        delta: computed - on_chain.
        on_chain: On-chain balance.

    Returns:
        (status, delta_pct) where delta_pct is the relative gap in
        percent (0 when on-chain balance is 0).
    """
    # Guard against divide-by-zero: a zero on-chain balance with a nonzero
    # delta is an absolute mismatch (100%).
    denominator = max(abs(on_chain), 1)
    delta_pct = abs(delta) / denominator * 100.0

    if delta == 0:
        status = "MATCH"
    elif delta_pct < 1.0:
        status = "APPROXIMATE"
    else:
        status = "MISMATCH"
    return status, delta_pct


async def verify_transfer_completeness(
    chain: str,
    sample_wallets: list[str],
    rpc_client: Any,
    ch_client: Any,
) -> dict[str, dict[str, Any]]:
    """TrueBlocks-style reconciliation for a sample of wallets.

    For each wallet:

    1. Compute the running balance from indexed ``evm_transfers`` deltas:
       incoming minus outgoing (both via ``sum(toInt128(value))``).
    2. Query ``eth_getBalance`` at latest block via RPC.
    3. Compare: ``delta == 0`` → ``MATCH``; relative gap < 1% →
       ``APPROXIMATE``; otherwise ``MISMATCH`` (with the gap).
    4. Cache the result row in ``wallet_memory.reconciliation_results``.

    Best-effort and non-blocking: RPC or ClickHouse failures for a single
    wallet skip that wallet rather than failing the whole sample.

    Args:
        chain: Chain name.
        sample_wallets: Wallet addresses to check (lowercase preferred).
        rpc_client: RPC client (RpcClient or fake with ``call``).
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        ``{wallet: {computed_balance, on_chain_balance, delta, delta_pct,
        status}}`` for every wallet that was successfully checked.
    """
    if not sample_wallets:
        return {}

    endpoints = GET_LOGS_ENDPOINTS.get(chain, [])
    if not endpoints:
        logger.warning("reconciliation_no_endpoints", chain=chain)
        return {}
    endpoint = get_best_endpoint(endpoints)

    wallets = [w.lower() for w in sample_wallets]
    results: dict[str, dict[str, Any]] = {}

    async def _check(wallet: str) -> tuple[str, dict[str, Any]] | None:
        try:
            incoming_rows = ch_client.execute(
                f"SELECT sum(toInt128(value)) FROM {CH_TABLE} "
                "WHERE chain = %(chain)s AND to_address = %(wallet)s GROUP BY to_address",
                {"chain": chain, "wallet": wallet},
            )
            outgoing_rows = ch_client.execute(
                f"SELECT sum(toInt128(value)) FROM {CH_TABLE} "
                "WHERE chain = %(chain)s AND from_address = %(wallet)s GROUP BY from_address",
                {"chain": chain, "wallet": wallet},
            )
        except Exception as exc:  # noqa: BLE001 — per-wallet best-effort
            logger.debug(
                "reconciliation_query_failed",
                chain=chain,
                wallet=wallet[:12],
                error=str(exc)[:80],
            )
            return None

        incoming = int(incoming_rows[0][0]) if incoming_rows else 0
        outgoing = int(outgoing_rows[0][0]) if outgoing_rows else 0
        computed_balance = incoming - outgoing

        on_chain_balance = await _fetch_balance(rpc_client, endpoint, wallet)
        if on_chain_balance is None:
            logger.debug("reconciliation_balance_skipped", wallet=wallet[:12])
            return None

        delta = computed_balance - on_chain_balance
        status, delta_pct = _classify(delta, on_chain_balance)

        info = {
            "computed_balance": computed_balance,
            "on_chain_balance": on_chain_balance,
            "delta": delta,
            "delta_pct": round(delta_pct, 4),
            "status": status,
        }
        return wallet, info

    semaphore = asyncio.Semaphore(_RECONCILIATION_CONCURRENCY)

    async def _check_limited(wallet: str) -> tuple[str, dict[str, Any]] | None:
        async with semaphore:
            return await _check(wallet)

    outcomes = await asyncio.gather(
        *(_check_limited(w) for w in wallets),
        return_exceptions=True,
    )
    for outcome in outcomes:
        if isinstance(outcome, tuple):
            wallet, info = outcome
            results[wallet] = info

    # Cache results (append-only; never blocks the caller on failure).
    await _cache_results(ch_client, chain, results)

    logger.info(
        "reconciliation_complete",
        chain=chain,
        checked=len(results),
        sample=len(wallets),
        matches=sum(1 for i in results.values() if i["status"] == "MATCH"),
        mismatches=sum(1 for i in results.values() if i["status"] == "MISMATCH"),
    )
    return results


def range_table_for(chain: str, chain_type: str = "evm") -> tuple[str, str]:
    """Return (table, block_column) for a chain's range completeness check.

    EVM + Tron write to ``evm_transfers`` keyed by a ``chain`` column;
    Solana writes to ``solana_spl_balance_changes`` (slot-based, no chain
    column — the table is solana-only).

    Args:
        chain: Chain name (unused for the table lookup but kept for a
            future per-chain table registry).
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.

    Returns:
        ``(table, column)`` tuple.
    """
    if chain_type == "solana":
        return SOLANA_CH_TABLE, "slot"
    return CH_TABLE, "block_number"


def check_range_completeness(
    chain: str,
    ch_client: Any,
    from_block: int,
    to_block: int,
    expected: int,
    chain_type: str = "evm",
) -> dict[str, Any]:
    """Compare ClickHouse row count for a range against the expected count.

    Counts the rows ClickHouse actually holds for ``[from_block,
    to_block]`` for this chain and returns the discrepancy. The indexer
    calls this every ``DATA_RECONCILIATION_EVERY_N_CYCLES`` cycles and
    backfills the range when the durable store is short by more than
    ``DATA_RECONCILIATION_MAX_DISCREPANCY_PCT`` percent.

    Best-effort: ClickHouse unavailable or a query failure returns a dict
    with ``ok=False`` and never raises.

    Args:
        chain: Chain name.
        ch_client: ClickHouse client (anything with ``execute``).
        from_block: Start of the synced range (inclusive).
        to_block: End of the synced range (inclusive).
        expected: Events fetched from RPC for the range.
        chain_type: ``"evm"`` / ``"solana"`` / ``"tron"``.

    Returns:
        ``{ok, expected, actual, discrepancy_pct}`` where ``ok`` is True
        when the check ran and the discrepancy is within tolerance, False
        when the durable store is short, and None-ish (ok=False) when the
        check could not run (CH unavailable / query failure).
    """
    result: dict[str, Any] = {
        "ok": False,
        "expected": expected,
        "actual": 0,
        "discrepancy_pct": 0.0,
        "error": "",
    }
    if expected <= 0:
        result["ok"] = True
        return result
    if ch_client is None:
        result["error"] = "clickhouse_unavailable"
        return result

    table, column = range_table_for(chain, chain_type)
    try:
        if chain_type == "solana":
            rows = ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                f"SELECT count() FROM {table} WHERE {column} BETWEEN %(f)s AND %(t)s",
                {"f": from_block, "t": to_block},
            )
        else:
            rows = ch_client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                f"SELECT count() FROM {table} WHERE chain = %(chain)s AND {{column}} BETWEEN %(f)s AND %(t)s".format(
                    column=column
                ),
                {"chain": chain, "f": from_block, "t": to_block},
            )
        actual = int(rows[0][0]) if rows and rows[0] else 0
    except Exception as exc:  # noqa: BLE001 — best-effort check
        result["error"] = str(exc)[:100]
        logger.debug("reconcile_count_failed", chain=chain, error=result["error"])
        return result

    result["actual"] = actual
    if actual >= expected:
        result["ok"] = True
        return result
    result["discrepancy_pct"] = (expected - actual) / expected * 100.0
    # "ok" is True only when the check ran AND the durable store is not
    # significantly short. The caller decides the tolerance; here we mark
    # ok=False so a short store is clearly visible to the caller.
    result["ok"] = False
    return result


async def run_reconciliation(
    chain: str,
    rpc_client: Any,
    ch_client: Any,
    limit: int = RECONCILIATION_SAMPLE_SIZE,
) -> dict[str, dict[str, Any]]:
    """Run reconciliation on a random sample of wallets (gated entry point).

    Picks ``limit`` random wallets from ``evm_transfers`` and verifies
    their balances against on-chain state. No-ops (returns ``{}``) when
    ``RECONCILIATION_ENABLED`` is false, ClickHouse is unavailable, or the
    chain has no endpoints.

    Args:
        chain: Chain name.
        rpc_client: RPC client (RpcClient or fake with ``call``).
        ch_client: ClickHouse client.
        limit: Sample size (default: 100 wallets).

    Returns:
        ``{wallet: {computed_balance, on_chain_balance, delta, delta_pct,
        status}}`` for the sample.
    """
    if not RECONCILIATION_ENABLED:
        logger.debug("reconciliation_disabled", chain=chain)
        return {}

    if ch_client is None:
        return {}

    await ensure_reconciliation_table(ch_client)
    wallets = select_random_wallets(ch_client, chain, limit=limit)
    if not wallets:
        logger.debug("reconciliation_no_sample", chain=chain)
        return {}

    return await verify_transfer_completeness(chain, wallets, rpc_client, ch_client)


__all__ = [
    "ensure_reconciliation_table",
    "select_random_wallets",
    "verify_transfer_completeness",
    "run_reconciliation",
    "range_table_for",
    "check_range_completeness",
]
