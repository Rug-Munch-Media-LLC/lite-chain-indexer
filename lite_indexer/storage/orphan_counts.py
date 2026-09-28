"""Row counting for the orphan sweep — SELECT only, never a mutation.

Split from the apply path so the counting code and the DELETE code cannot
be changed together by accident. Every statement here is a ``SELECT
count()`` against a table named by a module constant; the values are always
parameters.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def sweep_predicate(target: Any, chain: str) -> str:
    """Return the predicate for a rollback target.

    Chain-scoped tables hold rows for several chains and MUST be filtered
    on ``chain`` or the DELETE would take out other chains' live rows.
    Solidity-free EVM share that shape; ``solana_transfers`` and
    ``solana_spl_balance_changes`` are slot-keyed and carry no ``chain``
    column, so they get a height-only predicate.

    Args:
        target: A :class:`lite_indexer.config.rollback_tables.RollbackTarget`.
        chain: Chain being swept.

    Returns:
        The WHERE clause text (without the ``WHERE`` keyword).
    """
    if target.chain_scoped:
        return f"chain = %(chain)s AND {target.column} > %(safe)s AND {target.column} <= %(cap)s"
    return f"{target.column} > %(safe)s AND {target.column} <= %(cap)s"


def count_orphans(
    ch_client: Any,
    target: Any,
    chain: str,
    safe_point: int,
    cap: int,
) -> int:
    """Count the candidate rows in one table inside the sweepable window.

    The window is ``(safe_point, cap]`` where ``cap`` is
    ``safe_point + max_rows_per_run`` when a cap is in force — counting
    the whole window would report a number the subsequent capped DELETE
    would not remove, and an operator comparing the two would be misled.

    Args:
        ch_client: ClickHouse client.
        target: Rollback target naming the table and its height column.
        chain: Chain to sweep.
        safe_point: Exclusive lower bound of the window.
        cap: Inclusive upper bound of the window.

    Returns:
        Candidate row count, or 0 when the count query fails. A failure
        returns 0 rather than raising so one bad table cannot abort a
        read-only report; the caller logs the shortfall as a refusal.
    """
    if cap <= safe_point:
        return 0
    query = f"SELECT count() FROM {target.table} WHERE {sweep_predicate(target, chain)}"  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    params = {"chain": chain, "safe": safe_point, "cap": cap}
    try:
        rows = ch_client.execute(
            query, params
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    except Exception as exc:  # noqa: BLE001 — a report must survive one bad table
        logger.warning("orphan_sweep_count_failed", table=target.table, chain=chain, error=str(exc)[:120])
        return 0
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0] or 0)


def max_height_above(ch_client: Any, target: Any, chain: str, safe_point: int) -> int:
    """Return the highest height strictly above ``safe_point`` (0 when none).

    Used to report how far above the safe point the stored data reaches,
    which is the number that tells an operator whether the backlog is a
    handful of blocks or the whole reorg window.

    Args:
        ch_client: ClickHouse client.
        target: Rollback target.
        chain: Chain to inspect.
        safe_point: Exclusive lower bound.

    Returns:
        Highest height in the window, or 0 when the table has no row there.
    """
    column = target.column
    if target.chain_scoped:
        query = f"SELECT max({column}) FROM {target.table} WHERE chain = %(chain)s AND {column} > %(safe)s"  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    else:
        query = f"SELECT max({column}) FROM {target.table} WHERE {column} > %(safe)s"  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    try:
        rows = ch_client.execute(
            query, {"chain": chain, "safe": safe_point}
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    except Exception as exc:  # noqa: BLE001 — reporting only
        logger.warning("orphan_sweep_max_height_failed", table=target.table, error=str(exc)[:120])
        return 0
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0] or 0)


__all__ = ["count_orphans", "max_height_above", "sweep_predicate"]
