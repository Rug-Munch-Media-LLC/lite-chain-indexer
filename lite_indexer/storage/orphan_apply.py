"""Bounded, explicitly-confirmed DELETE for the orphan sweep.

The only place in this feature that mutates data. Three independent gates
stand between a candidate row and its deletion, and all three must pass:

1. The caller resolved a ``STATUS_OK`` safe point (a real, deep
   checkpoint exists). Every other status never reaches this module.
2. Both ``--apply`` and ``--yes`` were passed by the operator. A stray
   flag, a copy-pasted command or a cron entry cannot trigger a DELETE.
3. The DELETE carries a bounded height window ``(safe_point, cap]``, so
   the blast radius of any single statement is a finite range even if the
   caller mis-counted.

What the cap does and does not guarantee, precisely: the statement bounds
the deleted *heights*, not the deleted *rows*. A block can hold an
unbounded number of transfers, so a bounded window is not by itself a
bounded row count. The row budget (``--max-rows-per-run``) is therefore
enforced by the caller, which spends it across tables in order and stops
issuing statements once it is exhausted. The one residual race — a live
indexer writing new rows into the window between the caller's count and
this DELETE — is closed by a post-statement overrun check that raises the
run's error count rather than hiding it. Every statement is logged.
"""

from __future__ import annotations

from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.storage.orphan_counts import sweep_predicate

logger = get_logger(__name__)


class RefuseApplyError(RuntimeError):
    """Raised when an apply was requested without the required confirmation."""


def require_confirmation(apply_flag: bool, yes_flag: bool) -> None:
    """Fail closed unless the operator passed BOTH ``--apply`` and ``--yes``.

    Args:
        apply_flag: Value of ``--apply``.
        yes_flag: Value of ``--yes``.

    Raises:
        RefuseApplyError: When either flag is missing.
    """
    if not apply_flag or not yes_flag:
        missing = "--apply" if not apply_flag else "--yes"
        raise RefuseApplyError(f"refusing to delete without {missing}; both --apply and --yes are required")


def delete_orphans(ch_client: Any, target: Any, chain: str, safe_point: int, cap: int) -> int:
    """DELETE one table's rows inside the bounded window ``(safe_point, cap]``.

    The window is inclusive of ``cap`` and exclusive of ``safe_point``, so
    the fork block itself — which the reorg rollback deliberately keeps —
    can never be removed by this path.

    Args:
        ch_client: ClickHouse client.
        target: Rollback target naming the table and its height column.
        chain: Chain to sweep.
        safe_point: Exclusive lower bound; nothing at or below is deleted.
        cap: Inclusive upper bound of the approved height window.

    Returns:
        Rows deleted, or 0 when the window is empty or the DELETE failed.
        A failure returns 0 and increments the error metric so the caller
        reports partial progress instead of a clean sweep.
    """
    if cap <= safe_point:
        logger.info("orphan_sweep_delete_skipped", table=target.table, reason="empty_window")
        return 0
    query = f"DELETE FROM {target.table} WHERE {sweep_predicate(target, chain)}"  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    params = {"chain": chain, "safe": safe_point, "cap": cap}
    try:
        result = ch_client.execute(
            query, params
        )  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
    except Exception as exc:
        logger.error(
            "orphan_sweep_delete_failed",
            table=target.table,
            chain=chain,
            safe_point=safe_point,
            cap=cap,
            error=str(exc)[:120],
        )
        metrics_incr("lite_indexer_orphan_sweep_delete_errors")
        return 0
    deleted = result if isinstance(result, int) else 0
    metrics_incr("lite_indexer_orphan_sweep_rows_deleted", float(deleted))
    logger.warning(
        "orphan_sweep_table_deleted",
        table=target.table,
        chain=chain,
        safe_point=safe_point,
        cap=cap,
        deleted=deleted,
    )
    return deleted


__all__ = ["RefuseApplyError", "delete_orphans", "require_confirmation"]
