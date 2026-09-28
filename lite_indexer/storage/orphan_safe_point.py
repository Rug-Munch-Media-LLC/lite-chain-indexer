"""Safe-point resolution for the historical orphan sweep (read-only).

An orphan is a row in an indexed table that belongs to a block the chain
reorganized away, or to a batch that never reached a durable checkpoint.
Deleting a LIVE row is the catastrophic failure mode of this whole tool,
so the safe point is derived from evidence that is itself only written by
a batch that *completed*: the per-chain checkpoint table.

The rule is one line of arithmetic (:func:`resolve_safe_point`) and the
window it opens is deliberately narrow. See ``lite_indexer.cli._orphan_sweep``
for the operator-facing description; this module holds only the arithmetic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: The checkpoint table exists and holds rows for this chain; ``safe_point``
#: was computed and the (safe_point, checkpoint_max] window is sweepable.
STATUS_OK = "ok"
#: The checkpoint table does not exist (unknown table / no privileges).
#: NEVER interpreted as "everything above 0 is an orphan".
STATUS_NO_TABLE = "no_checkpoint_table"
#: The checkpoint table exists but has no row for this chain.
STATUS_EMPTY = "empty_checkpoint_table"
#: The query raised for any other reason.
STATUS_QUERY_FAILED = "query_failed"
#: A checkpoint exists but is not deeper than the reorg depth, so
#: ``checkpoint_max - depth`` falls at or below genesis.
STATUS_TOO_SHALLOW = "checkpoint_not_deep_enough"

#: Statuses that must abort the sweep for that chain. Only :data:`STATUS_OK`
#: permits a DELETE. Everything else is a refusal, never an empty result.
REFUSAL_STATUSES = frozenset(
    {
        STATUS_NO_TABLE,
        STATUS_EMPTY,
        STATUS_QUERY_FAILED,
        STATUS_TOO_SHALLOW,
    }
)


@dataclass(frozen=True)
class SafePoint:
    """Where the sweepable window opens, and why it is where it is.

    Attributes:
        status: One of the ``STATUS_*`` constants. Only ``STATUS_OK`` may
            be deleted from.
        safe_point: Lowest orphan candidate, exclusive. Meaningless unless
            ``status == STATUS_OK``.
        checkpoint_max: Highest checkpointed block for the chain, or 0.
        reason: Human-readable explanation for the report.
    """

    status: str
    safe_point: int
    checkpoint_max: int
    reason: str

    @property
    def sweepable(self) -> bool:
        """True only when a DELETE above ``safe_point`` is authorised."""
        return self.status == STATUS_OK


def _max_checkpointed(ch_client: Any, checkpoint_table: str, chain: str, chain_scoped: bool) -> int:
    """Read the highest checkpointed height for a chain (0 when none).

    Args:
        ch_client: ClickHouse client.
        checkpoint_table: Fully-qualified checkpoint table name.
        chain: Chain name.
        chain_scoped: True when the table carries a ``chain`` column.

    Returns:
        Highest checkpointed height, or 0 when the chain has no row.
    """
    if chain_scoped:
        query = f"SELECT max({'(block_number)'}) FROM {checkpoint_table} WHERE chain = %(chain)s"  # nosemgrep: formatted-sql-query — identifier is a module constant; values parameterized
    else:
        query = f"SELECT max({'(slot)'}) FROM {checkpoint_table}"
    rows = ch_client.execute(
        query, {"chain": chain}
    )  # nosemgrep: formatted-sql-query — identifier is a module constant; values parameterized
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0] or 0)


def resolve_safe_point(
    ch_client: Any,
    checkpoint_table: str,
    chain: str,
    depth: int,
    *,
    chain_scoped: bool = True,
) -> SafePoint:
    """Resolve the highest contiguously-checkpointed block for a chain.

    The checkpoint table is written once per *completed batch* (the batch
    tip), so its rows are batch tips rather than every block. ``max`` is
    therefore the highest height whose whole batch is known-durable, which
    is the strongest durable evidence this indexer records anywhere.

    The safe point is ``checkpoint_max - depth``: one full reorg depth
    below the last durably checkpointed block. Deleting anything at or
    below that line can only delete a block the live staging path has
    already verified against a reorg, so those rows are not orphans.

    Args:
        ch_client: ClickHouse client.
        checkpoint_table: Fully-qualified checkpoint table name.
        chain: Chain name.
        depth: Reorg depth for the chain (blocks / slots).
        chain_scoped: True when the checkpoint table carries a ``chain``
            column (EVM); False for the Solana slot-keyed table.

    Returns:
        :class:`SafePoint`. Only ``STATUS_OK`` authorises a DELETE; every
        other status is a refusal with the count left untouched.
    """
    if ch_client is None:
        return SafePoint(STATUS_QUERY_FAILED, 0, 0, "clickhouse_unavailable")
    try:
        checkpoint_max = _max_checkpointed(ch_client, checkpoint_table, chain, chain_scoped)
    except Exception as exc:  # noqa: BLE001 — any failure refuses the chain
        text = str(exc)
        missing = "doesn't exist" in text or "UNKNOWN_TABLE" in text or "unknown table" in text
        status = STATUS_NO_TABLE if missing else STATUS_QUERY_FAILED
        logger.warning("orphan_sweep_checkpoint_unavailable", chain=chain, table=checkpoint_table, status=status)
        return SafePoint(status, 0, 0, text[:120])

    if checkpoint_max <= 0:
        return SafePoint(
            STATUS_EMPTY,
            0,
            0,
            f"no checkpoint row for chain {chain} in {checkpoint_table}",
        )
    safe_point = checkpoint_max - depth
    if safe_point <= 0:
        return SafePoint(
            STATUS_TOO_SHALLOW,
            0,
            checkpoint_max,
            f"checkpoint_max={checkpoint_max} minus depth={depth} is not above genesis",
        )
    return SafePoint(
        STATUS_OK,
        safe_point,
        checkpoint_max,
        f"safe_point={safe_point} from checkpoint_max={checkpoint_max} depth={depth}",
    )


__all__ = [
    "REFUSAL_STATUSES",
    "STATUS_EMPTY",
    "STATUS_NO_TABLE",
    "STATUS_OK",
    "STATUS_QUERY_FAILED",
    "STATUS_TOO_SHALLOW",
    "SafePoint",
    "resolve_safe_point",
]
