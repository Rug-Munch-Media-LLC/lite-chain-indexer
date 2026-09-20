"""Tron deployer-events coverage — mirror SunPump/SunSwap launches into
``wallet_memory.deployer_events`` (the scanner-contract table).

Gap (batch-1 audit, SCANNER_CONTRACT.md): EVM and Solana strategies write
deployer rows; the Tron strategy only persisted launches to
``wallet_memory.tron_launches`` + NATS, so the scanner never saw tron
token creations. This module converts the already-detected
:class:`LaunchEvent` / :class:`PairEvent` objects into the contract's
``deployer_events`` row shape and inserts them.

Deployer semantics mirror the Solana path (structurally resolved, no
extra RPC): a SunPump ``TokenCreate`` carries the creator EOA in the
event itself; a SunSwap ``PairCreated`` exposes only the factory, so its
``deployer``/``deployer_eoa`` stay empty rather than being mislabeled
with the factory address (same fail-closed rule as the EVM path).

Best-effort by design: a ClickHouse failure is logged and contained —
the launch write path is unaffected.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

from .launch import LaunchEvent, PairEvent

logger = get_logger(__name__)

# Contract columns (docs/SCANNER_CONTRACT.md → deployer_events), in
# insert order. No log_index column exists live (contract §deployer_events).
_COLUMNS = (
    "deployer, deployer_eoa, token_address, token0, token1, pair_address, "
    "chain, block_number, block_timestamp, tx_hash, type, "
    "factory, ingested_at, source"
)


def _parse_ts(value: str, now: datetime) -> datetime:
    """Parse an ISO block timestamp, falling back to ``now``."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else now
    except (ValueError, TypeError):
        logger.debug("tron_ts_parse_failed", value=str(value)[:40])
        return now


def _launch_row(evt: LaunchEvent, now: datetime) -> tuple[Any, ...]:
    """Map a SunPump launch to a deployer_events row (creator = deployer)."""
    return (
        evt.creator,
        evt.creator,
        evt.token_address,
        evt.token_address,
        "",
        "",
        evt.chain,
        evt.block_number,
        _parse_ts(evt.block_timestamp, now),
        evt.tx_hash,
        evt.type,
        evt.contract_address,
        now,
        evt.source,
    )


def _pair_row(evt: PairEvent, now: datetime) -> tuple[Any, ...]:
    """Map a SunSwap PairCreated to a deployer_events row.

    The factory tx sender is not structurally available, so deployer
    fields stay empty (never mislabeled with the factory address).
    """
    return (
        "",
        "",
        evt.token0,
        evt.token0,
        evt.token1,
        evt.pair_address,
        evt.chain,
        evt.block_number,
        _parse_ts(evt.block_timestamp, now),
        evt.tx_hash,
        evt.type,
        evt.factory,
        now,
        evt.source,
    )


def to_deployer_rows(events: list[LaunchEvent | PairEvent]) -> list[tuple[Any, ...]]:
    """Convert tron launch/pair events into deployer_events row tuples.

    Pure function — no I/O. Unknown event types are skipped (logged) so a
    future event kind cannot corrupt the insert batch.
    """
    now = datetime.now(UTC)
    rows: list[tuple[Any, ...]] = []
    for evt in events:
        if isinstance(evt, LaunchEvent):
            rows.append(_launch_row(evt, now))
        elif isinstance(evt, PairEvent):
            rows.append(_pair_row(evt, now))
        else:
            logger.warning("tron_deployer_row_unknown_event", event_type=type(evt).__name__)
    return rows


async def write_tron_deployer_events(
    chain: str,
    events: list[LaunchEvent | PairEvent],
    ch_writer: Any,
) -> int:
    """Insert tron launch/pair events into ``wallet_memory.deployer_events``.

    Uses the raw ClickHouse client (duck-typed ``ch_writer.client()``),
    the same documented pattern as :func:`tron.launch.write_launch` —
    the shared writer has no Tron-specific deployer method.

    Args:
        chain: Chain name (``tron``).
        events: Buffered launch/pair events from the strategy.
        ch_writer: ClickHouse writer (or raw client duck-type).

    Returns:
        Number of rows inserted (0 when no events or on failure).
    """
    if not events:
        return 0
    rows = to_deployer_rows(events)
    if not rows:
        return 0
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer
    if client is None:
        return 0
    try:
        client.execute(
            f"INSERT INTO wallet_memory.deployer_events ({_COLUMNS}) VALUES",
            rows,
        )
        logger.info("tron_deployer_events_inserted", chain=chain, rows=len(rows))
        metrics_incr("clickhouse_inserts")
        return len(rows)
    except Exception as exc:  # noqa: BLE001 — best-effort persistence
        metrics_incr("clickhouse_errors")
        logger.error("tron_deployer_events_insert_failed", chain=chain, rows=len(rows), error=str(exc)[:150])
        return 0


__all__ = ["to_deployer_rows", "write_tron_deployer_events"]
