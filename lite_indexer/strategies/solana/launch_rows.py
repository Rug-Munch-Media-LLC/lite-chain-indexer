"""Solana launch ClickHouse row helpers.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the sink contract.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr
from lite_indexer.strategies.solana.launch_constants import SOLANA_LAUNCHES_TABLE
from lite_indexer.strategies.solana.launch_models import LaunchEvent

logger = get_logger(__name__)


_SOLANA_LAUNCH_INSERT_COLUMNS = (
    "chain",
    "slot",
    "block_timestamp",
    "signature",
    "mint",
    "creator",
    "venue",
    "launch_signal",
    "graduation_signal",
    "confidence",
    "instruction_name",
    "source",
    "ingested_at",
)


def _parse_block_timestamp(value: Any, fallback: datetime) -> datetime:
    """Parse an ISO string or Unix timestamp into an aware datetime."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return fallback
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(int(value), tz=UTC)
        except (ValueError, TypeError, OverflowError):
            return fallback
    return fallback


def _launch_row(launch: LaunchEvent, ts: datetime, now: datetime) -> tuple[Any, ...]:
    """Flatten a LaunchEvent into the ClickHouse insert row tuple."""
    return (
        launch.chain or "solana",
        int(launch.slot or 0),
        ts,
        launch.signature or "",
        launch.mint or "",
        launch.creator or "",
        launch.venue or "",
        int(launch.launch_signal),
        int(launch.graduation_signal),
        launch.confidence or "low",
        launch.instruction_name or "",
        launch.source or "lite_indexer",
        now,
    )


def _insert_launch(client: Any, launch: LaunchEvent, now: datetime) -> None:
    """Insert one launch row into ClickHouse (best-effort)."""
    ts = _parse_block_timestamp(launch.block_timestamp, now)
    try:
        client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
            f"INSERT INTO {SOLANA_LAUNCHES_TABLE} ({', '.join(_SOLANA_LAUNCH_INSERT_COLUMNS)}) VALUES",
            [_launch_row(launch, ts, now)],
        )
        logger.info(
            "solana_launch_written",
            mint=launch.mint,
            venue=launch.venue,
            signature=launch.signature[:16],
        )
        metrics_incr("clickhouse_inserts")
    except Exception as exc:  # noqa: BLE001 — best-effort persistence
        metrics_incr("clickhouse_errors")
        logger.error(
            "solana_launch_insert_failed",
            mint=launch.mint,
            error=str(exc)[:150],
        )


