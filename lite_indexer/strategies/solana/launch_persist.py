"""Solana launch persistence — ClickHouse writes and NATS publishing.

Extracted from ``launch.py`` (ratchet-down split). See that module's
docstring for the sink contract and REVIEW notes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from lite_indexer.config import RETENTION_POLICY
from lite_indexer.logging import get_logger
from lite_indexer.strategies.solana.launch_constants import (
    NATS_EVENT_TYPE,
    NATS_SUBJECT,
)
from lite_indexer.strategies.solana.launch_models import LaunchEvent
from lite_indexer.strategies.solana.launch_rows import _insert_launch

logger = get_logger(__name__)

# One-time warning guard for an unmapped NATS subject.
_nats_subject_warned = False

_SOLANA_LAUNCH_RULE = RETENTION_POLICY["solana_launches"]

_SOLANA_LAUNCHES_DDL = f"""
CREATE TABLE IF NOT EXISTS wallet_memory.solana_launches (
    chain LowCardinality(String) DEFAULT 'solana',
    slot UInt64,
    block_timestamp DateTime64(3),
    signature String,
    mint String CODEC(ZSTD(3)),
    creator String DEFAULT '',
    venue LowCardinality(String) DEFAULT '',
    launch_signal UInt8 DEFAULT 0,
    graduation_signal UInt8 DEFAULT 0,
    confidence LowCardinality(String) DEFAULT 'low',
    instruction_name String DEFAULT '',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (mint, slot)
TTL {_SOLANA_LAUNCH_RULE.time_column} + INTERVAL {_SOLANA_LAUNCH_RULE.days} DAY
"""

def ensure_solana_launches_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.solana_launches`` if it does not exist.

    Idempotent and best-effort, mirroring
    ``lite_indexer.traces.solana.ensure_solana_transfers_table``: the
    table is also provisioned by ``migrations/007_solana_launch.sql``;
    this exists so the table appears on first launch write without
    operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_SOLANA_LAUNCHES_DDL)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("solana_launches_table_skip", error=str(exc)[:80])
        else:
            logger.warning("solana_launches_table_failed", error=str(exc)[:100])
        return False


async def _publish_launch(nats: Any, launch: LaunchEvent) -> None:
    """Publish a launch event to NATS (best-effort, one-time unmapped warning)."""
    from lite_indexer.streaming.nats_publisher import NATSPublisher

    global _nats_subject_warned
    if NATSPublisher.subject_for(launch.chain, NATS_EVENT_TYPE) is None:
        if not _nats_subject_warned:
            _nats_subject_warned = True
            logger.warning(
                "solana_launch_nats_subject_unmapped",
                subject=NATS_SUBJECT,
                event_type=NATS_EVENT_TYPE,
                hint="add 'launch': 'chain.{chain}.launch' to NATSPublisher.SUBJECTS",
            )
        return

    try:
        await nats.publish_event(launch.chain, NATS_EVENT_TYPE, launch.model_dump())
    except Exception as exc:  # noqa: BLE001 — NATS is best-effort
        logger.warning(
            "solana_launch_publish_failed",
            mint=launch.mint,
            error=str(exc)[:120],
        )


async def write_launch(
    launch: LaunchEvent,
    ch_writer: Any,
    nats: Any | None = None,
) -> None:
    """Persist a Solana launch event to ClickHouse and publish to NATS.

    Dependency-injected sinks keep the detectors pure:
      - ``ch_writer`` — a
        :class:`lite_indexer.storage.clickhouse.ClickHouseWriter` (or a
        duck-typed equivalent exposing ``client()``). The raw client is
        used for the INSERT because the writer has no Solana-specific
        method; this is the documented purpose of ``ClickHouseWriter.client()``.
      - ``nats`` — a
        :class:`lite_indexer.streaming.nats_publisher.NATSPublisher`
        (optional). Publishes to ``chain.solana.launch`` (event type
        ``launch``).

    Best-effort by design: ClickHouse or NATS failures are logged and
    swallowed — NATS is a fan-out layer, ClickHouse the durable store.
    The row's ``block_timestamp`` falls back to now when the event
    carried no timestamp.

    [REVIEW] The NATS subject ``chain.solana.launch`` requires a
    ``"launch": "chain.{chain}.launch"`` entry in
    ``NATSPublisher.SUBJECTS`` — not added here because
    ``nats_publisher.py`` is out of scope (same convention as
    ``lite_indexer.strategies.tron.launch``). Until the integration
    point wires that mapping, the publish is skipped with a one-time
    warning.

    Args:
        launch: Detected launch event.
        ch_writer: ClickHouse writer (or raw client duck-type).
        nats: Optional NATS publisher.
    """
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer

    if client is not None:
        ensure_solana_launches_table(client)
        _insert_launch(client, launch, datetime.now(UTC))

    if nats is None or not hasattr(nats, "publish_event"):
        return

    await _publish_launch(nats, launch)
