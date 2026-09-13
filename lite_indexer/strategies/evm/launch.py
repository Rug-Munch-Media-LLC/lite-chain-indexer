"""Minimal EVM token-launch detection: DEX pair/pool creation → ``evm_launches``.

EVM chains have no single on-chain "launchpad create" like Solana
pump.fun or Tron SunPump; the closest universal launch signal is a DEX
pair/pool deployment from a known factory (``PairCreated`` /
``PoolCreated``). Those events are already fetched, verified (factory tx
receipt ``status == 1`` + deployed code at the pair address) and written
to ``deployer_events`` by the deployer path
(:mod:`lite_indexer.strategies.evm.deployer_handler`).

This module mirrors the *same verified events* into a dedicated
``wallet_memory.evm_launches`` table so the HTTP API's
``GET /api/v1/launches`` can surface "what launched recently" per EVM
chain without a new RPC scan. Detection is deliberately structural: a
pair/pool-creation tx IS the launch of that token pair on that DEX, at
``high`` confidence (the deployer path's false-launch guard already ran).

The table is provisioned idempotently at runtime (like the Solana/Tron
launch tables) and by ``migrations/011_evm_launches.sql``.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

EVM_LAUNCHES_TABLE: str = os.environ.get("CH_EVM_LAUNCHES_TABLE", "wallet_memory.evm_launches")

NATS_EVENT_TYPE = "launch"

_EVM_LAUNCHES_DDL = """
CREATE TABLE IF NOT EXISTS wallet_memory.evm_launches (
    chain LowCardinality(String),
    block_number UInt64,
    block_timestamp DateTime64(3),
    tx_hash String,
    token0 String,
    token1 String,
    pair_address String,
    factory String,
    version LowCardinality(String) DEFAULT 'v2',
    source String DEFAULT 'lite_indexer',
    ingested_at DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree()
ORDER BY (chain, block_number, tx_hash)
TTL block_timestamp + INTERVAL 365 DAY
"""

_EVM_LAUNCH_INSERT_COLUMNS = (
    "chain",
    "block_number",
    "block_timestamp",
    "tx_hash",
    "token0",
    "token1",
    "pair_address",
    "factory",
    "version",
    "source",
    "ingested_at",
)


def ensure_evm_launches_table(ch_client: Any) -> bool:
    """Create ``wallet_memory.evm_launches`` if it does not exist.

    Idempotent and best-effort, mirroring
    ``lite_indexer.strategies.solana.launch.ensure_solana_launches_table``:
    the table is also provisioned by ``migrations/011_evm_launches.sql``;
    this exists so the table appears on first launch write without
    operator DDL.

    Args:
        ch_client: ClickHouse client (anything with ``execute``).

    Returns:
        True when the table exists (or was created), False on failure.
    """
    try:
        ch_client.execute(_EVM_LAUNCHES_DDL)
        return True
    except Exception as exc:  # noqa: BLE001 — DDL is best-effort
        if "privileges" in str(exc).lower() or "doesn't exist" in str(exc).lower():
            logger.debug("evm_launches_table_skip", error=str(exc)[:80])
        else:
            logger.warning("evm_launches_table_failed", error=str(exc)[:100])
        return False


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


def _launch_row(
    chain: str,
    event: dict[str, Any],
    ts: datetime,
    now: datetime,
) -> tuple[Any, ...]:
    """Flatten a verified pair-created event into the insert row tuple.

    The event dict is the enriched deployer-event shape produced by
    ``lite_indexer.dex.parse_pair_created_logs`` + the indexer's
    timestamp enrichment: ``factory``, ``pair_address``, ``token0``,
    ``token1``, ``version``, ``blockNumber``, ``transactionHash``,
    ``blockTimestamp``.
    """
    return (
        chain,
        int(event.get("blockNumber") or 0),
        ts,
        event.get("transactionHash") or "",
        event.get("token0") or "",
        event.get("token1") or "",
        event.get("pair_address") or "",
        event.get("factory") or "",
        event.get("version") or "v2",
        "lite_indexer",
        now,
    )


async def write_launch(
    chain: str,
    event: dict[str, Any],
    ch_writer: Any,
    nats: Any | None = None,
) -> None:
    """Persist a verified EVM pair/pool-creation launch to ClickHouse + NATS.

    Dependency-injected sinks keep the detector pure:
      - ``ch_writer`` — a
        :class:`lite_indexer.storage.clickhouse.ClickHouseWriter` (or a
        duck-typed equivalent exposing ``client()``). The raw client is
        used for the INSERT because the writer has no launch-specific
        method (same convention as the Solana/Tron launch writers).
      - ``nats`` — a
        :class:`lite_indexer.streaming.nats_publisher.NATSPublisher`
        (optional). Publishes to ``chain.{chain}.launch`` (event type
        ``launch``), the same subject the Solana/Tron writers use.

    Best-effort by design: ClickHouse or NATS failures are logged and
    swallowed — NATS is a fan-out layer, ClickHouse the durable store.
    """
    client = ch_writer.client() if hasattr(ch_writer, "client") else ch_writer

    if client is not None:
        ensure_evm_launches_table(client)
        now = datetime.now(UTC)
        ts = _parse_block_timestamp(event.get("blockTimestamp"), now)
        try:
            client.execute(  # nosemgrep: formatted-sql-query — identifiers are module constants; values parameterized
                f"INSERT INTO {EVM_LAUNCHES_TABLE} ({', '.join(_EVM_LAUNCH_INSERT_COLUMNS)}) VALUES",
                [_launch_row(chain, event, ts, now)],
            )
            logger.info(
                "evm_launch_written",
                chain=chain,
                pair=event.get("pair_address", "")[:16],
                tx=event.get("transactionHash", "")[:16],
            )
            metrics_incr("clickhouse_inserts")
        except Exception as exc:  # noqa: BLE001 — best-effort persistence
            metrics_incr("clickhouse_errors")
            logger.error(
                "evm_launch_insert_failed",
                chain=chain,
                pair=event.get("pair_address", "")[:16],
                error=str(exc)[:150],
            )

    if nats is None or not hasattr(nats, "publish_event"):
        return

    from lite_indexer.streaming.nats_publisher import NATSPublisher

    if NATSPublisher.subject_for(chain, NATS_EVENT_TYPE) is None:
        return

    payload = {
        "chain": chain,
        "block_number": int(event.get("blockNumber") or 0),
        "block_timestamp": event.get("blockTimestamp") or "",
        "tx_hash": event.get("transactionHash") or "",
        "token0": event.get("token0") or "",
        "token1": event.get("token1") or "",
        "pair_address": event.get("pair_address") or "",
        "factory": event.get("factory") or "",
        "version": event.get("version") or "v2",
        "event_type": "pair_created",
        "confidence": "high",
    }
    try:
        await nats.publish_event(chain, NATS_EVENT_TYPE, payload)
    except Exception as exc:  # noqa: BLE001 — NATS is best-effort
        logger.warning(
            "evm_launch_publish_failed",
            chain=chain,
            pair=event.get("pair_address", "")[:16],
            error=str(exc)[:120],
        )


__all__ = [
    "EVM_LAUNCHES_TABLE",
    "NATS_EVENT_TYPE",
    "ensure_evm_launches_table",
    "write_launch",
]
