"""
LiteIndexer writers — write/telemetry phase helpers.

Extracted from ``lite_indexer/indexer.py`` (Batch 11 degod) into a
cohesive module. Pure, self-contained write helpers used by the sync
loop's persist phase:

  - :func:`build_nats_payloads` — route cycle events to NATS subjects
  - :func:`write_deployer_events` — EVM + Solana deployer event writes
  - :func:`flush_solana_launch_bundles` — Solana launch + Jito bundle flush
  - :func:`flush_tron_launch` — Tron SunPump launch / SunSwap pair flush
  - :func:`write_ndjson` — NDJSON write-ahead log (gated by NDJSON_ENABLED)
  - :func:`maybe_run_vector_ingestion` — LanceDB vector ingestion pipeline

Every helper is best-effort by design: a write failure logs and is
contained so the core sync loop never aborts on a telemetry-path error.
The ClickHouse write of the durable transfer record itself stays in the
sync loop (it carries the data-loss guard that rewinds state).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from lite_indexer.config import (
    VECTOR_INGESTION_BATCH_SIZE,
    VECTOR_INGESTION_INTERVAL_BLOCKS,
)
from lite_indexer.logging import get_logger
from lite_indexer.storage import ClickHouseWriter, NDJSONWriter
from lite_indexer.strategies import EVMChainStrategy, SolanaStrategy, TronStrategy
from lite_indexer.strategies.solana.bundles import write_bundle
from lite_indexer.strategies.solana.launch import write_launch as write_solana_launch
from lite_indexer.strategies.tron.launch import PairEvent as TronPairEvent
from lite_indexer.strategies.tron.launch import write_launch as write_tron_launch
from lite_indexer.strategies.tron.launch import write_pair as write_tron_pair
from lite_indexer.streaming import NATSPublisher

logger = get_logger(__name__)


def build_nats_payloads(
    transfers: list[dict[str, Any]],
    dex_events: list[dict[str, Any]],
    deployer_events: list[dict[str, Any]],
    trace_events: list[dict[str, Any]],
) -> list[tuple[str, dict[str, Any]]]:
    """Route a cycle's events to NATS ``(subject, payload)`` pairs.

    Event routing:
      - transfer rows        → ``evm_transfer`` subject
      - DEX swap rows        → ``dex_swap`` subject
      - whale-flagged swaps  → ``whale_alert`` subject (in addition)
      - deployer rows        → ``deployer_event`` subject
      - internal trace rows  → ``evm_transfer`` subject (type field
        distinguishes ``internal_transfer`` on the wire)

    Non-dict entries (e.g. the reorg sentinel) are skipped.

    Returns:
        List of ``(subject, payload)`` pairs (possibly empty).
    """
    payloads: list[tuple[str, dict[str, Any]]] = []
    for event in transfers:
        if isinstance(event, dict):
            payloads.append(("evm_transfer", event))
    for event in dex_events:
        if isinstance(event, dict):
            payloads.append(("dex_swap", event))
            if event.get("whale"):
                payloads.append(("whale_alert", event))
    for event in deployer_events:
        if isinstance(event, dict):
            payloads.append(("deployer_event", event))
    for event in trace_events:
        if isinstance(event, dict):
            payloads.append(("evm_transfer", event))
    return payloads


async def write_deployer_events(
    chain: str,
    evm_strategy: EVMChainStrategy | None,
    solana_strategy: SolanaStrategy | None,
    ch_writer: ClickHouseWriter,
    enrich: Callable[[list[dict[str, Any]]], Awaitable[list[dict[str, Any]]]],
) -> int:
    """Persist the buffered deployer events (EVM + Solana).

    EVM deployer rows resolve the real deployer EOA (tx ``from``) via
    ``eth_getTransactionByHash`` before inserting, so enrichment is
    awaited. Solana deployer events are captured structurally by the
    strategy (creation tx fee payer) and written as-is.

    Args:
        chain: Chain name (ClickHouse ``chain`` column).
        evm_strategy: EVM strategy whose ``_last_pair_events`` buffer is
            enriched + written; None for non-EVM chains.
        solana_strategy: Solana strategy whose ``_last_deployer_events``
            buffer is written; None for non-Solana chains.
        ch_writer: Shared ClickHouse writer.
        enrich: Timestamp-enrichment callable (the indexer's
            ``_enrich_timestamps``) applied to EVM pair events.

    Returns:
        Number of deployer rows written (0 when no buffer is pending).
    """
    deployer_written = 0
    if evm_strategy and evm_strategy._last_pair_events:
        evm_strategy._last_pair_events = await enrich(evm_strategy._last_pair_events)
        deployer_written = await ch_writer.write_deployer_events(chain, evm_strategy._last_pair_events)
    elif solana_strategy and solana_strategy._last_deployer_events:
        deployer_written = await ch_writer.write_solana_deployer_events(chain, solana_strategy._last_deployer_events)
    return deployer_written


async def flush_solana_launch_bundles(
    strategy: SolanaStrategy,
    ch_writer: ClickHouseWriter,
    nats_publisher: NATSPublisher | None,
) -> int:
    """Drain + persist the Solana launch and Jito bundle buffers.

    Each write persists to ClickHouse AND fans out to NATS, mirroring the
    deployer-event path. Best-effort: a per-event failure logs and keeps
    the event dropped (buffers are cleared regardless, matching the sync
    loop's flush semantics).

    Args:
        strategy: Solana strategy whose ``_last_launch_events`` and
            ``_last_bundle_events`` buffers are drained.
        ch_writer: Shared ClickHouse writer.
        nats_publisher: Optional NATS publisher for real-time fan-out.

    Returns:
        Number of events written.
    """
    launch_written = 0
    try:
        sol_launch_events = strategy._last_launch_events
        strategy._last_launch_events = []
        for evt in sol_launch_events:
            try:
                await write_solana_launch(evt, ch_writer, nats_publisher)
                launch_written += 1
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.warning("solana_launch_write_failed", error=str(exc)[:100])
        sol_bundle_events = strategy._last_bundle_events
        strategy._last_bundle_events = []
        for evt in sol_bundle_events:
            try:
                await write_bundle(evt, ch_writer, nats_publisher)
                launch_written += 1
            except Exception as exc:  # noqa: BLE001 — best-effort
                logger.warning("solana_bundle_write_failed", error=str(exc)[:100])
    except Exception as exc:  # noqa: BLE001 — best-effort
        logger.warning("solana_launch_flush_failed", error=str(exc)[:100])
    return launch_written


async def flush_tron_launch(
    strategy: TronStrategy,
    ch_writer: ClickHouseWriter,
    nats_publisher: NATSPublisher | None,
) -> int:
    """Drain + persist the Tron launch / pair buffers.

    Pair events go through :func:`write_tron_pair`, everything else
    through :func:`write_tron_launch`. Best-effort: a per-event failure
    logs and the buffer is cleared regardless (matching the sync loop's
    flush semantics).

    Args:
        strategy: Tron strategy whose ``_last_launch_events`` buffer is
            drained.
        ch_writer: Shared ClickHouse writer.
        nats_publisher: Optional NATS publisher for real-time fan-out.

    Returns:
        Number of events written.
    """
    launch_written = 0
    tron_launch_events = strategy._last_launch_events
    strategy._last_launch_events = []
    for evt in tron_launch_events:
        try:
            if isinstance(evt, TronPairEvent):
                await write_tron_pair(evt, ch_writer, nats_publisher)
            else:
                await write_tron_launch(evt, ch_writer, nats_publisher)
            launch_written += 1
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.warning("tron_launch_write_failed", error=str(exc)[:100])
    return launch_written


def write_ndjson(
    ndjson_writer: NDJSONWriter,
    chain: str,
    events: list[dict[str, Any]],
    dex_events: list[dict[str, Any]],
    summary: dict[str, Any],
) -> int:
    """Append the cycle's events to the NDJSON write-ahead log.

    Gated by NDJSON_ENABLED — redundant with ClickHouse, which is the
    durable store. When disabled, recovery is restart-from-state: at most
    one sync cycle is lost on crash and re-fetched from RPC.

    Args:
        ndjson_writer: Configured NDJSON writer (only present when
            NDJSON is enabled).
        chain: Chain name.
        events: Transfer rows.
        dex_events: DEX swap rows.
        summary: Sync summary dict; write stats are merged on success.

    Returns:
        Total bytes written (0 when nothing was appended).
    """
    bytes_written = 0
    if events:
        bytes_written = ndjson_writer.write_events(chain, events)
        if bytes_written:
            summary["events_written_ndjson"] = len(events)
            summary["bytes_written"] = bytes_written

    if dex_events:
        dex_bytes = ndjson_writer.write_dex_events(chain, dex_events)
        if dex_bytes:
            summary["dex_events_written_ndjson"] = len(dex_events)
            summary["dex_bytes_written"] = dex_bytes

    return bytes_written


async def maybe_run_vector_ingestion(
    chain: str,
    vector_store: Any,
    ch_writer: ClickHouseWriter,
    last_ingestion_block: int | None,
    from_block: int,
    to_block: int,
) -> tuple[dict[str, int] | None, Any, int | None]:
    """Run the LanceDB ingestion pipeline when the block interval elapsed.

    The pipeline reads the freshly synced ``[from_block, to_block]``
    range from ClickHouse, generates text representations for wallets and
    contracts, embeds them via Ollama (``nomic-embed-text``), and stores
    them in LanceDB collections. The first cycle always ingests (there is
    no prior block marker); afterwards ingestion runs every
    ``VECTOR_INGESTION_INTERVAL_BLOCKS`` blocks.

    Best-effort: ClickHouse unavailability or a pipeline error returns
    ``None`` stats and leaves the marker unchanged — vector data is
    enrichment, never the durable transfer record.

    Args:
        chain: Chain name.
        vector_store: The lazy LanceDB store (None until first ingestion).
        ch_writer: Shared ClickHouse writer (the raw client is the read
            source for the fresh range).
        last_ingestion_block: Prior ingestion marker (None = never ran).
        from_block: First block of the synced range.
        to_block: Last block of the synced range.

    Returns:
        ``(stats, vector_store, last_ingestion_block)`` — stats dict
        (``wallets_indexed`` / ``contracts_indexed``) or None when
        skipped, the (possibly newly-created) vector store, and the
        updated marker (unchanged when skipped).
    """
    if last_ingestion_block is not None and to_block - last_ingestion_block < VECTOR_INGESTION_INTERVAL_BLOCKS:
        return None, vector_store, last_ingestion_block

    client = ch_writer.client()
    if client is None:
        logger.warning("vector_ingestion_skipped_no_ch", chain=chain)
        return None, vector_store, last_ingestion_block

    from lite_indexer.vectors.ingestion import run_vector_ingestion
    from lite_indexer.vectors.lance_store import LanceVectorStore

    if vector_store is None:
        vector_store = LanceVectorStore()

    stats = await run_vector_ingestion(
        vector_store,
        client,
        chain,
        from_block,
        to_block,
        limit=VECTOR_INGESTION_BATCH_SIZE,
    )
    logger.info(
        "vector_ingestion_complete",
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        wallets=stats["wallets_indexed"],
        contracts=stats["contracts_indexed"],
    )
    return stats, vector_store, to_block
