"""
NATS JetStream real-time event streaming.

Publishes indexed events to NATS for real-time consumers (scanner
alerts, dashboards, bots) immediately after ClickHouse writes succeed.
The stream is a fan-out layer, never the durable store — publishing is
best-effort and protected by a circuit breaker so NATS downtime never
blocks indexing.

Subjects (per event type, chain-substituted)::

    evm_transfer    → chain.{chain}.transfer
    dex_swap        → chain.{chain}.dex.swap
    deployer_event  → chain.{chain}.deploy
    whale_alert     → alerts.whale.{chain}

Usage::

    publisher = NATSPublisher()          # gated by NATS_ENABLED
    ok = await publisher.publish_event("ethereum", "dex_swap", {...})
    n = await publisher.stream_block_batch("base", 12_345_678, payloads)
    await publisher.close()

The publisher connects lazily on first publish, auto-provisions a
JetStream stream (``CHAIN_EVENTS``) capturing ``chain.>`` and
``alerts.>`` when JetStream is enabled, and degrades to core NATS
publish when the server has JetStream disabled.
"""

from __future__ import annotations

from lite_indexer.streaming.nats_publisher import NATSPublisher
from lite_indexer.streaming.solana_ws import SolanaWebsocketConfig, SolanaWebsocketIngestor

__all__ = ["NATSPublisher", "SolanaWebsocketConfig", "SolanaWebsocketIngestor"]
