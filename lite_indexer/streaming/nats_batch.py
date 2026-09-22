"""Pipelined block-batch publishing helpers (split from nats_publisher).

One bounded wait per batch instead of per-event acks: the old sequential
loop stalled sync windows for minutes when the NATS server degraded.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def prepare_batch(
    chain: str,
    block: int,
    events: list[tuple[str, dict[str, Any]]],
    subject_for: Callable[[str, str], str | None],
) -> list[tuple[str, bytes]]:
    """Serialize events into ``(subject, payload)`` pairs; unknown types skipped."""
    prepared: list[tuple[str, bytes]] = []
    for event_type, data in events:
        subject = subject_for(chain, event_type)
        if subject is None:
            continue
        payload = dict(data)
        payload.setdefault("block", block)
        envelope = {
            "chain": chain,
            "type": event_type,
            "published_at": datetime.now(UTC).isoformat(),
            "data": payload,
        }
        try:
            prepared.append((subject, json.dumps(envelope, default=str).encode("utf-8")))
        except (TypeError, ValueError) as exc:
            logger.warning("nats_payload_serialize_failed", chain=chain, error=str(exc)[:120])
    return prepared


async def send_batch(js: Any, nc: Any, prepared: list[tuple[str, bytes]], *, timeout: float) -> tuple[int, int]:
    """Pipeline the sends; returns ``(ok, failed)``.

    JetStream: async publishes gathered under one ``timeout``. Core NATS:
    buffered sends then a single flush. Raises only on connection-level
    failures (caller handles circuit + metrics).
    """
    if js is not None:
        futures = [js.publish_async(subject, payload) for subject, payload in prepared]
        results = await asyncio.wait_for(
            asyncio.gather(*futures, return_exceptions=True), timeout=timeout
        )
        ok = sum(1 for result in results if not isinstance(result, BaseException))
        return ok, len(prepared) - ok
    for subject, payload in prepared:
        await nc.publish(subject, payload)
    await asyncio.wait_for(nc.flush(), timeout=timeout)
    return len(prepared), 0
