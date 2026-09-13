"""EVM websocket head pool — push the chain tip instead of polling it.

EVM chains previously discovered a new tip only when their HTTP sync loop
polled ``eth_blockNumber``. That wastes request quota and adds up to a full
poll interval of tip latency. This module opens a single websocket to a
healthy endpoint, subscribes to ``eth_subscribe(["newHeads"])`` and invokes
``on_head(block_number, block_hash)`` on every pushed head so the sync loop
can start a pass immediately.

Scope discipline
----------------
The websocket URLs come **only** from the private endpoint overlay
(``ws`` list or ``ws://`` / ``wss://`` entries — see
``docs/PROVIDER_OVERLAY.md``). The public tree names no provider. With no WS
endpoint configured the pool is a no-op and HTTP polling continues as the
sole tip source.

Robustness
----------
The loop reconnects with jittered exponential backoff on any disconnect,
rotates to the next endpoint after repeated failures, and — when every
endpoint has failed — logs ``ws_head_fallback`` and returns cleanly so the
caller keeps relying on HTTP polling. A websocket failure never stalls the
indexer and never raises into the sync loop. The ``ws_connect`` / ``sleep``
injections let tests drive the loop with a fake socket and clock.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from lite_indexer.config import (
    RPC_WS_HEAD_ENABLED,
    RPC_WS_HEAD_MAX_ENDPOINTS,
    RPC_WS_HEAD_RECONNECT_BASE_SECONDS,
    RPC_WS_HEAD_RECONNECT_MAX_SECONDS,
)
from lite_indexer.logging import get_logger
from lite_indexer.metrics import incr as metrics_incr

logger = get_logger(__name__)

#: ``on_head(block_number, block_hash)`` — sync or async callable.
HeadCallback = Callable[[int, str], Any]
#: ``ws_connect(url)`` → async context manager yielding a connected socket.
WsConnect = Callable[[str], Any]
#: ``sleep(seconds)`` — injectable for tests.
Sleep = Callable[[float], Awaitable[None]]

# Consecutive connect/read failures on one endpoint before rotating to the next.
DEFAULT_FAILURES_PER_ENDPOINT = 2
_WS_MAX_MESSAGE_BYTES = 2**20
_WS_PING_INTERVAL_SECONDS = 30.0


def _subscribe_request(request_id: int = 1) -> dict[str, Any]:
    """Build the ``eth_subscribe`` request for new block heads."""
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "eth_subscribe",
        "params": ["newHeads"],
    }


def _parse_head(raw: Any) -> tuple[int, str] | None:
    """Parse one websocket frame into ``(block_number, block_hash)``.

    Accepts JSON text/bytes or an already-decoded mapping. Returns ``None``
    for the subscription acknowledgement and any frame that is not a
    ``newHeads`` notification.
    """
    try:
        message = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except (ValueError, TypeError):
        return None
    if not isinstance(message, dict) or message.get("method") != "eth_subscription":
        return None
    params = message.get("params")
    if not isinstance(params, dict):
        return None
    result = params.get("result")
    if not isinstance(result, dict):
        return None
    number = result.get("number")
    block_hash = result.get("hash")
    if not isinstance(number, str) or not isinstance(block_hash, str):
        return None
    try:
        block_number = int(number, 16)
    except ValueError:
        return None
    return block_number, block_hash


def _connect(url: str, ws_connect: WsConnect | None) -> Any:
    """Return an async context manager yielding a connected websocket."""
    if ws_connect is not None:
        return ws_connect(url)
    import websockets  # imported lazily — the indexer runs without it

    return websockets.connect(
        url,
        ping_interval=_WS_PING_INTERVAL_SECONDS,
        ping_timeout=_WS_PING_INTERVAL_SECONDS * 2,
        max_size=_WS_MAX_MESSAGE_BYTES,
    )


async def _invoke(on_head: HeadCallback, block_number: int, block_hash: str) -> None:
    """Invoke a head callback, awaiting it when it is a coroutine function."""
    outcome = on_head(block_number, block_hash)
    if asyncio.iscoroutine(outcome):
        await outcome


def _jittered_delay(base: float, max_seconds: float, failures: int) -> float:
    """Exponential backoff with full jitter (bounded by ``max_seconds``)."""
    backoff = min(base * (2 ** max(0, failures - 1)), max_seconds)
    return random.uniform(backoff * 0.5, backoff) if backoff > 0 else 0.0


async def subscribe_heads(
    chain: str,
    endpoints: Sequence[str],
    on_head: HeadCallback,
    *,
    enabled: bool = RPC_WS_HEAD_ENABLED,
    max_endpoints: int = RPC_WS_HEAD_MAX_ENDPOINTS,
    failures_per_endpoint: int = DEFAULT_FAILURES_PER_ENDPOINT,
    reconnect_base_seconds: float = RPC_WS_HEAD_RECONNECT_BASE_SECONDS,
    reconnect_max_seconds: float = RPC_WS_HEAD_RECONNECT_MAX_SECONDS,
    ws_connect: WsConnect | None = None,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """Subscribe to new EVM heads and drive ``on_head`` until exhausted.

    Opens one websocket at a time, subscribes to ``newHeads``, and invokes
    ``on_head(block_number, block_hash)`` for each notification. On a drop it
    reconnects with jittered backoff; after ``failures_per_endpoint``
    consecutive failures it rotates to the next endpoint. When every endpoint
    in the (bounded) pool has failed it logs ``ws_head_fallback`` and returns
    — no exception reaches the caller, which then relies on HTTP polling.

    Args:
        chain: Chain name (logging/metrics scope).
        endpoints: Candidate websocket URLs (overlay-provided, ordered).
        on_head: Sync or async ``(block_number, block_hash)`` callback.
        enabled: Master switch; ``False`` makes the pool a no-op.
        max_endpoints: Cap on how many endpoints are rotated.
        failures_per_endpoint: Consecutive failures before rotating.
        reconnect_base_seconds: Base delay for the jittered backoff.
        reconnect_max_seconds: Upper bound for the backoff delay.
        ws_connect: Optional ``ws_connect(url)`` factory (tests).
        sleep: Optional async sleep (tests).
    """
    if not enabled:
        return
    pool = [url for url in endpoints if url][: max(0, max_endpoints)]
    if not pool:
        # No WS configured → polling continues; nothing to do.
        return

    failures: dict[str, int] = dict.fromkeys(pool, 0)
    index = 0
    while True:
        url = pool[index % len(pool)]
        disconnected: str | None = None
        try:
            async with _connect(url, ws_connect) as ws:
                await ws.send(json.dumps(_subscribe_request()))
                metrics_incr("ws_head_connected")
                logger.info("ws_head_connected", chain=chain, endpoint=url)
                async for raw in ws:
                    parsed = _parse_head(raw)
                    if parsed is None:
                        continue
                    # A head proves this connection is genuinely useful — only
                    # then is the endpoint's failure streak cleared, so a
                    # connect-then-drop loop still rotates and falls back.
                    failures[url] = 0
                    block_number, block_hash = parsed
                    metrics_incr("ws_head_messages")
                    await _invoke(on_head, block_number, block_hash)
            # Server closed the stream cleanly — treat as a disconnect.
            disconnected = "closed"
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — any socket error reconnects
            disconnected = str(exc)[:100]

        failures[url] = failures.get(url, 0) + 1
        metrics_incr("ws_head_reconnects")
        logger.warning(
            "ws_head_disconnected",
            chain=chain,
            endpoint=url,
            failures=failures[url],
            reason=disconnected,
        )
        if failures[url] >= failures_per_endpoint:
            index += 1
        if all(failures.get(candidate, 0) >= failures_per_endpoint for candidate in pool):
            metrics_incr("ws_head_fallback")
            logger.warning("ws_head_fallback", chain=chain, endpoints=len(pool))
            return
        delay = _jittered_delay(reconnect_base_seconds, reconnect_max_seconds, failures[url])
        await sleep(delay)


__all__ = ["HeadCallback", "WsConnect", "subscribe_heads"]
