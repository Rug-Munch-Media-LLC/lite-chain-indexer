"""
Optional ``msgspec`` JSON fast path for hot RPC response parsing.

Every ``eth_getLogs`` / ``eth_call`` / ``eth_blockNumber`` response is JSON
decoded. ``msgspec.json.decode`` is a C-accelerated decoder that is several
times faster than the stdlib on large payloads; when it is importable we run
it on the response BYTES, otherwise this is exactly ``json.loads``.

Parity contract: on ANY msgspec error/edge case we fall back to stdlib
``json.loads``, so success values and failure semantics (error dicts, missing
fields, malformed, non-UTF8, empty) are identical to the previous behaviour.
``msgspec`` is optional (the ``perf`` extra).
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from lite_indexer.config import JSON_DECODE_THREADS

_decoder: Callable[[bytes], Any] | None
try:
    import msgspec

    _decoder = msgspec.json.decode
except ImportError:  # pragma: no cover — optional dependency
    _decoder = None


def msgspec_available() -> bool:
    """Whether the msgspec fast path is active."""
    return _decoder is not None


def decode_bytes(data: bytes) -> Any:
    """Decode JSON ``bytes`` with msgspec when available, else stdlib.

    Falls back to ``json.loads`` on any msgspec failure so the result and
    the raised exception type are identical to the stdlib path.
    """
    if _decoder is not None:
        try:
            return _decoder(data)
        except Exception:  # noqa: BLE001 — fall back for byte-identical parity
            pass
    return json.loads(data)


def decode_response(resp: Any) -> Any:
    """Decode an httpx-like response, preferring the raw-byte fast path.

    Uses ``resp.content`` (bytes) through :func:`decode_bytes` when present
    and non-empty; otherwise defers to ``resp.json()`` so lightweight test
    doubles without a ``content`` attribute keep working unchanged.
    """
    content = getattr(resp, "content", None)
    if isinstance(content, (bytes, bytearray)) and content:
        return decode_bytes(bytes(content))
    return resp.json()


# ── Event-loop offload ────────────────────────────────────────────────
# Decode is a synchronous C call. Doing it inline on the event loop
# serializes every chain's decode behind the others (head-of-line
# blocking); a burst of large eth_getLogs responses can stall the loop
# for >100ms. These async wrappers run the same decoder in a dedicated,
# bounded thread pool so the loop keeps servicing other chains' I/O.
# The pool is created lazily and capped at JSON_DECODE_THREADS (never
# unbounded); behaviour/return values are identical to the sync path.
_decode_pool: ThreadPoolExecutor | None = None
_decode_pool_lock = threading.Lock()


def _decode_executor() -> ThreadPoolExecutor:
    """Return the process-wide bounded decode executor (created once)."""
    global _decode_pool
    if _decode_pool is None:
        with _decode_pool_lock:
            if _decode_pool is None:
                _decode_pool = ThreadPoolExecutor(
                    max_workers=JSON_DECODE_THREADS,
                    thread_name_prefix="json-decode",
                )
    return _decode_pool


async def decode_bytes_async(data: bytes) -> Any:
    """Off-loop variant of :func:`decode_bytes` (same result, same errors)."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(_decode_executor(), decode_bytes, data)


async def decode_response_async(resp: Any) -> Any:
    """Off-loop variant of :func:`decode_response`.

    ``resp.content`` byte payloads (the real httpx path) are decoded in the
    bounded pool; lightweight doubles without ``content`` fall back to the
    synchronous ``resp.json()`` exactly as before.
    """
    content = getattr(resp, "content", None)
    if isinstance(content, (bytes, bytearray)) and content:
        return await decode_bytes_async(bytes(content))
    return resp.json()


__all__ = [
    "decode_bytes",
    "decode_bytes_async",
    "decode_response",
    "decode_response_async",
    "msgspec_available",
]
