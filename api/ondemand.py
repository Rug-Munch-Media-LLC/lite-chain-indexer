"""On-demand RPC bridge for the sync FastAPI endpoints.

The structured REST endpoints (transfers, deployers) are synchronous —
FastAPI runs them on a threadpool so the blocking ClickHouse reads never
stall the event loop. The :class:`QueryRouter` is async and its
``RpcClient`` holds shared httpx clients bound to a *specific* event
loop, so we must not create a fresh loop per request (a client created
on loop A cannot be reused on loop B — it would raise "Event loop is
closed").

:func:`run_ondemand` bridges the two worlds with a single long-lived
worker thread running one event loop for the whole API process. Every
fallback call is dispatched onto that loop via
``asyncio.run_coroutine_threadsafe``; the calling (sync) thread blocks
on the result for at most ``ON_DEMAND_API_TIMEOUT`` seconds.

Guarantees:
* One stable event loop → the shared RPC clients work across calls.
* The sync endpoint thread never blocks longer than the timeout.
* Any failure (timeout, RPC error) degrades to the indexed-only result —
  on-demand routing is a fallback, never a request-killer.
"""

from __future__ import annotations

import asyncio
import os
import threading
from collections.abc import Coroutine
from typing import Any

_ON_DEMAND_API_TIMEOUT: float = float(os.environ.get("ON_DEMAND_API_TIMEOUT", "30.0"))

_loop: asyncio.AbstractEventLoop | None = None
_loop_lock = threading.Lock()
_loop_thread: threading.Thread | None = None


def _get_loop() -> asyncio.AbstractEventLoop:
    """Return the process-wide event loop, starting its worker thread once."""
    global _loop, _loop_thread
    with _loop_lock:
        if _loop is None or _loop.is_closed():
            _loop = asyncio.new_event_loop()

            def _run() -> None:
                asyncio.set_event_loop(_loop)
                _loop.run_forever()

            _loop_thread = threading.Thread(
                target=_run,
                name="ondemand-rpc-loop",
                daemon=True,
            )
            _loop_thread.start()
        assert _loop is not None  # noqa: S101 — guarded by the lock above
        return _loop


def run_ondemand(coro: Coroutine[Any, Any, Any], timeout: float = _ON_DEMAND_API_TIMEOUT) -> Any:
    """Run an async router coroutine on the dedicated loop and return its result.

    Blocks the calling thread until the coroutine finishes or ``timeout``
    elapses. A timeout raises :class:`TimeoutError` — callers should treat
    that as "no on-demand result" and keep the indexed-only answer.
    """
    loop = _get_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=timeout)


__all__ = ["run_ondemand"]
