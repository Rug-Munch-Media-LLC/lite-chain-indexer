"""Bounded off-loop executor for synchronous ClickHouse writes.

The ClickHouse native client (``clickhouse_driver``) is blocking, and so is
the row-building/serialization around every insert. Running those calls
inline on the asyncio event loop serializes all chains behind one chain's
insert — py-spy sampling of the live daemon (2026-09-13) attributed ~53% of
GIL-holding samples to ClickHouse serialization and ~43% to address-activity
row-building, all on the loop thread.

:func:`run_ch_write` moves a blocking write callable onto a dedicated,
**bounded** thread pool (``CH_WRITE_THREADS``, default 4) so the loop keeps
servicing other chains while a batch is serialized/sent. A per-loop
semaphore caps in-flight submissions and provides backpressure, so a burst
of writes cannot grow the pool's work queue without bound.

Connections: ``ClickHouseWriter`` keeps ONE client per worker thread
(thread-local), because ``clickhouse_driver.Client`` is not thread-safe for
concurrent use of a single connection. The pool is intentionally small so
the connection count stays bounded.
"""

from __future__ import annotations

import asyncio
import functools
import threading
import weakref
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from lite_indexer.config import CH_WRITE_THREADS

_T = TypeVar("_T")

_pool: ThreadPoolExecutor | None = None
_pool_lock = threading.Lock()
# One semaphore per running event loop (tests create/dispose loops); weak
# keys so a finished loop does not leak. Only touched from the loop thread.
_semaphores: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore] = weakref.WeakKeyDictionary()


def _executor() -> ThreadPoolExecutor:
    """Return the process-wide bounded ClickHouse write executor."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = ThreadPoolExecutor(
                    max_workers=CH_WRITE_THREADS,
                    thread_name_prefix="ch-write",
                )
    return _pool


def _semaphore() -> asyncio.Semaphore:
    """Per-loop backpressure semaphore (at most ``CH_WRITE_THREADS`` in flight)."""
    loop = asyncio.get_running_loop()
    sem = _semaphores.get(loop)
    if sem is None:
        sem = asyncio.Semaphore(CH_WRITE_THREADS)
        _semaphores[loop] = sem
    return sem


async def run_ch_write(fn: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Run a blocking ClickHouse write ``fn`` in the bounded write pool.

    Args:
        fn: Blocking callable (e.g. ``ClickHouseWriter.write_transfers``,
            ``strategy.write_clickhouse``). Exceptions propagate unchanged.
        *args: Positional args for ``fn``.
        **kwargs: Keyword args for ``fn``.

    Returns:
        Whatever ``fn`` returned.
    """
    loop = asyncio.get_running_loop()
    call = functools.partial(fn, *args, **kwargs)
    async with _semaphore():
        return await loop.run_in_executor(_executor(), call)


__all__ = ["run_ch_write"]
