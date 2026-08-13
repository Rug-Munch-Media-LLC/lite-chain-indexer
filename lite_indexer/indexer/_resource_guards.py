"""
LiteIndexer resource guards — memory / ClickHouse watchdog helpers.

Extracted from ``lite_indexer/indexer.py`` (Batch 11 degod) into a
cohesive module. Pure, self-contained helpers used by the indexer's sync
loop to pause ingestion before the process OOMs or ClickHouse becomes
unreachable:

  - :func:`get_process_rss_mb` — RSS of this process in MiB (procfs, no psutil)
  - :class:`MemoryGate` — cached in-process memory watchdog
  - :class:`ClickHouseHealth` — cached ClickHouse reachability probe

Public names are re-exported from ``lite_indexer.indexer`` so the
``LiteIndexer`` surface is unchanged.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Callable
from typing import cast

from lite_indexer.config import (
    CH_HEALTH_CHECK_ENABLED,
    CH_HEALTH_INTERVAL,
    MEMORY_CHECK_INTERVAL,
    MEMORY_HIGH_MB,
    MEMORY_PAUSE_SLEEP,
)
from lite_indexer.logging import get_logger
from lite_indexer.storage import ClickHouseWriter

logger = get_logger(__name__)


def get_process_rss_mb() -> int:
    """Return the current RSS of this process in MiB (no psutil needed).

    Reads ``/proc/self/statm`` (resident set size in pages) and
    multiplies by the page size. Falls back to 0 when the file is
    unavailable (non-Linux) so callers treat "unknown" as "under limit".

    Returns:
        Resident set size in MiB, or 0 when unavailable.
    """
    try:
        with open("/proc/self/statm") as f:
            fields = f.read().split()
        if len(fields) < 2:
            return 0
        pages = int(fields[1])
        page_size = os.sysconf("SC_PAGE_SIZE")
        return int(pages * page_size / (1024 * 1024))
    except (OSError, ValueError, AttributeError):
        return 0


def _public_rss_reader() -> int:
    """RSS via the public ``lite_indexer.indexer.get_process_rss_mb`` binding.

    ``MemoryGate`` lives in this module, but callers and tests patch
    ``lite_indexer.indexer.get_process_rss_mb`` (the package re-export).
    Resolving the reader at call time through the package keeps that
    patch surface intact; when the package binding is unavailable the
    local implementation is used.
    """
    module = sys.modules.get("lite_indexer.indexer")
    reader = getattr(module, "get_process_rss_mb", None) if module is not None else None
    if reader is not None:
        return cast(Callable[[], int], reader)()
    return get_process_rss_mb()


class MemoryGate:
    """Cached in-process memory watchdog (pause ingestion when RSS is high).

    The daemon checks :meth:`ok` before starting a new sync cycle. When
    RSS exceeds :data:`MEMORY_HIGH_MB` the gate reports ``False`` and the
    caller sleeps (:data:`MEMORY_PAUSE_SLEEP`) instead of ingesting —
    pausing new chain ingestion until memory drops, which is the
    in-process backstop before the systemd cgroup OOM reaper fires.

    The RSS read is cached for :data:`MEMORY_CHECK_INTERVAL` seconds so
    the gate doesn't thrash under GC churn.
    """

    def __init__(self, high_mb: int = MEMORY_HIGH_MB) -> None:
        self._high_mb = high_mb
        self._last_check: float = 0.0
        self._cached_ok: bool = True

    @property
    def high_mb(self) -> int:
        """The RSS threshold in MiB."""
        return self._high_mb

    def ok(self) -> bool:
        """Whether new ingestion may proceed (RSS under the threshold)."""
        if self._high_mb <= 0:
            return True
        now = time.monotonic()
        if now - self._last_check < MEMORY_CHECK_INTERVAL:
            return self._cached_ok
        self._last_check = now
        rss = _public_rss_reader()
        self._cached_ok = rss < self._high_mb
        if not self._cached_ok:
            logger.warning(
                "memory_gate_closed",
                rss_mb=rss,
                high_mb=self._high_mb,
                pause_s=MEMORY_PAUSE_SLEEP,
            )
        return self._cached_ok


class ClickHouseHealth:
    """Cached ClickHouse reachability probe for graceful CH pauses.

    ``ok()`` runs ``SELECT 1`` through the shared ClickHouseWriter client
    (lazy connect, 5s connect timeout) and caches the result for
    :data:`CH_HEALTH_INTERVAL` seconds. The sync loop calls this before
    fetching events from RPC: when CH is unreachable the cycle aborts
    with ``ch_unavailable`` — no RPC budget burned, no state movement,
    no crash.
    """

    def __init__(self, ch_writer: ClickHouseWriter | None = None) -> None:
        self._writer: ClickHouseWriter | None = ch_writer
        self._last_check: float = 0.0
        self._cached_ok: bool = True

    def ok(self) -> bool:
        """Whether ClickHouse is currently reachable and writable."""
        if not CH_HEALTH_CHECK_ENABLED:
            return True
        now = time.monotonic()
        if now - self._last_check < CH_HEALTH_INTERVAL:
            return self._cached_ok
        self._last_check = now
        writer = self._writer if self._writer is not None else ClickHouseWriter()
        try:
            client = writer.client()
            self._cached_ok = client is not None
        except Exception as exc:  # noqa: BLE001 — health probe must never raise
            logger.warning("ch_health_probe_failed", error=str(exc)[:100])
            self._cached_ok = False
        if not self._cached_ok:
            logger.warning("ch_health_down", next_probe_s=CH_HEALTH_INTERVAL)
        return self._cached_ok
