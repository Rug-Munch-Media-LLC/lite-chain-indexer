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
    CGROUP_MEMORY_PATH,
    CH_HEALTH_CHECK_ENABLED,
    CH_HEALTH_INTERVAL,
    MEMORY_CHECK_INTERVAL,
    MEMORY_HIGH_MB,
    MEMORY_PAUSE_SLEEP,
)
from lite_indexer.indexer._cgroup import cgroup_dir, read_mb
from lite_indexer.logging import get_logger
from lite_indexer.storage import ClickHouseWriter

logger = get_logger(__name__)

# Sysfs root for cgroup v2 files; overridable in tests.
_CGROUP_ROOT: str = "/sys/fs/cgroup"
# Procfs cgroup listing; overridable in tests.
_PROC_CGROUP: str = "/proc/self/cgroup"


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


def _cgroup_v2_path(root: str | None = None) -> str | None:
    """Resolve this process's cgroup v2 ``memory.current`` path.

    Delegates to :func:`lite_indexer.indexer._cgroup.cgroup_dir`, which also
    handles the cgroup-ROOT case (``0::/``) that made every containerised
    deployment read 0 bytes (2026-10-24 post-mortem). ``None`` when cgroup v2
    is genuinely unavailable, so callers fall back to RSS.
    """
    directory = cgroup_dir(root if root is not None else _CGROUP_ROOT, _PROC_CGROUP)
    return os.path.join(directory, "memory.current") if directory else None


def get_cgroup_memory_mb(root: str | None = None) -> int:
    """Return the cgroup v2 ``memory.current`` of this process's cgroup in MiB.

    This is the metric the kernel OOM killer acts on (cgroup charge:
    anon + file cache + kernel + sock), unlike process RSS. Returns 0 on
    any failure so callers fall back to the RSS reader.
    """
    if CGROUP_MEMORY_PATH:
        try:
            with open(CGROUP_MEMORY_PATH) as f:
                return int(int(f.read().strip()) / (1024 * 1024))
        except (OSError, ValueError):
            return 0
    # read_mb() warns ONCE per process when the file cannot be read, then
    # returns 0: the guard fails OPEN to RSS instead of pausing on a
    # fabricated value (2026-10-24 post-mortem).
    return read_mb("memory.current", root if root is not None else _CGROUP_ROOT, _PROC_CGROUP)


def _public_cgroup_reader() -> int:
    """Cgroup memory via the public ``lite_indexer.indexer.get_cgroup_memory_mb`` binding.

    Mirrors :func:`_public_rss_reader`: tests patch the package re-export,
    so resolve the reader at call time through the package when available.
    """
    module = sys.modules.get("lite_indexer.indexer")
    reader = getattr(module, "get_cgroup_memory_mb", None) if module is not None else None
    if reader is not None:
        return cast(Callable[[], int], reader)()
    return get_cgroup_memory_mb()


def get_guarded_memory_mb() -> int:
    """Effective memory metric the watchdog guards: max(RSS, cgroup charge).

    The kernel cgroup OOM killer fires on ``memory.current``, not process
    RSS (2026-09-13 OOM: anon-RSS 5.34 GiB < 6 GiB gate while the cgroup
    crossed the 7 GiB MemoryMax). Guarding the max of both metrics closes
    the gate on whichever is higher; RSS remains the fallback when the
    cgroup file is unavailable (returns 0).
    """
    return max(_public_rss_reader(), _public_cgroup_reader())


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
        mem = get_guarded_memory_mb()
        self._cached_ok = mem < self._high_mb
        if not self._cached_ok:
            logger.warning(
                "memory_gate_closed",
                rss_mb=mem,
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
