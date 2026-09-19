"""Cgroup-cap memory recycle guard (2026-09-19 OOM post-mortem).

A transient systemd override set ``MemoryMax=4G`` while the unit declared
``MEMORY_RECYCLE_MB=5800`` — the in-process recycle guard could never fire
and the kernel OOM-killed the daemon instead of recycling gracefully.
Nothing in the code prevented recurrence (docker/k8s/future hosts can set
any cap), so the guard now reads the cgroup v2 ``memory.max`` hard cap and
clamps the declared recycle ceiling below it, logging loudly when it does.

INVARIANT: MEMORY_RECYCLE_MB must sit below the cgroup hard cap. NEVER
reintroduce a kernel memory.high drop-in — it wedged the daemon in
D-state for 5h45m (removed 2026-09-14).
"""

from __future__ import annotations

import os

from lite_indexer.config import CGROUP_MEMORY_PATH, MEMORY_HIGH_MB
from lite_indexer.indexer._resource_guards import _cgroup_v2_path
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


def _cgroup_v2_dir() -> str | None:
    """Directory of this process's cgroup v2 memory controller files.

    Same resolution as :func:`get_cgroup_memory_mb` (``CGROUP_MEMORY_PATH``
    or the auto-detected ``memory.current`` sibling); ``None`` on failure.
    """
    path = CGROUP_MEMORY_PATH or _cgroup_v2_path()
    if not path:
        return None
    return os.path.dirname(path)


def cgroup_memory_max_mb(path: str | None = None) -> int:
    """Return the cgroup v2 ``memory.max`` hard cap in MiB (0 = no cap).

    Returns 0 when the file is missing/unreadable, contains garbage, or
    contains ``max`` (unlimited) — callers treat 0 as "no cap known".
    Accepts an explicit ``path=`` seam so tests need no patching.
    """
    if path is None:
        cgroup_dir = _cgroup_v2_dir()
        if not cgroup_dir:
            return 0
        path = os.path.join(cgroup_dir, "memory.max")
    try:
        with open(path) as f:
            content = f.read().strip()
    except OSError as exc:
        logger.debug("cgroup_memory_max_unreadable", path=path, error=str(exc)[:100])
        return 0
    if content == "max":
        return 0
    try:
        return int(int(content) / (1024 * 1024))
    except ValueError as exc:
        logger.debug("cgroup_memory_max_garbage", path=path, error=str(exc)[:100])
        return 0


def clamp_recycle_mb(declared_mb: int, cap_mb: int) -> int:
    """Clamp a declared recycle ceiling below a cgroup hard cap (PURE).

    The ceiling keeps a safety margin of ``max(512, 15%)`` below the cap.
    ``declared_mb <= 0`` (disabled) or ``cap_mb <= 0`` (no cap known)
    returns ``declared_mb`` unchanged.
    """
    if declared_mb <= 0 or cap_mb <= 0:
        return declared_mb
    ceiling = cap_mb - max(512, int(cap_mb * 0.15))
    if declared_mb <= ceiling:
        return declared_mb
    return max(1, ceiling)


def resolve_recycle_mb(
    cap_mb: int | None = None,
    declared_mb: int | None = None,
) -> int:
    """Effective self-recycle ceiling: declared MEMORY_RECYCLE_MB clamped
    below the cgroup ``memory.max`` hard cap.

    One cgroup file read per call (cheap enough for the watchdog to
    re-resolve every tick, so it auto-adapts if the cap changes). Logs
    ``memory_recycle_clamped`` when clamping changes the value and
    ``memory_pause_above_recycle`` when MEMORY_HIGH_MB would make the
    pause gate dead code. ``cap_mb=``/``declared_mb=`` are explicit seams
    for tests (defaults: cgroup read / configured MEMORY_RECYCLE_MB).
    """
    from lite_indexer.config import indexing as _indexing_cfg

    declared = int(_indexing_cfg.MEMORY_RECYCLE_MB) if declared_mb is None else declared_mb
    cap = cgroup_memory_max_mb() if cap_mb is None else cap_mb
    effective = clamp_recycle_mb(declared, cap)
    if declared > 0 and effective != declared:
        logger.error("memory_recycle_clamped", declared_mb=declared, cgroup_cap_mb=cap, effective_mb=effective)
    if 0 < effective <= MEMORY_HIGH_MB:
        logger.warning("memory_pause_above_recycle", high_mb=MEMORY_HIGH_MB, recycle_mb=effective)
    return effective
