"""cgroup v2 memory reader — one correct resolver, loud on failure.

2026-10-24 production post-mortem: the previous resolver treated the cgroup
ROOT (``/proc/self/cgroup`` = ``0::/``) as unreadable and returned ``None``.
Inside a container with a private cgroup namespace the process always sits at
the root, so EVERY container deployment silently read 0 bytes for
``memory.current`` and 0 MB for ``memory.max``:

* ``lite_indexer_cgroup_memory_bytes`` scraped as 0.0 (alert rule blind),
* the recycle clamp never applied (guard believed "no cap known"),
* ``memory_pause_above_recycle`` fired every watchdog tick (config warning
  that looked like a pause gate firing — it never was).

Policy: a gate that pauses on an UNREADABLE file is worse than no gate.
Readers here return ``0`` (metric) or ``0`` (no-cap) on failure and emit ONE
loud warning per process per failure class, so the daemon keeps ingesting and
the operator gets a single actionable line instead of silence or log spam.
"""

from __future__ import annotations

import os

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

CGROUP_ROOT: str = "/sys/fs/cgroup"
PROC_CGROUP: str = "/proc/self/cgroup"

#: Event names already logged at WARNING by this process (spam guard).
_WARNED: set[str] = set()


def warn_once(event: str, **fields: object) -> None:
    """Log ``event`` at WARNING the first time this process sees it."""
    if event in _WARNED:
        return
    _WARNED.add(event)
    logger.warning(event, **fields)


def reset_warnings() -> None:
    """Clear the once-per-process warning set (tests)."""
    _WARNED.clear()


def cgroup_dir(root: str | None = None, proc: str | None = None) -> str | None:
    """Directory holding this process's cgroup v2 ``memory.*`` files.

    Parses the ``0::<path>`` line of ``/proc/self/cgroup``. An empty path
    (``0::/``) means the process IS at the cgroup root — which is the normal
    case inside a container with its own cgroup namespace — and resolves to
    ``<root>`` itself, NOT to a failure.

    Returns ``None`` (after a single loud warning) only when cgroup v2 is
    genuinely unavailable: no ``0::`` line, no memory controller file at the
    resolved directory, or an unreadable ``/proc/self/cgroup``.
    """
    base = root if root is not None else CGROUP_ROOT
    proc_path = proc if proc is not None else PROC_CGROUP
    try:
        with open(proc_path) as f:
            lines = f.read().splitlines()
    except OSError as exc:
        warn_once("cgroup_proc_unreadable", path=proc_path, error=str(exc)[:100])
        return None
    for line in lines:
        parts = line.split(":", 2)
        if len(parts) == 3 and parts[0] == "0" and parts[1] == "":
            rel = parts[2].strip("/")
            resolved = os.path.join(base, rel) if rel else base
            if not os.path.exists(os.path.join(resolved, "memory.current")):
                warn_once("cgroup_memory_controller_missing", path=resolved)
                return None
            return resolved
    warn_once("cgroup_v2_unavailable", path=proc_path)
    return None


def read_bytes(filename: str, root: str | None = None, proc: str | None = None) -> int | None:
    """Raw byte value of ``<cgroup_dir>/<filename>``; ``None`` when unknown.

    ``memory.max`` may contain the literal ``max`` (no limit) — reported as
    ``None`` so callers treat it as "no cap known" and fail OPEN.
    """
    directory = cgroup_dir(root, proc)
    if directory is None:
        return None
    path = os.path.join(directory, filename)
    try:
        with open(path) as f:
            content = f.read().strip()
    except OSError as exc:
        warn_once(f"cgroup_{filename}_unreadable", path=path, error=str(exc)[:100])
        return None
    if content == "max":
        return None
    try:
        return int(content)
    except ValueError:
        warn_once(f"cgroup_{filename}_garbage", path=path, value=content[:32])
        return None


def read_mb(filename: str, root: str | None = None, proc: str | None = None) -> int:
    """``<cgroup_dir>/<filename>`` in MiB; 0 when unreadable/unlimited."""
    raw = read_bytes(filename, root, proc)
    return 0 if raw is None else int(raw / (1024 * 1024))
