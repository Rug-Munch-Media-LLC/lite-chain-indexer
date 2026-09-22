"""Tool seams for Parquet export (rclone lookup + subprocess execution).

Dependency-injection seams so tests and ops never monkeypatch module
internals: call sites read the injected fakes, or fall back to the real
``shutil.which`` / ``subprocess.run``.
"""
from __future__ import annotations

import shutil
import subprocess
from collections.abc import Callable
from typing import Any

WhichFn = Callable[[str], str | None]
RunFn = Callable[..., Any]

_WHICH: WhichFn | None = None
_RUN: RunFn | None = None


def set_tool_seams(which: WhichFn | None = None, run: RunFn | None = None) -> None:
    """Inject which()/run() fakes (None restores the real implementations)."""
    global _WHICH, _RUN
    _WHICH, _RUN = which, run


def which(binary: str) -> str | None:
    """``shutil.which``, or the injected fake."""
    if _WHICH is not None:
        return _WHICH(binary)
    return shutil.which(binary)


def run(cmd: list[str], **kwargs: Any) -> Any:
    """``subprocess.run``, or the injected fake."""
    if _RUN is not None:
        return _RUN(cmd, **kwargs)
    return subprocess.run(cmd, **kwargs)
