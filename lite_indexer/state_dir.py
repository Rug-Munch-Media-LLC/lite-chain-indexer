"""Resolve the directory a state reader/writer should use.

Own module so ``state.py`` stays under its shrink-only size ratchet, and so
the "explicit value wins, otherwise read the global at CALL time" rule has one
definition instead of three.
"""

from __future__ import annotations

import sys
from pathlib import Path

from lite_indexer.config import STATE_DIR


def resolve_state_dir(explicit: Path | str | None) -> Path:
    """Return the state directory to use.

    An explicit value wins. Otherwise the module-level ``STATE_DIR`` is read
    at call time, not captured at import, so a test or a
    ``LiteIndexer(state_dir=...)`` can point a state class at a temp path
    without rewriting the process-wide global underneath it.

    Args:
        explicit: A caller-supplied directory, or ``None`` for the default.

    Returns:
        The directory to read and write.
    """
    if explicit is not None:
        return Path(explicit)
    # Read through the ALREADY-IMPORTED state module so patching
    # ``lite_indexer.state.STATE_DIR`` still redirects the directory (the
    # documented patch point, used by 5 test files). A sys.modules lookup is
    # used deliberately: `from lite_indexer import state` would execute
    # lite_indexer/__init__.py, which imports the indexer, which imports this
    # module back — a cycle the import-boundaries gate correctly rejects.
    mod = sys.modules.get("lite_indexer.state")
    default = getattr(mod, "STATE_DIR", STATE_DIR) if mod is not None else STATE_DIR
    return Path(default)


__all__ = ["StateDirMixin", "resolve_state_dir"]


class StateDirMixin:
    """Gives a state class a ``state_dir`` that honours a caller-supplied value.

    Each state class grew a second ctor parameter and a resolve call purely to
    pin its directory. The mixin does it once, so ``state.py`` gains no
    constructor lines per class and stays under its shrink-only ratchet.
    """

    def _init_state_dir(self, state_dir: Path | str | None) -> Path:
        """Resolve and store this object's state directory.

        Args:
            state_dir: Caller-supplied directory, or ``None`` for the default.

        Returns:
            The resolved directory, also stored on ``self.state_dir``.
        """
        self.state_dir = resolve_state_dir(state_dir)
        return self.state_dir
