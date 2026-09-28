"""Resolve the directory a state reader/writer should use.

Own module so ``state.py`` stays under its shrink-only size ratchet, and so
the "explicit value wins, otherwise read the global at CALL time" rule has one
definition instead of three.
"""

from __future__ import annotations

from pathlib import Path


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
    # Read through the state module so patching ``lite_indexer.state.STATE_DIR``
    # still redirects the directory — the documented patch point for this repo.
    from lite_indexer import state as _state

    return Path(_state.STATE_DIR)


__all__ = ["StateDirMixin", "resolve_state_dir"]


class StateDirMixin:
    """Gives a state class a ``state_dir`` that honours a caller-supplied value.

    Each state class grew a second ctor parameter and a resolve call purely to
    pin its directory. The mixin does it once, so ``state.py`` gains no
    constructor lines per class and stays under its shrink-only ratchet.
    """

    def _init_state_pair(self, state_dir: Path | str | None) -> tuple[object, object]:
        """Build the two state stores this owner needs, on one directory.

        Args:
            state_dir: Caller-supplied directory, or ``None`` for the default.

        Returns:
            ``(StateManager, StateHighWater)`` pinned to the same directory.
        """
        from lite_indexer.state import StateHighWater
        from lite_indexer.state_manager import StateManager

        resolved = self._init_state_dir(state_dir)
        return StateManager(self.chain, resolved), StateHighWater(self.chain, resolved)

    def _init_state_dir(self, state_dir: Path | str | None) -> Path:
        """Resolve and store this object's state directory.

        Args:
            state_dir: Caller-supplied directory, or ``None`` for the default.

        Returns:
            The resolved directory, also stored on ``self.state_dir``.
        """
        self.state_dir = resolve_state_dir(state_dir)
        return self.state_dir
