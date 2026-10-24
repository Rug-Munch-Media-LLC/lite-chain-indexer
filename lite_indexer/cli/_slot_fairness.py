"""Persisted fairness counters for the slot rotation.

Why this exists (2026-10-24 stall)
----------------------------------
``SlotRotation`` grants the next free slot to the chain served the FEWEST
times. That table lived only in the daemon's memory, and CI recreated the
container every ~5-8 min, so the table reset to all-zero on each restart and
the chains that had waited longest were re-queued behind newcomers. The
rotation was fair inside a process and unfair across processes.

The counters are two small facts (``served`` per chain, ``arrivals``) used
ONLY to order grants, so persistence is best-effort by design: a missing,
corrupt, or unwritable file degrades to the previous in-memory behaviour and
must never break or delay the daemon. Written atomically (tmp + ``os.replace``)
so a restart mid-write cannot leave a truncated table.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from lite_indexer.config import STATE_DIR
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class SlotFairnessStore:
    """Load/save the rotation's least-served-first counters as JSON."""

    def __init__(self, path: str | Path) -> None:
        """Point the store at ``path``; nothing is read until :meth:`load`.

        Args:
            path: JSON file holding ``{"served": {chain: n}, "arrivals": n}``.
        """
        self._path = Path(path)

    def load(self) -> tuple[dict[str, int], int]:
        """Return ``(served, arrivals)`` from disk, or empty when unusable.

        Returns:
            The persisted counters, or ``({}, 0)`` when the file is missing,
            unreadable, or holds unexpected shapes.
        """
        try:
            raw = json.loads(self._path.read_text())
            served = {str(k): int(v) for k, v in (raw.get("served") or {}).items()}
            return served, int(raw.get("arrivals") or 0)
        except (OSError, ValueError, TypeError, AttributeError) as exc:
            logger.debug("slot_fairness_load_skipped", error=str(exc)[:80])
            return {}, 0

    def save(self, served: dict[str, int], arrivals: int) -> None:
        """Atomically persist the counters; never raise.

        Args:
            served: Chain -> number of grants since the counter was created.
            arrivals: Monotonic arrival sequence used to break fairness ties.
        """
        tmp = self._path.with_name(f".{self._path.name}.{os.getpid()}.tmp")
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps({"served": served, "arrivals": arrivals}))
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.debug("slot_fairness_save_skipped", error=str(exc)[:80])
            tmp.unlink(missing_ok=True)


def default_store() -> SlotFairnessStore:
    """Return the store the daemon should use (env override, else STATE_DIR)."""
    return SlotFairnessStore(os.environ.get("SLOT_FAIRNESS_STATE") or str(STATE_DIR / "slot_fairness.json"))


__all__ = ["SlotFairnessStore", "default_store"]
