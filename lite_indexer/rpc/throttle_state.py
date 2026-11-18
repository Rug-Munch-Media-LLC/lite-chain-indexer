"""Disk persistence for the RPC throttle tracker.

Single-writer JSON: load is best-effort (a corrupt file degrades to an empty
tracker) and writes are atomic (temp file + ``os.replace``), so a restart
mid-429-window still knows which endpoints told us to back off.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict

from lite_indexer.logging import get_logger
from lite_indexer.rpc.throttle_base import EndpointState, ThrottleBase

logger = get_logger(__name__)


class ThrottlePersistenceMixin(ThrottleBase):
    """Load / persist the throttled-endpoint state."""

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            for endpoint, data in raw.items():
                if not isinstance(data, dict):
                    continue
                data["endpoint"] = endpoint
                self._state[endpoint] = EndpointState(**data)
            if self._state:
                logger.debug("rpc_throttle_loaded", endpoints=len(self._state), path=str(self._path))
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.warning("rpc_throttle_load_failed", path=str(self._path), error=str(exc)[:80])

    def persist(self) -> None:
        """Atomically write the tracker state to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {ep: asdict(state) for ep, state in self._state.items()},
                indent=2,
            )
            tmp = self._path.with_suffix(".json.tmp")
            tmp.write_text(payload)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("rpc_throttle_save_failed", path=str(self._path), error=str(exc)[:80])

    def persist_if_stale(self, now: float | None = None) -> None:
        """Persist when the last write is older than the interval.

        Called after every recorded response so a crash loses at most
        ``persist_interval`` seconds of throttle state (a restart mid-429
        window still knows to back off).

        Args:
            now: Epoch time override (tests).
        """
        now = time.time() if now is None else now
        if now - self._last_persist >= self._persist_interval:
            self._last_persist = now
            self.persist()


__all__ = ["ThrottlePersistenceMixin"]
