"""
State file management and pair cache persistence.

Manages:
  - LiteIndexer sync state (last indexed block per chain)
  - Monotonic high-water mark per chain (backward-state guard)
  - Pair cache (pair→(token0, token1) mapping across restarts)
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from lite_indexer.config import PAIR_CACHE_MAX
from lite_indexer.config import STATE_DIR as STATE_DIR
from lite_indexer.logging import get_logger
from lite_indexer.state_dir import StateDirMixin
from lite_indexer.util.bounded import BoundedDict

logger = get_logger(__name__)


def atomic_write_json(path: Path, payload: dict[str, Any] | str) -> None:
    """Atomically write a JSON payload to ``path``.

    Multi-writer safe:
      - Writes to a **unique** temp file (``.name.<pid>.<uuid>.tmp``) so
        two processes writing the same state file never clobber each
        other's temp file (the old shared ``.json.tmp`` name had a race:
        writer A's tmp could be truncated by writer B before A renamed).
      - Serializes writers with an exclusive ``fcntl.flock`` on the temp
        file (same-filesystem rename is atomic regardless, but the lock
        prevents read-after-partial-write from a lagging writer and makes
        the ordering deterministic).
      - ``fsync`` before the atomic ``os.replace`` so the rename never
        publishes a partially-flushed payload after a crash.

    The reader side (``json.loads(path.read_text())``) always sees either
    the old complete file or the new complete file — never a torn write.

    Args:
        path: Destination file path.
        payload: JSON-serializable dict or pre-serialized JSON string.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    try:
        data = payload if isinstance(payload, str) else json.dumps(payload, indent=2)
        with open(tmp, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
            fcntl.flock(f, fcntl.LOCK_UN)
        os.replace(tmp, path)
    finally:
        # Best-effort cleanup if the rename never happened (crash/error).
        with open(tmp, "a"):  # ensure the file exists before unlinking
            pass
        with contextlib.suppress(OSError):
            tmp.unlink()


class StateManager(StateDirMixin):
    """Persistent state management for LiteIndexer.

    Thread-safe for cron usage because every run reads/writes its own state file
    with file locking.
    """

    def __init__(self, chain: str, state_dir: Path | str | None = None) -> None:
        self.chain, _ = chain, self._init_state_dir(state_dir)

    def _state_path(self) -> Path:
        return self.state_dir / f"lite_{self.chain}.json"

    def load(self) -> dict[str, Any]:
        """Load last indexed block from state file."""
        path = self._state_path()
        if not path.exists():
            logger.info("no_state_file", chain=self.chain, path=str(path))
            return {}
        try:
            data = json.loads(path.read_text())
            logger.info(
                "state_loaded",
                chain=self.chain,
                last_block=data.get("last_block"),
            )
            return cast(dict[str, Any], data)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("state_load_failed", chain=self.chain, error=str(exc))
            return {}

    def save(
        self,
        last_block: int,
        events_count: int,
        source: str = "lite_indexer",
        enabled_events: list[str] | None = None,
    ) -> None:
        """Save last indexed block to state file (with file locking).

        Args:
            last_block: Last indexed block number.
            events_count: Number of events indexed in the cycle.
            source: Who wrote the state (default ``"lite_indexer"``).
            enabled_events: Event types enabled for this chain under
                lazy indexing (e.g. ``["dex", "deployer"]``). Persisted so
                DEX/deployer stay enabled across restarts once demand is
                observed.
        """
        self.state_dir.mkdir(parents=True, exist_ok=True)
        state: dict[str, Any] = {
            "last_block": last_block,
            "last_timestamp": datetime.now(UTC).isoformat(),
            "events_indexed": events_count,
            "source": source,
            "chain": self.chain,
        }
        if enabled_events:
            state["enabled_events"] = sorted(enabled_events)
        try:
            path = self._state_path()
            # Atomic write: unique temp file in the same directory then
            # os.replace() into place. A plain open("w") + write can be
            # torn by an OOM kill mid-write, truncating the JSON and causing
            # the next load to fall back to {} → restart from block ~0
            # (observed 08/03: ethereum state reset to block 100 while the
            # ClickHouse max was 25.6M, triggering a genesis backfill).
            atomic_write_json(path, state)
            logger.info("state_saved", chain=self.chain, last_block=last_block)
        except OSError as exc:
            logger.error("state_save_failed", chain=self.chain, error=str(exc))


class StateHighWater(StateDirMixin):
    """Monotonic per-chain high-water mark (backward-state guard).

    The regular state file (``lite_{chain}.json``) is written by the
    indexer on every cycle and CAN go backwards — a torn write, a
    corrupted JSON (fallback to ``{}``), or a buggy companion writer
    resets ``last_block`` and the indexer re-indexes from genesis.

    The high-water mark file (``highwater_{chain}.json``) only ever
    moves forward: it is updated after every successful forward sync and
    never rewound. ``LiteIndexer`` consults it on load (when
    ``STATE_BACKWARD_PROTECTION`` is enabled) to detect a regression and
    recover from the ClickHouse max block instead of re-indexing from 0.
    """

    def __init__(self, chain: str, state_dir: Path | str | None = None) -> None:
        self.chain, _ = chain, self._init_state_dir(state_dir)

    def _path(self) -> Path:
        return self.state_dir / f"highwater_{self.chain}.json"

    def load(self) -> int | None:
        """Return the highest last_block ever recorded, or None."""
        path = self._path()
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            block = data.get("last_block")
            return int(block) if block is not None else None
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.warning("highwater_load_failed", chain=self.chain, error=str(exc)[:80])
            return None

    def update(self, last_block: int) -> None:
        """Advance the high-water mark (no-op when going backwards).

        Args:
            last_block: The block just successfully indexed.
        """
        current = self.load()
        if current is not None and last_block <= current:
            return
        try:
            self.state_dir.mkdir(parents=True, exist_ok=True)
            path = self._path()
            payload = {
                "last_block": last_block,
                "last_timestamp": datetime.now(UTC).isoformat(),
                "chain": self.chain,
            }
            atomic_write_json(path, payload)
            logger.info("highwater_updated", chain=self.chain, last_block=last_block)
        except OSError as exc:
            logger.warning("highwater_save_failed", chain=self.chain, error=str(exc)[:80])


class PairCache(StateDirMixin):
    """Persistent pair→(token0, token1) cache.

    Built from PairCreated/PoolCreated events and eth_call fallback.
    Survives restarts so token resolution is never lost.
    """

    def __init__(self, chain: str, state_dir: Path | str | None = None) -> None:
        self.chain, _ = chain, self._init_state_dir(state_dir)
        # Bounded (2026-09-13): pair keys grow without limit; LRU-evict oldest.
        self._pairs: BoundedDict[str, tuple[str, str]] = BoundedDict(PAIR_CACHE_MAX)
        self._load()

    def _cache_path(self) -> Path:
        return self.state_dir / f"pair_tokens_{self.chain}.json"

    def _load(self) -> None:
        """Load persisted pair→tokens mapping from disk."""
        path = self._cache_path()
        if not path.exists():
            logger.debug("pair_cache_not_found", chain=self.chain, path=str(path))
            self._pairs.clear()
            return
        try:
            data = json.loads(path.read_text())
            loaded = {k: (v[0], v[1]) if isinstance(v, list) else v for k, v in data.items()}
            self._pairs = BoundedDict(PAIR_CACHE_MAX, loaded)
            logger.info("pair_cache_loaded", chain=self.chain, pairs=len(self._pairs))
        except (json.JSONDecodeError, OSError, TypeError) as exc:
            logger.warning("pair_cache_load_failed", chain=self.chain, error=str(exc)[:80])
            self._pairs.clear()

    def save(self) -> None:
        """Persist pair→tokens mapping to disk (atomic)."""
        if not self._pairs:
            return
        try:
            path = self._cache_path()
            atomic_write_json(path, dict(self._pairs))
            logger.debug("pair_cache_saved", chain=self.chain, pairs=len(self._pairs))
        except (OSError, TypeError) as exc:
            logger.warning("pair_cache_save_failed", chain=self.chain, error=str(exc)[:80])

    def get(self, pair: str) -> tuple[str, str] | None:
        """Get token0/token1 for a pair address."""
        return self._pairs.get(pair.lower())

    def set(self, pair: str, tokens: tuple[str, str]) -> None:
        """Set token0/token1 for a pair address."""
        self._pairs[pair.lower()] = tokens

    def update(self, mapping: dict[str, tuple[str, str]]) -> None:
        """Bulk-update pairs from a mapping."""
        self._pairs.update(mapping)

    @property
    def all(self) -> dict[str, tuple[str, str]]:
        """Get all pairs (snapshot)."""
        return dict(self._pairs)

    @property
    def count(self) -> int:
        """Number of cached pairs."""
        return len(self._pairs)

    def __contains__(self, pair: str) -> bool:
        return pair.lower() in self._pairs

    def __getitem__(self, pair: str) -> tuple[str, str]:
        return self._pairs[pair.lower()]

    def __len__(self) -> int:
        return len(self._pairs)
