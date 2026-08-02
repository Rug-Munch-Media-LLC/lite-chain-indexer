"""
State file management and pair cache persistence.

Manages:
  - LiteIndexer sync state (last indexed block per chain)
  - Pair cache (pair→(token0, token1) mapping across restarts)
"""

from __future__ import annotations

import fcntl
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from lite_indexer.config import STATE_DIR
from lite_indexer.logging import get_logger

logger = get_logger(__name__)


class StateManager:
    """Persistent state management for LiteIndexer.

    Thread-safe for cron usage because every run reads/writes its own state file
    with file locking.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain

    def _state_path(self) -> Path:
        return STATE_DIR / f"lite_{self.chain}.json"

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

    def save(self, last_block: int, events_count: int, source: str = "lite_indexer") -> None:
        """Save last indexed block to state file (with file locking)."""
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        state: dict[str, Any] = {
            "last_block": last_block,
            "last_timestamp": datetime.now(UTC).isoformat(),
            "events_indexed": events_count,
            "source": source,
            "chain": self.chain,
        }
        try:
            path = self._state_path()
            with open(path, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                f.write(json.dumps(state, indent=2))
                fcntl.flock(f, fcntl.LOCK_UN)
            logger.info("state_saved", chain=self.chain, last_block=last_block)
        except OSError as exc:
            logger.error("state_save_failed", chain=self.chain, error=str(exc))


class PairCache:
    """Persistent pair→(token0, token1) cache.

    Built from PairCreated/PoolCreated events and eth_call fallback.
    Survives restarts so token resolution is never lost.
    """

    def __init__(self, chain: str) -> None:
        self.chain = chain
        self._pairs: dict[str, tuple[str, str]] = {}
        self._load()

    def _cache_path(self) -> Path:
        return STATE_DIR / f"pair_tokens_{self.chain}.json"

    def _load(self) -> None:
        """Load persisted pair→tokens mapping from disk."""
        path = self._cache_path()
        if not path.exists():
            logger.debug("pair_cache_not_found", chain=self.chain, path=str(path))
            self._pairs = {}
            return
        try:
            data = json.loads(path.read_text())
            self._pairs = {k: (v[0], v[1]) if isinstance(v, list) else v for k, v in data.items()}
            logger.info("pair_cache_loaded", chain=self.chain, pairs=len(self._pairs))
        except (json.JSONDecodeError, OSError, TypeError) as exc:
            logger.warning("pair_cache_load_failed", chain=self.chain, error=str(exc)[:80])
            self._pairs = {}

    def save(self) -> None:
        """Persist pair→tokens mapping to disk."""
        if not self._pairs:
            return
        try:
            path = self._cache_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                fcntl.flock(f, fcntl.LOCK_EX)
                json.dump(self._pairs, f)
                fcntl.flock(f, fcntl.LOCK_UN)
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
        """Get all pairs."""
        return self._pairs

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
