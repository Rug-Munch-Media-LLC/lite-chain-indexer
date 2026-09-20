"""LakeProviderRouter — rotation, quota enforcement, and usage ledger for lake providers.

Multiple free lake/data providers are used in rotation like the RPC layer:
never overrun a provider's allotment (rps / daily requests / daily bytes),
rotate on errors via exponential cooldowns, and persist usage to
``{DATA_DIR}/lake_usage.json``. Registry defaults + DuckDB resource bounds
live in :mod:`lite_indexer.lakes.registry`.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lite_indexer.config.indexing import DATA_DIR
from lite_indexer.lakes.registry import LakeProvider, _default_registry
from lite_indexer.logging import get_logger

logger = get_logger(__name__)

_COOLDOWN_CAP_S = 3600.0
_KIND_RANK = {"s3/anonymous": 0, "http/api": 1, "token": 2}


@dataclass
class _State:
    window_start: float = 0.0
    window_count: int = 0
    failures: int = 0
    cooldown_until: float = 0.0
    usage: dict[str, Any] = field(default_factory=lambda: {"requests": 0, "bytes": 0, "day": "", "failures": 0})


class LakeProviderRouter:
    """Health- and quota-aware selector over the lake provider registry."""

    def __init__(self, *, registry: dict[str, LakeProvider] | None = None, usage_path: Path | None = None) -> None:
        self._registry = registry if registry is not None else _default_registry()
        self._usage_path = usage_path if usage_path is not None else Path(DATA_DIR) / "lake_usage.json"
        self._state: dict[str, _State] = {name: _State() for name in self._registry}
        self._load_usage()

    # ── Selection ────────────────────────────────────────────────────

    def select(
        self, chain: str, table: str, *, exclude: frozenset[str] | set[str] = frozenset()
    ) -> LakeProvider | None:
        """Pick the best available provider for ``(chain, table)`` or None."""
        now = time.monotonic()
        candidates = []
        refused = False
        for p in self._registry.values():
            if p.name in exclude or not self._serves(p, chain, table):
                continue
            st = self._state[p.name]
            if st.cooldown_until > now:
                continue
            if not self._within_quota(p, st):
                refused = True
                continue
            candidates.append(p)
        if not candidates:
            if refused:
                logger.warning("lake_provider_quota_refused", chain=chain, table=table)
            return None
        best = min(candidates, key=lambda p: (_KIND_RANK.get(p.kind, 9), p.name))
        self._tick_rate(best)
        logger.info("lake_provider_selected", provider=best.name, chain=chain, table=table)
        return best

    @staticmethod
    def _serves(p: LakeProvider, chain: str, table: str) -> bool:
        return any(sc in ("*", chain) and st in ("*", table) for sc, st in p.serves)

    def _within_quota(self, p: LakeProvider, st: _State) -> bool:
        now = time.monotonic()
        self._rollover(st)
        if now - st.window_start >= 1.0:
            st.window_start, st.window_count = now, 0
        if st.window_count + 1 > p.rps:
            return False
        daily = st.usage.get("requests", 0)
        return not (p.daily_requests is not None and daily + 1 > p.daily_requests)

    def _tick_rate(self, p: LakeProvider) -> None:
        st = self._state[p.name]
        st.window_count += 1

    # ── Reporting ────────────────────────────────────────────────────

    def report_success(self, provider: str, *, bytes_read: int = 0) -> None:
        """Record a successful read: usage counters up, failure streak reset."""
        st = self._state.get(provider)
        if st is None:
            logger.debug("lake_provider_unknown", provider=provider)
            return
        st.failures = 0
        st.cooldown_until = 0.0
        self._rollover(st)
        st.usage["requests"] = int(st.usage.get("requests", 0)) + 1
        st.usage["bytes"] = int(st.usage.get("bytes", 0)) + max(0, int(bytes_read))
        self._save_usage()

    def report_failure(self, provider: str, *, error: str) -> None:
        """Record a failure: exponential cooldown (capped) + usage counter."""
        p = self._registry.get(provider)
        if p is None:
            logger.debug("lake_provider_unknown", provider=provider)
            return
        st = self._state[provider]
        st.failures += 1
        delay = min(p.cooldown_s * (2 ** (st.failures - 1)), _COOLDOWN_CAP_S)
        st.cooldown_until = time.monotonic() + delay
        self._rollover(st)
        st.usage["failures"] = int(st.usage.get("failures", 0)) + 1
        self._save_usage()
        logger.warning("lake_provider_failed", provider=provider, error=str(error)[:120])
        logger.warning("lake_provider_cooldown", provider=provider, cooldown_s=delay)

    @staticmethod
    def _rollover(st: _State) -> None:
        today = time.strftime("%Y-%m-%d")
        if st.usage.get("day") != today:
            st.usage["day"] = today
            st.usage["requests"] = 0
            st.usage["bytes"] = 0

    # ── Usage persistence ────────────────────────────────────────────

    def _load_usage(self) -> None:
        try:
            raw = json.loads(self._usage_path.read_text())
        except (OSError, ValueError):
            logger.debug("lake_usage_unreadable", path=str(self._usage_path))
            return
        for name, usage in raw.items():
            if name in self._state and isinstance(usage, dict):
                self._state[name].usage = {
                    "requests": int(usage.get("requests", 0)),
                    "bytes": int(usage.get("bytes", 0)),
                    "day": str(usage.get("day", "")),
                    "failures": int(usage.get("failures", 0)),
                }

    def _save_usage(self) -> None:
        payload = {name: st.usage for name, st in self._state.items()}
        try:
            self._usage_path.parent.mkdir(parents=True, exist_ok=True)
            self._usage_path.write_text(json.dumps(payload))
        except OSError as exc:
            logger.debug("lake_usage_persist_failed", error=str(exc)[:80])
