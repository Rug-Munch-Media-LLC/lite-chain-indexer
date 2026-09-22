"""Disk persistence for the provider quota ledger (mixin).

Extracted from ``rpc/ledger.py``: atomic JSON-on-disk load/save with
debounced writes — the same contract as
:class:`~lite_indexer.rpc.throttle.RPCRateTracker`. Mixed into
:class:`~lite_indexer.rpc.ledger.ProviderLedger`.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict
from pathlib import Path

from lite_indexer.config import DATA_DIR
from lite_indexer.logging import get_logger
from lite_indexer.rpc.ledger_budget import _QUOTA_FIELDS, QuotaBudget

logger = get_logger(__name__)

# Persisted ledger path (default: DATA_DIR/quota_ledger.json).
QUOTA_LEDGER_FILE: str = os.environ.get("QUOTA_LEDGER_FILE", str(DATA_DIR / "quota_ledger.json"))


class ProviderLedgerPersist:
    """Persistence mixin: load on init, debounced atomic saves."""

    _path: Path
    _persist_interval: float
    _last_persist: float
    _budgets: dict[str, QuotaBudget]

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
            for data in raw.values():
                if not isinstance(data, dict):
                    continue
                self._fold_loaded_budget(data)
            if self._budgets:
                logger.debug(
                    "quota_ledger_loaded", budgets=len(self._budgets), path=str(self._path)
                )
        except (json.JSONDecodeError, OSError, TypeError, ValueError) as exc:
            logger.warning("quota_ledger_load_failed", path=str(self._path), error=str(exc)[:80])

    def _fold_loaded_budget(self, data: dict[str, object]) -> None:
        """Insert one loaded budget, folding pre-account per-key ledgers.

        Migration: a pre-account ledger wrote one budget PER key. Fold those
        into the single account budget so an upgrade neither loses
        consumption nor resurrects the N× over-count.
        """
        clean = {k: v for k, v in data.items() if k in _QUOTA_FIELDS}
        budget = QuotaBudget(**clean)
        key = self._budget_key(budget.provider_id, budget.account_id)
        existing = self._budgets.get(key)
        if existing is None:
            self._budgets[key] = budget
            return
        existing.consumed_units += budget.consumed_units
        existing.degrade_level = max(existing.degrade_level, budget.degrade_level)
        existing.reset_at = max(existing.reset_at, budget.reset_at)
        existing.updated_at = max(existing.updated_at, budget.updated_at)

    def persist(self) -> None:
        """Atomically write the ledger state to disk."""
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            payload = json.dumps(
                {key: asdict(budget) for key, budget in self._budgets.items()},
                indent=2,
            )
            tmp = self._path.with_suffix(".json.tmp")
            tmp.write_text(payload)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("quota_ledger_save_failed", path=str(self._path), error=str(exc)[:80])

    def persist_if_stale(self, now: float | None = None) -> None:
        """Persist when the last write is older than the interval.

        Debounces disk writes so a burst of ``record_call`` traffic flushes
        at most once per ``persist_interval`` (a crash loses at most that
        much consumption tracking).
        """
        now = time.time() if now is None else now
        if now - self._last_persist >= self._persist_interval:
            self._last_persist = now
            self.persist()
