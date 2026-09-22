"""
Provider quota ledger + degrade ladder.

An ADDITIVE layer on top of the per-endpoint throttling (``rpc/throttle.py``)
and the per-provider token bucket (``rpc/ratelimit.py``): this module tracks
long-window **account-level quota consumption** (Compute Units / credits /
request units) and returns a degrade level so the caller can back off
*before* the 429. Quota is grouped PER ACCOUNT (``account_id``), NOT per key — all keys of one
account draw ONE budget (extra keys buy rate, never quota). Consumption is
tariff-weighted (``eth_getLogs`` 15x ``eth_chainId``) and the degrade ladder
(70% / 85% / 95%) is *reported*, never enforced here — callers decide how to
honor it. Sibling modules: ``ledger_tariff`` (constants), ``ledger_keys``
(id derivation), ``ledger_budget`` (seeds + budget state), ``ledger_persist``
(persistence mixin), ``ledger_lifecycle`` (seeding/resets mixin),
``ledger_views`` (read-only accessors mixin).
"""

from __future__ import annotations

import time
from pathlib import Path

from lite_indexer.logging import get_logger
from lite_indexer.rpc.ledger_budget import KNOWN_ACCOUNT_IDS, KNOWN_QUOTAS, QuotaBudget
from lite_indexer.rpc.ledger_keys import (
    account_id_for_endpoint,
    key_id_for_endpoint,
    provider_id_for_endpoint,
)
from lite_indexer.rpc.ledger_persist import QUOTA_LEDGER_FILE, ProviderLedgerPersist
from lite_indexer.rpc.ledger_tariff import (
    BASE_TARIFF_UNITS,
    CU_PER_UNIT,
    DEGRADE_LADDER,
    HEAVY_DEFER_METHODS,
    LEVEL1_GETLOGS_RANGE_BLOCKS,
    PERIODS,
    UNIT_TYPES,
    _is_heavy_method,
    degrade_action,
)  # noqa: F401 — re-exported for the public surface
from lite_indexer.rpc.ledger_views import ProviderLedgerViews

logger = get_logger(__name__)

__all__ = [
    "BASE_TARIFF_UNITS",
    "CU_PER_UNIT",
    "DEGRADE_LADDER",
    "HEAVY_DEFER_METHODS",
    "KNOWN_ACCOUNT_IDS",
    "KNOWN_QUOTAS",
    "LEDGER",
    "LEVEL1_GETLOGS_RANGE_BLOCKS",
    "PERIODS",
    "QUOTA_LEDGER_FILE",
    "ProviderLedger",
    "QuotaBudget",
    "UNIT_TYPES",
    "account_id_for_endpoint",
    "defer_reason",
    "degrade_action",
    "key_id_for_endpoint",
    "provider_id_for_endpoint",
    "record_call",
    "record_endpoint_call",
]


class ProviderLedger(ProviderLedgerViews, ProviderLedgerPersist):
    """Persisted per-account quota ledger + degrade ladder.

    Single-writer, JSON-on-disk, debounced saves; load is best-effort (a
    corrupt file degrades to an empty ledger). The ledger is a
    **reporter**, not an enforcer: callers consult ``degrade_level_for``
    and decide how to honor the ladder. Unknown providers never degrade.
    """

    def __init__(
        self,
        path: str | Path | None = None,
        persist_interval: float = 60.0,
    ) -> None:
        self._path = Path(path or QUOTA_LEDGER_FILE)
        self._persist_interval = persist_interval
        self._last_persist: float = 0.0
        self._budgets: dict[str, QuotaBudget] = {}
        self._load()

    def record_call(
        self,
        provider_id: str,
        method: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> int:
        """Record one call and return the resulting degrade level (0-3).

        Increments ``consumed_units`` by the method's tariff weight and
        recomputes the degrade ladder level. The call is charged to the
        ACCOUNT budget (``account_id``, defaulting to ``provider_id``):
        0 normal, 1 (70%) widen polls, 2 (85%) head-only, 3 (95%) parked.
        """
        now = time.time() if now is None else now
        budget = self._ensure_budget(provider_id, key_id, account_id)
        self._maybe_reset(budget, now)
        if budget.budget_units is not None:
            budget.consumed_units += self._tariff_for(budget, method)
        budget.updated_at = now
        budget.degrade_level = self._degrade_level(budget.consumed_pct())
        self.persist_if_stale(now)
        return budget.degrade_level

    def record_endpoint_call(self, endpoint: str, method: str, now: float | None = None) -> int:
        """Record a call from an endpoint URL (derives account/key id)."""
        return self.record_call(
            provider_id_for_endpoint(endpoint),
            method,
            key_id_for_endpoint(endpoint),
            now,
            account_id=account_id_for_endpoint(endpoint),
        )

    def reset(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> None:
        """Manually zero an ACCOUNT budget (e.g. operator top-up) and persist."""
        budget = self._ensure_budget(provider_id, key_id, account_id)
        now = time.time() if now is None else now
        budget.consumed_units = 0.0
        budget.degrade_level = 0
        budget.reset_at = self._compute_reset_at(budget.period, now)
        budget.updated_at = now
        self.persist()
        logger.info(
            "quota_budget_reset",
            provider=provider_id,
            account_id=budget.account_id,
            key_id=key_id or "",
        )


# Module-level ledger shared across the codebase (persists to disk).
LEDGER = ProviderLedger()


def record_call(provider_id: str, method: str, key_id: str | None = None) -> int:
    return LEDGER.record_call(provider_id, method, key_id)


def record_endpoint_call(endpoint: str, method: str) -> int:
    return LEDGER.record_endpoint_call(endpoint, method)
def defer_reason(
    provider_id: str,
    method: str,
    key_id: str | None = None,
    range_blocks: int | None = None,
    *,
    account_id: str | None = None,
) -> str | None:
    """Reason string when a call should be DEFERRED, else ``None``.

    Consults the shared ledger's current degrade level without recording a
    call — a deferred call was never dispatched, so it must not consume
    quota. Ladder: level 3 → defer ALL; level 2 → heavy methods only;
    level 1 → wide backfill ``eth_getLogs`` scans only; level 0 → never.
    Never raises: on any ledger error it degrades to ``None`` (allow).
    """
    try:
        level = LEDGER.degrade_level_for(provider_id, key_id, account_id=account_id)
        if level <= 0:
            return None
        if level >= 3:
            return f"quota_deferred_level3_parked ({degrade_action(3)})"
        if level == 2:
            if _is_heavy_method(method):
                return f"quota_deferred_level2_head_only ({degrade_action(2)})"
            return None
        return _level1_reason(method, range_blocks)
    except Exception as exc:  # noqa: BLE001 — enforcement must never raise
        logger.debug("quota_defer_reason_skipped", provider=provider_id, error=str(exc)[:80])
        return None


def _level1_reason(method: str, range_blocks: int | None) -> str | None:
    """Level-1 defer reason: wide backfill scans only; head-follow allowed."""
    if (
        method in {"eth_getLogs", "eth_getLogsV2"}
        and range_blocks is not None
        and range_blocks > LEVEL1_GETLOGS_RANGE_BLOCKS
    ):
        return f"quota_deferred_level1_wide_scan ({degrade_action(1)})"
    return None
