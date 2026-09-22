"""Budget lifecycle for the provider quota ledger (mixin).

Extracted from ``rpc/ledger.py``: seeding a budget from the known quota
table, tariff scaling, window resets, and the degrade-level computation.
Mixed into :class:`~lite_indexer.rpc.ledger.ProviderLedger`.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

from lite_indexer.logging import get_logger
from lite_indexer.rpc.ledger_budget import KNOWN_QUOTAS, QuotaBudget
from lite_indexer.rpc.ledger_tariff import BASE_TARIFF_UNITS, CU_PER_UNIT

logger = get_logger(__name__)


class ProviderLedgerLifecycle:
    """Budget lifecycle mixin: seed, reset, tariff, degrade level."""

    _budgets: dict[str, QuotaBudget]

    @staticmethod
    def _budget_key(provider_id: str, account_id: str | None = None) -> str:
        return f"{provider_id}:{account_id or provider_id}"

    @staticmethod
    def _build_tariff(unit_type: str) -> dict[str, float]:
        """Scale the relative tariff table for the budget's unit type.

        CU/credits budgets multiply by ``CU_PER_UNIT`` so the base
        ``eth_chainId`` unit costs 5 CU and ``eth_getLogs`` 75 CU;
        request-unit/raw budgets use the relative weights directly.
        """
        scale = CU_PER_UNIT if unit_type in {"CU", "credits"} else 1.0
        return {method: round(weight * scale, 4) for method, weight in BASE_TARIFF_UNITS.items()}

    @staticmethod
    def _compute_reset_at(period: str, now: float | None = None) -> float:
        """Epoch time the quota window resets.

        ``day``/``month`` align to UTC calendar boundaries (provider windows
        reset on those); ``30d`` uses a rolling window from seeding time.
        """
        now = time.time() if now is None else now
        if period == "day":
            dt = datetime.fromtimestamp(now, UTC)
            return (
                (dt + timedelta(days=1))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .timestamp()
            )
        if period == "month":
            return _month_reset_at(now)
        days = 30 if period == "30d" else 1
        return now + days * 86_400

    def _seed_budget(
        self, provider_id: str, account_id: str, key_id: str | None = None
    ) -> QuotaBudget:
        seed = KNOWN_QUOTAS.get(provider_id)
        now = time.time()
        if seed is None:
            budget_units = None
            unit_type = "raw"
            period = "day"
        else:
            budget_units = seed.budget_units
            unit_type = seed.unit_type
            period = seed.period
        return QuotaBudget(
            provider_id=provider_id,
            account_id=account_id or provider_id,
            key_id=key_id or "",
            unit_type=unit_type,
            period=period,
            budget_units=budget_units,
            consumed_units=0.0,
            method_tariff=self._build_tariff(unit_type),
            degrade_level=0,
            reset_at=self._compute_reset_at(period, now),
            updated_at=now,
        )

    def _ensure_budget(
        self,
        provider_id: str,
        key_id: str | None = None,
        account_id: str | None = None,
    ) -> QuotaBudget:
        """Return the ACCOUNT budget, seeding it once per account.

        ``key_id`` is provenance only; the budget is keyed on ``account_id``
        (defaulting to ``provider_id``), so N keys of one account share ONE
        budget instead of N copies of the same quota.
        """
        account = account_id or provider_id
        key = self._budget_key(provider_id, account)
        budget = self._budgets.get(key)
        if budget is None:
            budget = self._seed_budget(provider_id, account, key_id)
            self._budgets[key] = budget
            logger.debug(
                "quota_budget_seeded", provider=provider_id, account_id=account, key_id=key_id or ""
            )
        return budget

    def _maybe_reset(self, budget: QuotaBudget, now: float | None = None) -> None:
        """Roll a budget into a fresh window when ``reset_at`` has passed."""
        now = time.time() if now is None else now
        if now >= budget.reset_at:
            budget.consumed_units = 0.0
            budget.degrade_level = 0
            budget.reset_at = self._compute_reset_at(budget.period, now)
            budget.updated_at = now
            logger.info(
                "quota_period_reset",
                provider=budget.provider_id,
                account_id=budget.account_id,
                key_id=budget.key_id,
                next_reset_at=budget.reset_at,
            )

    @staticmethod
    def _degrade_level(pct: float) -> int:
        if pct >= 95.0:
            return 3
        if pct >= 85.0:
            return 2
        if pct >= 70.0:
            return 1
        return 0

    @staticmethod
    def _tariff_for(budget: QuotaBudget, method: str) -> float:
        return budget.method_tariff.get(method, budget.method_tariff.get("default", 1.0))


def _month_reset_at(now: float) -> float:
    """First instant of the next UTC month (calendar-aligned reset)."""
    dt = datetime.fromtimestamp(now, UTC)
    year = dt.year + (1 if dt.month == 12 else 0)
    month = dt.month % 12 + 1
    return datetime(year, month, 1, tzinfo=UTC).timestamp()
