"""Read-only views over the provider quota ledger (mixin).

Extracted from ``rpc/ledger.py``: the non-mutating accessors — current
degrade level, consumed percent, parked state, per-account state dict, and
the full snapshot. Mixed into :class:`~lite_indexer.rpc.ledger.ProviderLedger`.
"""

from __future__ import annotations

import time
from typing import Any

from lite_indexer.rpc.ledger_budget import QuotaBudget
from lite_indexer.rpc.ledger_lifecycle import ProviderLedgerLifecycle
from lite_indexer.rpc.ledger_tariff import degrade_action


class ProviderLedgerViews(ProviderLedgerLifecycle):
    """Read-only view mixin over the account budgets."""

    _budgets: dict[str, QuotaBudget]

    def degrade_level_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> int:
        """Current degrade level (0-3) for an account without recording a call."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return 0
        now = time.time() if now is None else now
        self._maybe_reset(budget, now)
        budget.degrade_level = self._degrade_level(budget.consumed_pct())
        return budget.degrade_level

    def consumed_pct_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        *,
        account_id: str | None = None,
    ) -> float:
        """Percent of the account budget consumed (0.0 for unknown/unmanaged)."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return 0.0
        return budget.consumed_pct()

    def parked(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> bool:
        """Whether the account is parked (level 3) until ``reset_at``."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return False
        now = time.time() if now is None else now
        return budget.parked(now)

    def state_for(
        self,
        provider_id: str,
        key_id: str | None = None,
        now: float | None = None,
        *,
        account_id: str | None = None,
    ) -> dict[str, Any]:
        """Public read-only view of one account's quota state."""
        budget = self._budgets.get(self._budget_key(provider_id, account_id or provider_id))
        if budget is None:
            return {}
        now = time.time() if now is None else now
        self._maybe_reset(budget, now)
        return _budget_state(budget, now)

    def snapshot(self, now: float | None = None) -> dict[str, dict[str, Any]]:
        """Full ledger view (for logging / metrics)."""
        return {
            key: self.state_for(
                budget.provider_id, budget.key_id, now, account_id=budget.account_id
            )
            for key, budget in self._budgets.items()
        }


def _budget_state(budget: QuotaBudget, now: float) -> dict[str, Any]:
    """Render one budget as the public state dict."""
    return {
        "provider_id": budget.provider_id,
        "account_id": budget.account_id,
        "key_id": budget.key_id,
        "unit_type": budget.unit_type,
        "period": budget.period,
        "budget_units": budget.budget_units,
        "consumed_units": round(budget.consumed_units, 2),
        "consumed_pct": round(budget.consumed_pct(), 1),
        "degrade_level": budget.degrade_level,
        "degrade_action": degrade_action(budget.degrade_level),
        "parked": budget.parked(now),
        "reset_at": budget.reset_at,
        "updated_at": budget.updated_at,
    }
