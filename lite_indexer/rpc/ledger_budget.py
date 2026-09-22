"""Quota seed + per-account budget state for the provider quota ledger.

Extracted from ``rpc/ledger.py``: the documented free-tier quota seeds
(operator-supplied via env), the account-id overrides, and the persisted
:class:`QuotaBudget` dataclass. No persistence I/O here — that lives in
``rpc/ledger_persist.py``.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field

from lite_indexer.logging import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class QuotaSeed:
    """Documented free-tier quota used to seed a provider budget."""

    budget_units: int
    unit_type: str
    period: str


# Free-tier quota seeds, supplied by the operator (private) as a JSON object
# in ``LITE_INDEXER_QUOTA_SEEDS`` keyed by PROVIDER ID (the registrable
# domain, e.g. ``"example-rpc.com"``)::
#
#   {"example-rpc.com": {"budget_units": 30000000, "unit_type": "CU", "period": "month"}}
#
# Empty by default: endpoints are tracked but never degraded (the degrade
# ladder is opt-in via seeds). Per-key caps in ``rpc/provider_caps.py`` work
# regardless.
def _load_known_quotas() -> dict[str, QuotaSeed]:
    raw = os.environ.get("LITE_INDEXER_QUOTA_SEEDS", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("quota_seeds_parse_failed")
        return {}
    if not isinstance(data, dict):
        return {}
    seeds: dict[str, QuotaSeed] = {}
    for provider_id, spec in data.items():
        if not isinstance(spec, dict):
            continue
        try:
            seeds[str(provider_id)] = QuotaSeed(
                budget_units=int(spec["budget_units"]),
                unit_type=str(spec["unit_type"]),
                period=str(spec["period"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
    return seeds


KNOWN_QUOTAS: dict[str, QuotaSeed] = _load_known_quotas()


def _load_account_ids() -> dict[str, str]:
    """Operator-supplied ``key_id``/``provider_id`` → ``account_id`` overrides.

    Read from ``LITE_INDEXER_ACCOUNT_IDS`` (a JSON object). Both keys are
    non-secret: a ``provider_id`` (registrable domain) groups all of that
    provider's keys into one named account, while a ``key_id`` (the hashed
    id from :func:`key_id_for_endpoint`) maps one key to a specific account.
    Empty by default — every provider then collapses to a single account id
    equal to its ``provider_id`` (one account per provider).
    """
    raw = os.environ.get("LITE_INDEXER_ACCOUNT_IDS", "")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        logger.warning("account_ids_parse_failed")
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(k): str(v) for k, v in data.items() if str(v)}


KNOWN_ACCOUNT_IDS: dict[str, str] = _load_account_ids()

# Dataclass field names used to filter persisted JSON on load (survives
# schema drift by ignoring unknown keys).
_QUOTA_FIELDS = frozenset(
    {
        "provider_id",
        "account_id",
        "key_id",
        "unit_type",
        "period",
        "budget_units",
        "consumed_units",
        "method_tariff",
        "degrade_level",
        "reset_at",
        "updated_at",
    }
)


@dataclass
class QuotaBudget:
    """Persisted per-ACCOUNT quota state.

    ``budget_units`` is ``None`` for providers without a documented free
    quota — they are tracked for stats but never degrade (graceful by
    default). ``consumed_units`` is tariff-weighted: an ``eth_getLogs``
    at 75 CU costs 15x an ``eth_chainId`` at 5 CU.

    ``account_id`` is what the quota is keyed on (NOT ``key_id``): all keys
    of one account share this budget. It defaults to ``provider_id`` (one
    account per provider) so keys are never counted as separate capacity.
    ``key_id`` is retained as non-secret provenance only — per-key rate
    limits live in ``rpc/budgets.py`` / ``rpc/provider_caps.py``.
    """

    provider_id: str
    account_id: str = ""
    key_id: str = ""
    unit_type: str = "raw"  # CU | credits | request_units | raw
    period: str = "day"  # day | 30d | month
    budget_units: int | None = None
    consumed_units: float = 0.0
    method_tariff: dict[str, float] = field(default_factory=dict)
    degrade_level: int = 0
    reset_at: float = 0.0
    updated_at: float = 0.0

    def __post_init__(self) -> None:
        # One account per provider unless the operator named a real account.
        if not self.account_id:
            self.account_id = self.provider_id

    def consumed_pct(self) -> float:
        """Percent of the budget consumed (None-safe → 0.0, can exceed 100)."""
        if not self.budget_units:
            return 0.0
        return 100.0 * self.consumed_units / self.budget_units

    def parked(self, now: float | None = None) -> bool:
        """Whether this account is parked until ``reset_at`` (level 3)."""
        now = time.time() if now is None else now
        return self.degrade_level >= 3 and now < self.reset_at
