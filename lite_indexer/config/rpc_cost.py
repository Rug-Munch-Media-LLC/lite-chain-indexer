"""Cost-model configuration for the RPC router (the money seam).

Split from :mod:`lite_indexer.config.rpc` so the routing/pacing config there
stays at its recorded size and every knob below is reviewable in one place.
``lite_indexer.config`` re-exports these names, so ``from
lite_indexer.config import RPC_COST_TIERS`` keeps working like every other
config constant.

Four knobs, all operator-supplied, all default-deny:

``RPC_COST_TIERS``
    account-id → ``free``/``freemium``/``paid``/``unknown``. The only way to
    declare that a keyed account is genuinely billable. Unset means every
    keyed account stays ``freemium`` and is never treated as free.
``RPC_SPILLOVER_ALLOWED``
    account ids permitted to serve a call once the free tier is exhausted.
    Empty (the default) means no keyed endpoint may ever be used as a
    fallback — a fresh clone spends nothing.
``RPC_PROVIDER_DAILY_BUDGET``
    global per-account daily request ceiling. See    :mod:`lite_indexer.rpc.budgets` for enforcement; the number is a request
    count per UTC day, not currency.
``RPC_COST_TIER_UNKNOWN``
    class assumed for an endpoint that cannot be classified at all. Defaults
    to ``unknown`` (never silently free).

Malformed JSON degrades to the safe default rather than raising: a typo in an
env var must never stop the indexer from starting, and must never widen
spending.
"""

from __future__ import annotations

import json
import os

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: The four cost classes. Mirrors ``CostClass`` without importing the router
#: (dependencies flow query/ -> rpc/ -> config/, never the reverse).
COST_TIERS: frozenset[str] = frozenset({"free", "freemium", "paid", "unknown"})


def parse_cost_tiers(raw: str) -> dict[str, str]:
    """Parse ``RPC_COST_TIERS`` (account-id → class), dropping bad entries.

    Args:
        raw: JSON object string, as supplied by the operator.

    Returns:
        The map with only known classes and non-empty account ids. Malformed
        JSON, a non-object, or an unknown class value yields that entry
        dropped — never an exception, never an invented class.
    """
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        logger.warning("rpc_cost_tiers_unparseable", error=str(exc)[:120], raw_len=len(raw))
        return {}
    if not isinstance(data, dict):
        return {}
    return {str(account): str(cls) for account, cls in data.items() if str(account) and str(cls) in COST_TIERS}


#: account-id → cost class. Empty = derive from key presence.
RPC_COST_TIERS: dict[str, str] = parse_cost_tiers(os.environ.get("RPC_COST_TIERS", ""))

#: Class assumed when an endpoint cannot be classified. Never "free".
RPC_COST_TIER_UNKNOWN: str = os.environ.get("RPC_COST_TIER_UNKNOWN", "unknown")

#: Account ids allowed to take a call when the free tier is exhausted, as a
#: comma-separated list of account ids. Default EMPTY = no implicit spending;
#: an account must be named deliberately before it can ever be paid.
RPC_SPILLOVER_ALLOWED: frozenset[str] = frozenset(
    part.strip() for part in os.environ.get("RPC_SPILLOVER_ALLOWED", "").split(",") if part.strip()
)

#: Per-account daily request ceiling applied when a provider or endpoint
#: budget omits ``daily_budget``. 100_000 requests/account/UTC-day.
#:
#: The load-distribution defect this addresses: selection kept favouring the
#: lowest-latency endpoint, so a handful of free endpoints blew their per-IP
#: caps and got soft-banned while 30+ sat idle. A per-identity budget — plus
#: a PRE-EMPTIVE cool-down at ``RPC_BUDGET_PREEMPTIVE_PCT`` of the daily
#: budget, i.e. *before* the provider's 429 arrives — spreads the load
#: instead of waiting for eviction.
#:
#: Concrete free-tier values come from the operator as JSON (never baked into
#: git); a worked map with redacted hosts lives in
#: ``config/production.example.toml``::
#:
#:   RPC_PROVIDER_BUDGETS='{"<provider-id>": {"rps": 5, "daily_budget": 100000}}'
#:   RPC_ENDPOINT_BUDGETS='{"https://<host>/<path>": {"rps": 3}}'
#:
#: Every field is optional. An endpoint entry wins over a provider entry;
#: both win over this default. On breach (rpc/budgets.py): at the pre-emptive
#: percentage the account is cooled and ``rpc_budget_daily_approaching``
#: fires; at 100% it is UNAVAILABLE for the rest of the UTC day
#: (``budget_available()`` -> False) and ``rpc_budget_daily_exhausted``
#: fires — a hard stop, not a slowdown. 0 restores unlimited and must be
#: typed deliberately.
