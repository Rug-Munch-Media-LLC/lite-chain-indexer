"""Cost classification for an RPC endpoint — the router's only money concept.

WHAT THIS DOES
    Assigns an endpoint exactly one class — ``free``, ``freemium``, ``paid``
    or ``unknown`` — so the router can prefer endpoints that cost the
    operator nothing and treat a billable endpoint as an *escalation target*
    rather than a default. Classification order (first match wins):

    1. **operator override** — ``RPC_COST_TIERS`` maps a provider account id
       to a class, so an account that really bills us is never mistaken for
       a free-tier one (and vice versa);
    2. **keyless** → ``free``;
    3. **keyed** (embedded key or unresolved ``${VAR}`` placeholder) →
       ``freemium`` — a key exists, so the provider meters it, but we do not
       assume it is billable until an operator says so;
    4. empty / undeclared-and-unkeyable → ``unknown``.

WHAT THIS DOES NOT DO
    * It does **not** price anything. There is no USD conversion, no
      per-provider rate table and no invoice correlation. ``paid`` means
      "an operator declared this account billable", never "we computed a
      cost". :mod:`lite_indexer.rpc.spend` is likewise a request-count
      proxy, not money.
    * It does **not** authorise spending. Deciding whether a keyed endpoint
      may serve a call is the spillover gate's job
      (:func:`lite_indexer.rpc.spillover.spillover_allowed`), not this
      module's.
    * It does **not** read, log or persist the key value. Identity comes
      from the ledger's non-secret account id (a hash), never the secret.

The operator map is injected via the ``tiers=`` keyword (the DI seam
convention of ``rpc/routing/seams.py``): production passes the parsed
config, tests pass a literal. Nothing is monkeypatched.
"""

from __future__ import annotations

from typing import Literal, cast

from lite_indexer.config import COST_TIERS, RPC_COST_TIER_UNKNOWN, RPC_COST_TIERS

CostClass = Literal["free", "freemium", "paid", "unknown"]


def cost_tier(endpoint: str, *, tiers: dict[str, str] | None = None) -> CostClass:
    """Classify ``endpoint`` into its cost class.

    Args:
        endpoint: RPC endpoint URL. An empty URL is ``unknown``.
        tiers: Optional account-id → class override map. Defaults to
            :data:`RPC_COST_TIERS`. Values outside the four known classes are
            ignored, so a typo degrades to the derived class instead of
            inventing a new one.

    Returns:
        One of ``"free"``, ``"freemium"``, ``"paid"``, ``"unknown"``.
    """
    if not endpoint:
        return "unknown"

    from lite_indexer.rpc.ledger import account_id_for_endpoint, key_id_for_endpoint

    declared = (RPC_COST_TIERS if tiers is None else tiers).get(account_id_for_endpoint(endpoint), "")
    if declared in COST_TIERS:
        return cast(CostClass, declared)
    if "${" in endpoint or key_id_for_endpoint(endpoint):
        return "freemium"
    override = RPC_COST_TIER_UNKNOWN
    if override in COST_TIERS and override != "unknown":
        return cast(CostClass, override)
    return "free"
