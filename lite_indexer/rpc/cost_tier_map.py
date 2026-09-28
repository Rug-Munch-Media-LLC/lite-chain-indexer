"""Cost-class vocabulary and the pool scoring weight for each class.

Pure DATA plus the one scoring lookup, kept separate from
:mod:`lite_indexer.rpc.cost_tier` so the classifier stays a single function.

WHY THE PENALTY EXISTS
    ``get_best_endpoint`` used to rank endpoints by list position and health
    alone, so a billable endpoint that happened to sit early in the pool won
    every call regardless of what it cost. :func:`cost_penalty` adds a term
    that is larger than any list position can ever be, so cost outranks
    position outright.

WHY THESE NUMBERS
    :func:`lite_indexer.rpc.pool.get_best_endpoint` keeps only candidates
    scoring below ``MAX_FAILURES * 1000`` (3000). Every value here is under
    that ceiling, so a paid endpoint stays *eligible* — this reorders, it
    never excludes, and a pool with no free endpoint still works. The
    ordering is strictly monotone in cost (free < freemium < unknown <
    paid) and every value exceeds the maximum list-position term, so for any
    pool of fewer than 900 endpoints the class term decides first.

``unknown`` sits between freemium and paid: we cannot prove it is free, so
it is treated as costing more than a keyless endpoint but less than one the
operator has explicitly declared billable.
"""

from __future__ import annotations

from typing import get_args

from lite_indexer.config import RPC_COST_TIER_UNKNOWN
from lite_indexer.rpc.cost_tier import CostClass

#: Every class ``cost_tier`` may return. Operators may only declare these.
COST_CLASSES: tuple[str, ...] = get_args(CostClass)

#: Class → penalty added by ``get_best_endpoint`` scoring (0 = free).
COST_PENALTY_SCORE: dict[str, int] = {
    "free": 0,
    "freemium": 900,
    "unknown": 1200,
    "paid": 1800,
}

#: Class assumed for a keyless endpoint when ``RPC_COST_TIERS`` says nothing.
KEYLESS_DEFAULT: CostClass = "free"


def cost_penalty(endpoint: str) -> int:
    """Pool-scoring penalty for ``endpoint``, derived from its cost class.

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        An integer penalty; 0 for free endpoints. An endpoint that cannot be
        classified at all is charged :data:`RPC_COST_TIER_UNKNOWN`'s class
        penalty rather than zero, so "we do not know" never means "free".
    """
    from lite_indexer.rpc.cost_tier import cost_tier

    return COST_PENALTY_SCORE.get(cost_tier(endpoint), COST_PENALTY_SCORE[RPC_COST_TIER_UNKNOWN])
