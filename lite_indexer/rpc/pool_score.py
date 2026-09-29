"""Endpoint selection score for the RPC pool.

Pure arithmetic, split out of :mod:`lite_indexer.rpc.pool` so it is testable
without a pool, a clock or a network.

WHY THE FAILURE TERM IS NOT FLAT
    ``pool.get_best_endpoint`` scores ``len(failures) * 1000 + position +
    cost_penalty(endpoint)``. A free endpoint therefore lost to a *healthy*
    freemium peer after a SINGLE recorded failure, because one failure (1000)
    exceeded the freemium cost penalty (900). Public keyless RPCs fail
    transiently as a matter of course — rate limits, timeouts, 5xx — so one
    blip pushed every subsequent call onto a billable endpoint for the whole
    cooldown window. That is the mechanism behind the provider quota notices:
    not a billing decision, a scoring artifact.

THE RULE
    Tolerate ONE transient failure, then escalate. The first recent failure
    costs less than the gap between cost classes, so a free endpoint that
    blipped once keeps serving. A second failure costs more than the gap, so a
    genuinely broken endpoint is still skipped in favour of a working paid one.

    This keeps the property the original design intended — cost outranks list
    position, and a broken endpoint is never preferred — while making cost
    dominate *transient* noise, which is what a flat 1000 per failure did not.
"""

from __future__ import annotations

#: Cost of the first recent failure. Deliberately below the smallest gap
#: between cost classes (free->freemium is COST_PENALTY_SCORE["freemium"] = 900)
#: so a single blip does not escalate the whole pool onto paid endpoints.
FIRST_FAILURE_COST = 400

#: Cost of each failure after the first. Above any cost-class gap, so a
#: sustained-failing endpoint is genuinely deprioritised.
REPEAT_FAILURE_COST = 1000


def failure_cost(failures: int) -> int:
    """Score contribution of ``failures`` recent failures.

    Args:
        failures: Number of failures still inside the cooldown window.

    Returns:
        ``FIRST_FAILURE_COST`` for a single blip, then
        ``REPEAT_FAILURE_COST`` for each further failure.
    """
    if failures <= 0:
        return 0
    if failures == 1:
        return FIRST_FAILURE_COST
    return FIRST_FAILURE_COST + (failures - 1) * REPEAT_FAILURE_COST


def endpoint_score(endpoint: str, position: int, failures: int) -> int:
    """Total selection score; lower is better.

    Args:
        endpoint: The endpoint being scored (for its cost class).
        position: Index in the configured pool.
        failures: Failures still inside the cooldown window.

    Returns:
        The score used to order candidates.
    """
    from lite_indexer.rpc.cost_tier_map import cost_penalty

    return failure_cost(failures) + position + cost_penalty(endpoint)


__all__ = [
    "FIRST_FAILURE_COST",
    "REPEAT_FAILURE_COST",
    "endpoint_score",
    "failure_cost",
]
