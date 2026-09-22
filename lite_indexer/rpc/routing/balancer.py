"""Load balancing — power-of-two-choices endpoint selection.

P2C draws two candidates and returns the one with the lower damped
latency score. Callers are expected to pass the *healthy* subset
(banned / circuit-open / stale endpoints already filtered out) — the
pool's :func:`lite_indexer.rpc.pool.get_best_endpoint` does this with
its penalty scoring. Use :func:`select_p2c_healthy` when you need the
filtering done here.

Split out of the former monolithic ``rpc/routing.py``.
"""

from __future__ import annotations

import random

from lite_indexer.rpc.routing.health import _stats, stale_endpoints, tip_failed_endpoints


def select_p2c(endpoints: list[str]) -> str:
    """Power-of-two-choices endpoint selection.

    Draws two candidates uniformly and returns the one with the lower
    damped latency score ``ewma_ms * (1 + in_flight)``; ties break on
    the windowed error rate. With a single candidate that endpoint is
    returned directly.

    Callers are expected to pass the *healthy* subset (banned /
    circuit-open / stale endpoints already filtered out) — the pool's
    :func:`get_best_endpoint` does this with its penalty scoring. Use
    :func:`select_p2c_healthy` when you need the filtering done here.

    Args:
        endpoints: Candidate endpoint URLs (must be non-empty).

    Returns:
        The selected endpoint URL.
    """
    if not endpoints:
        raise ValueError("select_p2c requires a non-empty endpoint list")
    if len(endpoints) == 1:
        return endpoints[0]
    a, b = random.sample(endpoints, 2)  # noqa: S311
    sa, sb = _stats(a), _stats(b)
    score_a, score_b = sa.damped_score(), sb.damped_score()
    if score_a != score_b:
        return a if score_a < score_b else b
    return a if sa.error_rate() <= sb.error_rate() else b


def select_p2c_healthy(endpoints: list[str]) -> str:
    """P2C selection over the healthy subset of ``endpoints``.

    Filters out hard-banned, circuit-open, and stale endpoints before
    drawing — a convenience wrapper for callers that do not already
    maintain a healthy list (``get_best_endpoint`` does its own penalty
    scoring and calls :func:`select_p2c` directly). Falls back to the
    full list when fewer than two healthy candidates exist.

    Args:
        endpoints: Pool candidate URLs.

    Returns:
        The selected endpoint URL.
    """
    from lite_indexer.rpc.pool import is_endpoint_banned
    from lite_indexer.rpc.routing.health import is_circuit_open
    from lite_indexer.rpc.routing.seams import selector

    stale = set(stale_endpoints(endpoints))
    tip_failed = set(tip_failed_endpoints(endpoints))
    healthy = [
        ep
        for ep in endpoints
        if not is_endpoint_banned(ep) and not is_circuit_open(ep) and ep not in stale and ep not in tip_failed
    ]
    candidates = healthy if len(healthy) >= 2 else endpoints
    return selector()(candidates)
