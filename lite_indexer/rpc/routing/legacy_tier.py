"""Legacy two-bucket tier classification — KEPT FOR API COMPATIBILITY.

``endpoint_tier`` is the pre-2026-09-28 view: ``dedicated`` (a key is
derivable) versus ``public`` (keyless). That is exactly the blind spot this
cost model exists to close — it cannot tell a $0 free-tier key from a
billable paid one, so routing on it silently spends money.

Nothing routes on it any more (see ``rpc/routing/selector.py``). It lives
here so every existing import path — ``lite_indexer.rpc``,
``lite_indexer.rpc.routing`` — keeps resolving, and so the deprecation is
visible at the definition rather than in a comment somewhere else.

New code must use :func:`lite_indexer.rpc.cost_tier.cost_tier`, which
distinguishes ``free`` / ``freemium`` / ``paid`` / ``unknown``.
"""

from __future__ import annotations


def endpoint_tier(endpoint: str) -> str:
    """Classify an endpoint as ``dedicated`` (keyed) or ``public`` (keyless).

    Args:
        endpoint: RPC endpoint URL.

    Returns:
        ``"dedicated"`` when a key is derivable (unresolved ``${VAR}``
        placeholder or an embedded key), ``"public"`` for a keyless endpoint,
        or ``"unknown"`` for an empty URL.
    """
    from lite_indexer.rpc.ledger import key_id_for_endpoint

    if not endpoint:
        return "unknown"
    if "${" in endpoint or key_id_for_endpoint(endpoint):
        return "dedicated"
    return "public"
