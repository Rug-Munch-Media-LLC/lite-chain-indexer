"""Shared provider gate chain for every outbound RPC POST path.

``RpcClient.call`` runs ``throttle_before`` → ``provider_throttle_before``
→ ``autotune_wait`` → ``_budget_before`` before dispatching. Side paths
(last-resort fallback, rate-limited crawl, capability/discovery probes)
POST directly and previously bypassed those gates — hammering shared
shared provider accounts into 429 streaks.

:func:`provider_gates` is the single reusable gate chain; every path that
POSTs to a provider calls it first. Imports are lazy to avoid import
cycles (client.py imports capabilities.py, which imports this module).
"""

from __future__ import annotations

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

__all__ = ["provider_gates"]


async def provider_gates(endpoint: str, method: str) -> None:
    """Run the full RpcClient.call gate chain before a direct POST.

    Same order as ``RpcClient.call``: per-method pacing, provider
    (account) throttle, auto-tune spacing, then the per-identity budget
    (pace to rps + record the dispatch).
    """
    from lite_indexer.rpc.autotune import autotune_wait
    from lite_indexer.rpc.client import _budget_before
    from lite_indexer.rpc.provider_caps import provider_throttle_before
    from lite_indexer.rpc.throttle import throttle_before

    await throttle_before(endpoint, method)
    await provider_throttle_before(endpoint)
    await autotune_wait(endpoint)
    await _budget_before(endpoint)
