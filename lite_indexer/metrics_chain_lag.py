"""Chain-lag and block-hash metric specs, split out of the core metrics map.

A chain 45,388,234 blocks behind a chain that is still producing blocks is
crawling, not stalled. Paging on it is a false alarm that trains the operator
to ignore the real stall — and it is why two competent fixes to the RPC pool
missed the cause. The lag gauge and the per-condition series exist so those
two conditions can never share a series again.

Split out rather than appended: ``metrics.py`` is under a shrink-only size
ratchet, and a seventh metric family does not belong in the core map.
"""

from __future__ import annotations

CHAIN_LAG_GAUGE_SPECS: dict[str, tuple[str, ...]] = {
    "lite_indexer_chain_lag_blocks": ("chain",),
    "lite_indexer_chain_stalled": ("chain",),
    "lite_indexer_chain_lagging": ("chain",),
    "lite_indexer_chain_unknown": ("chain",),
    "lite_indexer_chain_caught_up": ("chain",),
    "lite_indexer_rpc_block_hash_unserved": ("chain",),
    "lite_indexer_rpc_block_hash_fallback": ("chain",),
}

__all__ = ["CHAIN_LAG_GAUGE_SPECS"]
