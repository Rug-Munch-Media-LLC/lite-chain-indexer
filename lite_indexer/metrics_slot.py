"""Slot-stage metrics: the discriminator between an RPC fault and starvation.

A chain can hold healthy RPC endpoints (``lite_indexer_rpc_pool_healthy_endpoints
> 0``) and still ingest nothing, because it never received a concurrency slot.
Before this module the data-freshness alert could only say "nothing landed"; it
could not say WHICH of the three stages stopped: slot -> fetch -> write.

  lite_indexer_chain_slot_state{chain}        2 holding, 1 queued, absent idle
  lite_indexer_chain_slot_wait_seconds{chain} age of the queue entry

Read together with the pool gauges:
  healthy>0 and slot_state==1 (wait>900) -> SCHEDULER starvation, not RPC
  healthy==0                             -> RPC pool fault
  slot_state==2 and ingest stale         -> fetch/parse/write stage

2026-10-23/24 production: 3 grants, 12 queued, 0 releases in 7 min while bsc
sat at queue depth 2 and never emitted a single ``sync_start`` — the scheduler
case, invisible in every existing series.
"""

from __future__ import annotations

from lite_indexer.cli import _slot_rotation
from lite_indexer.metrics import set_labeled_gauge


def export_slot_metrics() -> None:
    """Publish per-chain slot state; never raises (observability only)."""
    rotation = _slot_rotation.CURRENT
    if rotation is None:
        return
    for chain, (state, wait_s) in rotation.snapshot().items():
        set_labeled_gauge("lite_indexer_chain_slot_state", state, chain=chain)
        set_labeled_gauge("lite_indexer_chain_slot_wait_seconds", wait_s, chain=chain)


__all__ = ["export_slot_metrics"]
