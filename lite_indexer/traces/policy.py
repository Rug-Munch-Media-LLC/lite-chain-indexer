"""Per-chain trace budget — how much of a sync slice may go to tracing.

Why this exists (2026-10-23 base stall)
--------------------------------------
Base's only free trace endpoint (``base.merkle.io``) answers
``debug_traceBlockByNumber`` with a 429 and rate-limits ``trace_block`` at
the provider level. The collector probed the 429 method every cycle, so each
slice of a sync window burned the full global 300 s phase timeout. A 200-block
base window is ~6 slices, i.e. ~1800 s per window against ~400 s of chain
time — base fell ~21 days behind tip while its highwater kept advancing.

ERC-20 logs are the durable record; traces are enrichment. Base therefore
gets a small per-slice sweep under a short phase cap so a rate-limited trace
endpoint can never starve the transfer/dex legs. Unlisted chains keep the
historical global defaults.
"""

from __future__ import annotations

from dataclasses import dataclass

#: 2026-10-24: 300 s was a SLICE-KILLER, not a budget. The trace phase runs
#: before the durable legs checkpoint, so with a 90 s slice ceiling the outer
#: timeout fired first and bsc/ethereum advanced NOTHING (measured: highwater
#: frozen 23 min, sync_slice_timeout slices_done=0 on the same range). A phase
#: budget must sit well BELOW the slice ceiling so traces can only ever cost
#: their own box; 15 s leaves ~75 s for transfers/dex/insert.
DEFAULT_PHASE_TIMEOUT_S = 15.0
DEFAULT_MAX_BLOCKS = 100
DEFAULT_CONCURRENCY = 2


@dataclass(frozen=True)
class TracePolicy:
    """Bound on the internal-transfer trace phase for one chain."""

    max_blocks_per_cycle: int
    phase_timeout_s: float
    concurrency: int


_BASE_POLICY = TracePolicy(max_blocks_per_cycle=5, phase_timeout_s=30.0, concurrency=1)

#: Chains needing a tighter-than-global trace budget.
TRACE_CHAIN_POLICY: dict[str, TracePolicy] = {"base": _BASE_POLICY}

_DEFAULT_POLICY = TracePolicy(
    max_blocks_per_cycle=DEFAULT_MAX_BLOCKS,
    phase_timeout_s=DEFAULT_PHASE_TIMEOUT_S,
    concurrency=DEFAULT_CONCURRENCY,
)


def trace_policy(chain: str) -> TracePolicy:
    """Return the trace budget for ``chain`` (global defaults when unlisted).

    Args:
        chain: Chain name (e.g. ``"base"``).

    Returns:
        The chain's :class:`TracePolicy`.
    """
    return TRACE_CHAIN_POLICY.get(chain, _DEFAULT_POLICY)
