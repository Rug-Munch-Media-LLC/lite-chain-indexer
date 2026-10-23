"""Per-chain trace policy — how much sync budget a chain may spend tracing.

Why this exists (2026-10-23 base stall)
--------------------------------------
Base's only free trace endpoint (``base.merkle.io``) answers
``debug_traceBlockByNumber`` with a 429 and rate-limits ``trace_block`` at
the provider level. The collector probed the 429 method first on every
cycle, so each slice of a sync window burned the full global 300 s phase
timeout. A 200-block base window is 6 slices, i.e. ~1800 s per window
against ~400 s of chain time — base fell ~21 days behind tip while its
highwater kept advancing.

Traces are enrichment (ERC-20 logs are the durable record), so base gets a
small per-slice sweep, a short phase timeout and a static method hint
instead of a live probe. Transfers/dex/deployers are untouched.
"""

from __future__ import annotations

from dataclasses import dataclass

DEFAULT_PHASE_TIMEOUT_S = 300.0
DEFAULT_MAX_BLOCKS = 100
DEFAULT_CONCURRENCY = 2


@dataclass(frozen=True)
class TracePolicy:
    """Bound on the internal-transfer trace phase for one chain."""

    max_blocks_per_cycle: int
    phase_timeout_s: float
    concurrency: int
    preferred_method: str | None = None


#: Chains that need a tighter-than-global trace budget. Anything absent
#: here keeps the historical global defaults.
TRACE_CHAIN_POLICY: dict[str, TracePolicy] = {
    # base.merkle.io: 429 on debug_traceBlockByNumber, provider-rate-limited
    # on trace_block. 5 blocks + 1 call at a time + 30 s cap keeps the
    # per-slice cost bounded; a rate-limited slice advances (traces are
    # backfillable) instead of stalling the whole window.
    "base": TracePolicy(
        max_blocks_per_cycle=5,
        phase_timeout_s=30.0,
        concurrency=1,
        preferred_method="trace_block",
    ),
}


def trace_policy(chain: str) -> TracePolicy:
    """Return the trace budget for ``chain`` (defaults when unlisted).

    Args:
        chain: Chain name (e.g. ``"base"``).

    Returns:
        The chain's :class:`TracePolicy`.
    """
    return TRACE_CHAIN_POLICY.get(
        chain,
        TracePolicy(
            max_blocks_per_cycle=DEFAULT_MAX_BLOCKS,
            phase_timeout_s=DEFAULT_PHASE_TIMEOUT_S,
            concurrency=DEFAULT_CONCURRENCY,
            preferred_method=None,
        ),
    )
