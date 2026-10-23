"""Persist a real ``eth_getLogs`` probe as an endpoint capability record.

Why this module exists (2026-10-23): the capability registry was only
filled by the *verification* path, so an endpoint proven usable by a LIVE
probe (the 5-minute health monitor, a desperation probe) still carried no
record. Its span then stayed unknown (``getlogs_max_range == 0``), the pool
gauge excluded it, and the range planner judged every request a violation.
Keyed URLs (Ankr / Alchemy / dRPC) were the worst hit: proven on every
cycle, never recorded.

The verdict is written through :func:`set_capability`, so it is both in
memory (this process) and persisted (survives restart). A transient
failure (timeout, 429, 5xx) never writes ``logs_capable=False`` — only a
real capability refusal (401/403/405, -32701, pruned/range ceiling) does.
"""

from __future__ import annotations

import time

from lite_indexer.logging import get_logger

logger = get_logger(__name__)

#: Reason fragments that mean "this endpoint structurally cannot serve
#: logs" (a capability property, not a transient fault).
_REFUSAL_TOKENS: tuple[str, ...] = (
    "http 401",
    "http 403",
    "http 405",
    "rpc -32701",
    "rpc -32601",
    "range",
    "limit",
    "exceed",
    "header not found",
    "pruned",
    "archive",
)


def note_logs_probe(
    endpoint: str,
    chain: str,
    span_blocks: int,
    ok: bool,
    reason: str,
) -> bool:
    """Record one real ``eth_getLogs`` probe outcome for ``endpoint``.

    A PASS sets ``logs_capable=True`` and raises ``getlogs_max_range`` to
    at least the span that just succeeded (never shrinks a measured span).
    A FAIL that classifies as a capability refusal sets
    ``logs_capable=False`` so the endpoint reads unhealthy; a transient
    failure writes nothing (a timeout must never remove capacity).

    Args:
        endpoint: RPC endpoint URL (keyed URLs included — the record key is
            the configured URL, so the gauge lookup matches).
        chain: Chain the endpoint serves.
        span_blocks: Blocks the probe just requested.
        ok: Whether the probe served the span.
        reason: Classifier reason (``"ok"`` or the provider rejection).

    Returns:
        True when a record was written (capability verdict changed or span
        measured), False when the failure was transient and nothing was
        recorded.
    """
    from lite_indexer.rpc.capabilities import CapabilityRecord, get_capability, set_capability

    refusal = any(token in reason.lower() for token in _REFUSAL_TOKENS)
    if not ok and not refusal:
        return False

    record = get_capability(endpoint)
    if record is None:
        record = CapabilityRecord(endpoint=endpoint, chain=chain)
    if ok:
        record.logs_capable = True
        record.getlogs_max_range = max(int(record.getlogs_max_range), int(span_blocks))
    else:
        record.logs_capable = False
    record.probed_at = time.time()
    set_capability(record)
    logger.debug(
        "logs_capability_recorded",
        endpoint=endpoint[:60],
        chain=chain,
        logs_capable=record.logs_capable,
        getlogs_max_range=record.getlogs_max_range,
        reason=reason[:60],
    )
    return True
